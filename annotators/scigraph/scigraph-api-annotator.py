#!/usr/bin/env python3
#
# scigraph-api-annotator.py <input directory of JSON files>
#
# Given an input directory of JSON files, produces a list of annotations for each CRF and CDE as a CSV.
# Optionally, it can produce a YAML file that describes the CRF and CDE as a KGX file.
#

# Python libraries
import json
from json import JSONDecodeError

import click
import csv
import urllib

from kgx.graph.nx_graph import NxGraph
from kgx.transformer import Transformer
from kgx.source import graph_source
from kgx.sink import jsonl_sink

import requests

# Add logging support
import logging

logging.basicConfig(level=logging.INFO)

# We read config from `.env`.
import os
from dotenv import dotenv_values
config = {
    **dotenv_values(".env.default"),  # default configuration
    **dotenv_values(".env"),          # override with user-specific® configuration
    **os.environ,                     # override loaded values with environment variables
}

# Set up Requests to retry failed connections.
session = requests.Session()
http_adapter = requests.adapters.HTTPAdapter(max_retries=10)
session.mount('http://', http_adapter)
session.mount('https://', http_adapter)

# Ignore certain concepts and categories.
IGNORED_CONCEPTS = set(
    'UBERON:0002542',                   # "scale" -- not a body part!
    'UBERON:0007380',                   # "dermal scale" -- not a body part!
    'MONDO:0019395',                    # "have" is not Hinman syndrome
    'UBERON:0006611',                   # "test" is not an exoskeleton
    'PUBCHEM.COMPOUND:135398658',       # "being" is not folic acid
    'MONDO:0010472',                    # "being" is not developmental and epileptic encephalopathy
    'MONDO:0011760',                    # "being" is not Scheie syndrome
    'MONDO:0008534',                    # "getting" is not generalized essential telangiectasia
    'UBERON:0004704',                   # "depression" is not bone fossa
    'UBERON:0000467',                   # "system" is not an anatomical system
    'MONDO:0017879',                    # "have" is not hantavirus pulmonary syndrome
    'MONDO:0009271',                    # "going" or "goes" is not geroderma osteodysplastica
    'MONDO:0017015',                    # "children" is not primary interstitial lung disease specific to childhood
    'CHEBI:24433',                      # "rested" or "group" aren't a chemical group
    'MONDO:0010953',                    # "face" is not Fanconi anemia complementation group E
    'GO:0043336',                       # "rested" is not site-specific telomere resolvase activity
    'NCBIGene:5978',                    # "rested" is not the gene REST
    'MONDO:0009176',                    # "ever" is not epidermodysplasia verruciformis
    'GO:0019013',                       # "core" is not viral nucleocapsid
    'MONDO:0012833',                    # "could" is not Crouzon syndrome-acanthosis nigricans syndrome (): 18 CRFs
    'NCBITaxon:6754',                   # "cancer" is not the animal group Cancer
    'UBERON:0004529',                   # "spine" is not anatomical projection
    'UBERON:0013496',                   # "spine" is not unbarbed keratin-coated spine
    'MONDO:0000605',                    # "sensitive" is not hypersensitivity reaction disease
    'PUBCHEM.COMPOUND:84815',           # "meds" is not D-Methionine
    'MONDO:0016648',                    # "meds" is not multiple epiphyseal dysplasia (disease)
    'GO:0044326',                       # "neck" is not dendritic spine neck
    'UBERON:2002175',                   # "role" is not rostral octaval nerve motor nucleus
    'MONDO:0002531',                    # "skin" is not skin neoplasm
    'MONDO:0024457',                    # "plans" is not neurodegeneration with brain iron accumulation 2A
    'MONDO:0017998',                    # "plans" is not PLA2G6-associated neurodegeneration
    'UBERON:0004111',                   # "open" is not anatomical conduit
    'MONDO:0002169',                    # "read" is not rectum adenocarcinoma
    'UBERON:0007358',                   # "read" is not abomasum
    'CHEBI:18282',                      # "based" is not nucleobase
    'UBERON:0035971',                   # "post" is not postsubiculum
    'GO:0033867',                       # "fast" is not Fas-activated serine/threonine kinase activity
    'CHEMBL.COMPOUND:CHEMBL224120',     # "same" is not CHEMBL224120
    'MONDO:0019380',                    # "weed" is not western equine encephalitis
    'CL:0000968',                       # "cell" is not Be cell
    'PR:000004978',                     # "calm" is not calmodulin
    'GO:0008705',                       # "meth" is not methionine synthase activity
    'UBERON:0001932',                   # "arch" is not arcuate nucleus of hypothalamus
    'MONDO:0004980',                    # "allergy" is not atopic eczema
    'MONDO:0009994',                    # "arms" are not alveolar rhabdomyosarcoma
    # Stopped at decompression sickness (MONDO:0020797): 5 CRFs
)

# Some URLs we use.
TRANSLATOR_NORMALIZATION_URL = 'https://nodenormalization-sri.renci.org/1.2/get_normalized_nodes'
MONARCH_API_URI = 'https://api.monarchinitiative.org/api/nlp/annotate/entities'


def get_id_for_heal_crf(filename):
    """ Get an ID for a HEAL CRF. """
    return f'HEALCDE:{urllib.parse.quote(filename)}'


def get_designation(element):
    """ Return the designations for a CDE. If any designations are present, we concatenate them together so they can be
    passed to the Monarch API in a single API call.
    """
    if 'designations' in element:
        return '; '.join(map(lambda d: d['designation'], element['designations']))
    else:
        return ''


def normalize_curie(curie):
    """
    Normalize a CURIE and return the information about that term.
    API docs at https://nodenormalization-sri.renci.org/docs#/default/get_normalized_node_handler_get_normalized_nodes_post

    :param curie: The CURIE to be normalized.
    :return: The normalization results, including the normalized curie and categorization information.
    """

    try:
        result = session.post(TRANSLATOR_NORMALIZATION_URL, json={
            'curies': [curie]
        })
    except Exception as err:
        logging.error(f"Could not read Node Normalization POST result for curie '{curie}': {err}")
        return None

    try:
        results = result.json()
        # logging.info(f"Result: {results}")
        if curie in results:
            return results[curie]
        else:
            return None
    except JSONDecodeError as err:
        logging.error(f"Could not parse Node Normalization POST result for curie '{curie}': {err}")


def ner_via_monarch_api(text, included_categories=[], excluded_categories=[]):
    """
    Query the Monarch API to do NER on a string via https://api.monarchinitiative.org/api/#operations-nlp/annotate-post_annotate.

    :param text: The text to run NER on
    :param included_categories: The categories of content to include (see API docs).
    :param excluded_categories: The categories of content to exclude (see API docs).
    :return: The response from the NER service, translated into token definitions.
    """

    try:
        result = session.post(MONARCH_API_URI, {
            'content': text,
            'include_category': included_categories,
            'exclude_category': excluded_categories,
            'min_length': 4,
            'longest_only': True,
            'include_abbreviation': False,
            'include_acronym': False,
            'include_numbers': False
        })
    except Exception as err:
        logging.error(f"Could not read Monarch NER POST result for text '{text}': {err}")
        return []

    try:
        json = result.json()
    except JSONDecodeError as err:
        logging.error(f"Could not parse Monarch NER POST result for text '{text}': {err}")
        json = {
            'spans': []
        }

    tokens = []
    spans = json['spans']
    logging.info(f"Querying Monarch API for '{text}' produced the following tokens:")
    for span in spans:
        for token in span['token']:
            token_definition = dict(
                text=span['text'],
                id=token['id'],
                categories=token['category'],
                terms=token['terms']
            )

            logging.debug(f" - [{token['id']}] \"{token['terms']}\": {token_definition}")

            normalized = normalize_curie(token['id'])
            if normalized:
                token_definition['normalized'] = normalized
                logging.debug(f"   - Normalized to {normalized['id']['identifier']} '{normalized['id']['label']}' of types {normalized['type']}")

            tokens.append(token_definition)

    return tokens


# Number of associations in this file.
association_count = 0
# Numbers of errors (generally terms without a valid ID).
error_count = 0
# Terms ignored.
ignored_count = 0


def process_crf(graph, filename, crf):
    """
    Process a CRF. We need to recursively process the CDEs as well.

    :param graph: A KGX graph to add the CRF to.
    :param filename: The filename being processed.
    :param crf: The CRF in JSON format to process.
    :return: None. It modifies graph and writes outputs to STDOUT. Disgusting!
    """

    crf_id = get_id_for_heal_crf(filename)
    designation = get_designation(crf)

    # Generate text for the entire form in one go.
    crf_text = designation + "\n"
    for element in crf['formElements']:
        question_text = element['label']
        crf_text += question_text

        if 'question' in element and 'cde' in element['question']:
            crf_text += f" (name: {element['question']['cde']['name']})"

            if 'newCde' in element['question']['cde']:
                definitions = element['question']['cde']['newCde'].get('definitions') or []
                for definition in definitions:
                    if 'sources' in definition:
                        crf_text += f" (definition: {definition['definition']}, sources: {'; '.join(definition['sources'])})"
                    else:
                        crf_text += f" (definition: {definition['definition']})"

        crf_text += "\n"

    crf_name = crf['designations'][0]['designation']

    graph.add_node(crf_id)
    graph.add_node_attribute(crf_id, 'name', crf_name)
    graph.add_node_attribute(crf_id, 'summary', designation)
    graph.add_node_attribute(crf_id, 'category', ['biolink:Publication'])
    # graph.add_node_attribute(crf_id, 'summary', crf_text)

    tokens = ner_via_monarch_api(crf_text)
    logging.info(f"Querying CRF '{designation}' with text: {crf_text}")
    for token in tokens:
        logging.info(f"Found token: {token}")

        if graph and 'normalized' in token:
            # Create the NamedThing that is the token.
            if 'id' in token['normalized'] and 'identifier' in token['normalized']['id']:
                term_id = token['normalized']['id']['identifier']
            else:
                global error_count
                error_count += 1

                term_id = f'ERROR:{error_count}'

            if term_id in IGNORED_CONCEPTS:
                global ignored_count
                ignored_count += 1

                logging.info(f'Ignoring concept {term_id} as it is on the list of ignored concepts')
                continue

            graph.add_node(term_id)
            graph.add_node_attribute(term_id, 'category', token['normalized']['type'])
            graph.add_node_attribute(term_id, 'name', (token['normalized'].get('id') or {'label': 'ERROR'}).get('label'))
            graph.add_node_attribute(term_id, 'provided_by', 'Monarch NER service + Translator normalization API')
                                     # f'Monarch NER service ({MONARCH_API_URI}) + Translator normalization API ({TRANSLATOR_NORMALIZATION_URL})')

            # Add an edge/association between the CRF and the term.
            global association_count
            association_count += 1

            association_id = f'HEALCDE:edge_{association_count}'

            graph.add_edge(crf_id, term_id, association_id)
            graph.add_edge_attribute(crf_id, term_id, association_id, 'category', ['biolink:InformationContentEntityToNamedThingAssociation'])
            graph.add_edge_attribute(crf_id, term_id, association_id, 'name', token['text'])
            graph.add_edge_attribute(crf_id, term_id, association_id, 'knowledge_source', 'Monarch NER service + Translator normalization API')
                                     # f'Monarch NER service ({MONARCH_API_URI}) + Translator normalization API ({TRANSLATOR_NORMALIZATION_URL})')
            # graph.add_edge_attribute(crf_id, term_id, association_id, 'description', f"NER found '{token['text']}' in CRF text '{crf_text}'")

            graph.add_edge_attribute(crf_id, term_id, association_id, 'subject', crf_id)
            graph.add_edge_attribute(crf_id, term_id, association_id, 'predicate', 'biolink:mentions') # https://biolink.github.io/biolink-model/docs/mentions.html
            graph.add_edge_attribute(crf_id, term_id, association_id, 'predicate_label', 'mentions')

            graph.add_edge_attribute(crf_id, term_id, association_id, 'object', term_id)

# Process input commands
@click.command()
@click.option('--output', '-o', type=click.File(
    mode='w'
), default='-')
@click.argument('input-dir', type=click.Path(
    exists=True,
    file_okay=False,
    dir_okay=True,
    allow_dash=False
))
@click.argument('cde-mappings-csv', type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=False,
    allow_dash=False
))
@click.option('--to-kgx', type=click.Path(
    file_okay=True,
    dir_okay=False
))
def main(input_dir, output, cde_mappings_csv, to_kgx):
    input_path = click.format_filename(input_dir)
    csv_table_path = click.format_filename(cde_mappings_csv)

    # Set up the KGX graph
    graph = NxGraph()

    # Read the CSV table.
    cde_mappings = {}
    with open(csv_table_path, 'r') as f:
        for row in csv.DictReader(f):
            cde_mappings[row['Filename']] = row

    count_files = 0
    count_elements = 0

    iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
    for root, dirs, files in iterator:
        logging.debug(f' - Recursing into directory {root}')
        for filename in files:
            if filename.lower().endswith('.json'):
                filepath = os.path.join(root, filename)
                logging.debug(f'   - Found {filepath}')

                # Load JSON file.
                with open(filepath, 'r') as f:
                    count_files += 1

                    crf = json.load(f)

                    mapped_cde = {}
                    if filename in cde_mappings:
                        mapped_cde = cde_mappings[filename]

                    process_crf(graph, filename, crf)

    output.close()

    if to_kgx:
        kgx_filename = click.format_filename(to_kgx)
        t = Transformer()
        t.process(
            source=graph_source.GraphSource().parse(graph),
            sink=jsonl_sink.JsonlSink(kgx_filename)
        )

    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )
    if error_count > 0:
        logging.error(f'Note that {error_count} terms resulted in errors.')
    if ignored_count > 0:
        logging.warning(f'Note that {ignored_count} terms were ignored.')


if __name__ == '__main__':
    main()
