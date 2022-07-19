#!/usr/bin/env python3
#
# oger-annotator.py <input JSON file> --terms 'terms/*.txt' --n-workers 10
#
# Given an input directory of JSON files or a single JSON file, this script will produce a list of annotations for each
# CRF and CDE as KGX.
#

# Python libraries
import glob
import io
import json
from itertools import chain
from json import JSONDecodeError

import click
import csv
import urllib

import oger.doc
from oger.ctrl.router import PipelineServer, Router

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
    **dotenv_values(".env"),  # override with user-specific® configuration
    **os.environ,  # override loaded values with environment variables
}

# Set up Requests to retry failed connections.
session = requests.Session()
http_adapter = requests.adapters.HTTPAdapter(max_retries=10)
session.mount('http://', http_adapter)
session.mount('https://', http_adapter)

# Some URLs we use.
TRANSLATOR_NORMALIZATION_URL = 'https://nodenormalization-sri.renci.org/1.2/get_normalized_nodes'

# Some concepts that are always ignored.
IGNORED_CONCEPTS = [

]
PREFIXES = {
    'http://www.ebi.ac.uk/efo/EFO_': 'EFO:'
}

# TODO: add language to ID.
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


def ner_via_oger(text, oger_pipeline, id=""):
    """
    Query OGER to do NER on a string.

    :param text: The text to run NER on.
    """

    txt_loader = oger.doc.TXTLoader(oger_pipeline.conf)

    stream = io.StringIO(text)
    doc = txt_loader.document(stream, id)
    oger_pipeline.process(doc)

    tokens = []

    if tokens:
        # Some debugging code for investigating how OGER presents results.
        def entity_as_str(entity):
            return json.dumps({
                'type': entity.type,
                'pref': entity.pref,
                'db': entity.db,
                'cid': entity.cid,
                'cui': entity.cui,
                'extra': entity.extra,
                'info': list(entity.info_items())
            })

        def recurse_subelements(t, level=0):
            print((" " * level) + " - [" + str(type(t)) + "] " + str(t))
            if hasattr(t, 'entities'):
                for entity in (t.entities or []):
                    print((" " * level) + "   -> [Entity] " + entity_as_str(entity))
            for element in (t.subelements or []):
                recurse_subelements(element, level + 1)

        recurse_subelements(doc)

    for entity in doc.iter_entities():
        entity_id = entity.cid
        is_synonym = False
        if entity_id.endswith('_SYNONYM'):
            entity_id = entity_id[:-8]
            is_synonym = True

        for prefix in PREFIXES.keys():
            if entity_id.startswith(prefix):
                entity_id = PREFIXES[prefix] + entity_id[len(prefix):]

        token_definition = dict(
            text=entity.pref,
            id=entity_id,
            is_synonym=is_synonym,
            categories=[entity.type]
        )
        normalized = normalize_curie(id)
        if normalized:
            token_definition['normalized'] = normalized
            logging.debug(
                f"   - Normalized to {normalized['id']['identifier']} '{normalized['id']['label']}' of types {normalized['type']}")

        tokens.append(token_definition)

    return tokens


# Number of associations in this file.
association_count = 0
# Count files.
count_files = 0
# Count elements.
count_elements = 0
# Count terms.
count_tokens = 0
# Numbers of errors (generally terms without a valid ID).
count_errors = 0
# Terms ignored.
count_ignored = 0
# Terms normalized.
count_normalized = 0
# Terms that were not normalized.
count_not_normalized = 0


def process_crf(graph, filename, crf, oger_pipeline):
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

    # Let's figure out how to categorize this CDE. We'll record two categories:
    # - 1. Let's create a `cde_categories` attribute that will be a list of all the categories
    #   we know about. This is the most comprehensive option, but is also likely to lead to
    #   incomplete categories such as "English", "Adult" and so on.
    file_paths = filter(lambda d: d['designation'].startswith('File path: '), crf['designations'])
    # chain.from_iterable() effectively flattens the list.
    categories = list(chain.from_iterable(map(lambda d: d['designation'][11:].split('/'), file_paths)))
    logging.info(f"Categories for CDE {crf_name}: {categories}")
    graph.add_node_attribute(crf_id, 'cde_categories', list(categories))
    # - 2. Let's create a `cde_category` property that summarizes the longlist of categories into
    #   the categories created in https://www.jpain.org/article/S1526-5900(21)00321-7/fulltext#tbl0001

    # The top-level category should tell us if it's core or not.
    core_or_not = categories[0]

    # Is this adult or pediatric?
    flag_has_adult_pediatric = False
    if 'Adult' in categories:
        adult_or_pediatric = 'Adult'
        flag_has_adult_pediatric = True
    elif 'Pediatric' in categories:
        adult_or_pediatric = 'Pediatric'
        flag_has_adult_pediatric = True
    else:
        adult_or_pediatric = 'Adult/Pediatric'
        logging.error(f"Could not determine if adult or pediatric from categories: {categories}")

    # Is this relating to acute or chronic pain?
    flag_has_acute_chronic = False
    if 'Acute Pain' in categories:
        acute_or_chronic_pain = 'Acute Pain'
        flag_has_acute_chronic = True
    elif 'Chronic Pain' in categories:
        acute_or_chronic_pain = 'Chronic Pain'
        flag_has_acute_chronic = True
    else:
        acute_or_chronic_pain = 'Acute/Chronic Pain'
        logging.error(f"Could not determine if acute or chronic pain from categories: {categories}")

    # Filter out any final categories that aren't the most specific category.
    if categories[-1] == 'English' or categories[-1] == 'Spanish':
        # We're not interested in these.
        del categories[-1]

    # The last category should now be the most specific category.
    graph.add_node_attribute(crf_id, 'cde_category_extended', [
        core_or_not,
        adult_or_pediatric,
        acute_or_chronic_pain,
        categories[-1]
    ])

    # Let's summarize all of this into a single category (as per
    # https://github.com/helxplatform/development/issues/868#issuecomment-1072485659)
    if flag_has_adult_pediatric:
        if flag_has_acute_chronic:
            cde_category = f"{acute_or_chronic_pain} ({adult_or_pediatric})"
        else:
            cde_category = adult_or_pediatric
    else:
        if flag_has_acute_chronic:
            cde_category = acute_or_chronic_pain
        else:
            cde_category = core_or_not

    graph.add_node_attribute(crf_id, 'cde_category', cde_category)
    logging.info(f"Categorized CRF {crf_name} as {cde_category}")

    crf['_ner'] = {
        'oger': {
            'crf_id': crf_id,
            'crf_name': crf_name,
            'crf_text': crf_text,
            'tokens': {
                'not_normalized': [],
                'errors': [],
                'ignored': [],
                'normalized': []
            }
        }
    }

    global count_tokens, count_ignored, count_errors, count_normalized, count_not_normalized

    tokens = ner_via_oger(crf_text, oger_pipeline, crf_id)
    logging.info(f"Querying CRF '{designation}' with text: {crf_text}")
    for token in tokens:
        count_tokens += 1
        logging.info(f"Found token: {token}")

        if graph and 'normalized' in token:
            # Create the NamedThing that is the token.
            if 'id' in token['normalized'] and 'identifier' in token['normalized']['id']:
                term_id = token['normalized']['id']['identifier']
            else:
                count_errors += 1
                crf['_ner']['oger']['tokens']['errors'].append(token)
                term_id = f'ERROR:{count_errors}'

            if term_id in IGNORED_CONCEPTS:
                logging.info(f'Ignoring concept {term_id} as it is on the list of ignored concepts')
                crf['_ner']['oger']['tokens']['ignored'].append(token)
                count_ignored += 1
                continue

            crf['_ner']['oger']['tokens']['normalized'].append(token)
            count_normalized += 1

            graph.add_node(term_id)
            graph.add_node_attribute(term_id, 'category', token['normalized']['type'])
            graph.add_node_attribute(term_id, 'name',
                                     (token['normalized'].get('id') or {'label': 'ERROR'}).get('label'))
            graph.add_node_attribute(term_id, 'provided_by', 'MedType NER service + Translator normalization API')
            # f'Monarch NER service ({MONARCH_API_URI}) + Translator normalization API ({TRANSLATOR_NORMALIZATION_URL})')

            # Add an edge/association between the CRF and the term.
            global association_count
            association_count += 1

            association_id = f'HEALCDE:edge_{association_count}'

            graph.add_edge(crf_id, term_id, association_id)
            graph.add_edge_attribute(crf_id, term_id, association_id, 'category',
                                     ['biolink:InformationContentEntityToNamedThingAssociation'])
            graph.add_edge_attribute(crf_id, term_id, association_id, 'name', token['text'])
            graph.add_edge_attribute(crf_id, term_id, association_id, 'knowledge_source',
                                     'MedType NER service + Translator normalization API')
            # f'Monarch NER service ({MONARCH_API_URI}) + Translator normalization API ({TRANSLATOR_NORMALIZATION_URL})')
            # graph.add_edge_attribute(crf_id, term_id, association_id, 'description', f"NER found '{token['text']}' in CRF text '{crf_text}'")

            graph.add_edge_attribute(crf_id, term_id, association_id, 'subject', crf_id)
            graph.add_edge_attribute(crf_id, term_id, association_id, 'predicate',
                                     'biolink:mentions')  # https://biolink.github.io/biolink-model/docs/mentions.html
            graph.add_edge_attribute(crf_id, term_id, association_id, 'predicate_label', 'mentions')

            graph.add_edge_attribute(crf_id, term_id, association_id, 'object', term_id)
        else:
            crf['_ner']['oger']['tokens']['not_normalized'].append(token)
            count_not_normalized += 1


def process_file(filepath, cde_mappings, graph, oger_pipeline):
    filename = os.path.basename(filepath)

    # Load JSON file.
    with open(filepath, 'r') as f:
        global count_files
        count_files += 1

        crf = json.load(f)

        mapped_cde = {}
        if filename in cde_mappings:
            mapped_cde = cde_mappings[filename]

        process_crf(graph, filename, crf, oger_pipeline)


# Process input commands
@click.command()
@click.argument('input', type=click.Path(
    exists=True,
    file_okay=True,
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
@click.option('--oger-terms', type=str, required=True, default='input/oger/terms/terms.tsv')
def oger_annotator(input, cde_mappings_csv, to_kgx, oger_terms):
    input_path = click.format_filename(input)
    csv_table_path = click.format_filename(cde_mappings_csv)

    # Set up OGER.
    conf = Router(
        include_header=True,
        article_format="txt_id",
        export_format="pubanno_json",
        termlist_path=oger_terms,
        termlist_stopwords='input/oger/stopwords/stopWords.txt',
        termlist_normalize='stem-Porter',
        postfilter='builtin:remove_overlaps builtin:remove_sametype_overlaps builtin:remove_submatches  builtin:remove_sametype_submatches'
    )
    oger_pipeline = PipelineServer(conf)

    # Set up the KGX graph
    graph = NxGraph()

    # Read the CSV table.
    cde_mappings = {}
    with open(csv_table_path, 'r') as f:
        for row in csv.DictReader(f):
            cde_mappings[row['Filename']] = row

    if os.path.isfile(input_path):
        # If the input is a single file, process just that one file.
        process_file(input_path, cde_mappings, graph, oger_pipeline)
    else:
        # If it is a directory, then recurse through that directory looking for input files.
        iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading file: {err}'),
                           followlinks=True)
        for root, dirs, files in iterator:
            logging.debug(f' - Recursing into directory {root}')
            for filename in files:
                if filename.lower().endswith('.json'):
                    filepath = os.path.join(root, filename)
                    logging.debug(f'   - Found {filepath}')

                    process_file(filepath, cde_mappings, graph, oger_pipeline)

    if to_kgx:
        kgx_filename = click.format_filename(to_kgx)
        t = Transformer()
        t.process(
            source=graph_source.GraphSource(owner=t).parse(graph),
            sink=jsonl_sink.JsonlSink(owner=t, filename=kgx_filename)
        )

    global count_files, count_elements, count_tokens, count_errors, count_ignored, count_normalized, count_not_normalized
    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )
    logging.info(
        f'Found {count_tokens} tokens, of which {count_errors} were errors, {count_ignored} were ignored, {count_normalized} was normalized and {count_not_normalized} was not normalized.'
    )


if __name__ == '__main__':
    oger_annotator()
