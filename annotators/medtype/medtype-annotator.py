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

IGNORED_CONCEPTS = set(
    'PUBCHEM.COMPOUND:7216'     # This is 'CDE'.
)

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

# Some URLs we use.
TRANSLATOR_NORMALIZATION_URL = 'https://nodenormalization-sri.renci.org/1.2/get_normalized_nodes'
MEDTYPE_API_URI = 'http://localhost:8125/run_linker'


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

def ner_via_medtype(text, id, entity_linker='scispacy'):
    """
    Query the MedType API to do NER on a string.

    :param text: The text to run NER on.
    :param id: The ID to use for this request.
    :param entity_linker: The entity linker to use.
    """

    try:
        result = session.post(MEDTYPE_API_URI, json={
            'id': id,
            'data': {
                'text': [text],
                'entity_linker': entity_linker
            }
        })
    except Exception as err:
        logging.error(f"Could not read MedType NER POST result for {id} with text '{text}': {err}")
        return []

    try:
        json = result.json()
        mentions = json['result']['elinks'][0]['mentions']
    except JSONDecodeError as err:
        logging.error(f"Could not parse MedType NER POST result for {id} with text '{text}': {err}")
        mentions = []

    logging.info(f"Querying MedType API for '{text}' produced the following tokens:")
    tokens = []
    for mention in mentions:
        ids = list(map(lambda fc: fc[0], mention['filtered_candidates']))
        category = mention['pred_type']
        text = mention['mention']
        begin = mention['start_offset']
        end = mention['end_offset']

        logging.debug(f" - {ids} \"{text}\"")

        for id in ids:
            if id.startswith('C'):
                id = 'UMLS:' + id

            token_definition = dict(
                text=text,
                id=id,
                categories=[category],
                terms=[text]
            )
            normalized = normalize_curie(id)
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

    tokens = ner_via_medtype(crf_text, crf_id)
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
            graph.add_node_attribute(term_id, 'provided_by', 'MedType NER service + Translator normalization API')
                                     # f'Monarch NER service ({MONARCH_API_URI}) + Translator normalization API ({TRANSLATOR_NORMALIZATION_URL})')

            # Add an edge/association between the CRF and the term.
            global association_count
            association_count += 1

            association_id = f'HEALCDE:edge_{association_count}'

            graph.add_edge(crf_id, term_id, association_id)
            graph.add_edge_attribute(crf_id, term_id, association_id, 'category', ['biolink:InformationContentEntityToNamedThingAssociation'])
            graph.add_edge_attribute(crf_id, term_id, association_id, 'name', token['text'])
            graph.add_edge_attribute(crf_id, term_id, association_id, 'knowledge_source', 'MedType NER service + Translator normalization API')
                                     # f'Monarch NER service ({MONARCH_API_URI}) + Translator normalization API ({TRANSLATOR_NORMALIZATION_URL})')
            # graph.add_edge_attribute(crf_id, term_id, association_id, 'description', f"NER found '{token['text']}' in CRF text '{crf_text}'")

            graph.add_edge_attribute(crf_id, term_id, association_id, 'subject', crf_id)
            graph.add_edge_attribute(crf_id, term_id, association_id, 'predicate', 'biolink:mentions') # https://biolink.github.io/biolink-model/docs/mentions.html
            graph.add_edge_attribute(crf_id, term_id, association_id, 'predicate_label', 'mentions')

            graph.add_edge_attribute(crf_id, term_id, association_id, 'object', term_id)

# Process individual file
count_files = 0

def process_file(filepath, cde_mappings, graph):
    filename = os.path.basename(filepath)

    # Load JSON file.
    with open(filepath, 'r') as f:
        global count_files
        count_files += 1

        crf = json.load(f)

        mapped_cde = {}
        if filename in cde_mappings:
            mapped_cde = cde_mappings[filename]

        process_crf(graph, filename, crf)

# Process input commands
@click.command()
@click.option('--output', '-o', type=click.File(
    mode='w'
), default='-')
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
def main(input, output, cde_mappings_csv, to_kgx):
    input_path = click.format_filename(input)
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

    if os.path.isfile(input_path):
        # If the input is a single file, process just that one file.
        process_file(input_path, cde_mappings, graph)
    else:
        # If it is a directory, then recurse through that directory looking for input files.
        iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
        for root, dirs, files in iterator:
            logging.debug(f' - Recursing into directory {root}')
            for filename in files:
                if filename.lower().endswith('.json'):
                    filepath = os.path.join(root, filename)
                    logging.debug(f'   - Found {filepath}')

                    process_file(filepath, cde_mappings, graph)

    if to_kgx:
        kgx_filename = click.format_filename(to_kgx)
        t = Transformer()
        t.process(
            source=graph_source.GraphSource(owner=t).parse(graph),
            sink=jsonl_sink.JsonlSink(owner=t, filename=kgx_filename)
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
