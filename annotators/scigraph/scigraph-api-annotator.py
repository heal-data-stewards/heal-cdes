#!/usr/bin/env python3
#
# scigraph-api-annotator.py <input directory of JSON files>
#
# Given an input directory of JSON files, produces a list of annotations for each CRF and CDE as a CSV.
# Optionally, it can produce a YAML file that describes the CRF and CDE as a KGX file.
#

# Python libraries
import json
import click
import csv
import urllib
from kgx.graph.base_graph import BaseGraph
import biolink.model
from biolinkml.dumpers import yaml_dumper

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
    TRANSLATOR_NORMALIZATION_URL = 'https://nodenormalization-sri.renci.org/1.2/get_normalized_nodes'

    result = requests.post(TRANSLATOR_NORMALIZATION_URL, json={
        'curies': [curie]
    })

    results = result.json()
    # logging.info(f"Result: {results}")
    if curie in results:
        return results[curie]
    else:
        return None


def ner_via_monarch_api(text, included_categories=[], excluded_categories=[]):
    """
    Query the Monarch API to do NER on a string via https://api.monarchinitiative.org/api/#operations-nlp/annotate-post_annotate.

    :param text: The text to run NER on
    :param included_categories: The categories of content to include (see API docs).
    :param excluded_categories: The categories of content to exclude (see API docs).
    :return: The response from the NER service, translated into token definitions.
    """

    MONARCH_API_URI = 'https://api.monarchinitiative.org/api/nlp/annotate/entities'
    result = requests.post(MONARCH_API_URI, {
        'content': text,
        'include_category': included_categories,
        'exclude_category': excluded_categories,
        'min_length': 4,
        'longest_only': True,
        'include_abbreviation': False,
        'include_acronym': False,
        'include_numbers': False
    })

    json = result.json()

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


def process_element(biolink_crf, filename, element):
    """
    Process a CDE.

    :param biolink_crf: The BioLink CRF that this element is a part of.
    :param filename: The filename being processed.
    :param element: The element being processed.
    :return: None.
    """

    cde = biolink.model.Publication(f'{get_id_for_heal_crf(filename)}#cde_{crf_index}', element['label'], type=
        'https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C19984'
    )

    if 'has_part' not in biolink_crf:
        biolink_crf['has_part'] = []

    biolink_crf['has_part'].append(cde)

    # Additional properties
    if 'mapped_id' in mapped_element and mapped_element['mapped_id'] != '':
        # We have mappings!
        if 'related_to' not in cde:
            cde['related_to'] = []

        publication = biolink.model.Publication(mapped_element['mapped_id'], mapped_element['mapped_text'], [
            'https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C19984'
        ])
        if 'mapped_copyright' in mapped_element:
            publication['rights'] = mapped_element.get('mapped_copyright') or ''

        # Add the related names as keywords
        if 'mapped_related_codes' in mapped_element:
            publication['keywords'] = mapped_element.get('mapped_related_codes') or []

        cde['related_to'].append(publication)

    return cde

    pv = biolink.model.Publication(f'{get_id_for_heal_crf(filename)}#cde_{crf_index}_pv_{pv_index}', pvalue, [
        'https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C41109'
    ])

    if 'has_part' not in biolink_cde:
        biolink_cde['has_part'] = []

    biolink_cde['has_part'].append(pv)

    tokens = ner_via_monarch_api(
        (element.get('label') or '') + "\n" + (element.get('definition') or '')
    )

# Number of associations in this file.
association_count = 0
named_thing_count = 0


def process_crf(dataset, filename, crf):
    """
    Process a CRF. We need to recursively process the CDEs as well.

    :param dataset: A BioLink dataset to add the CRF to.
    :param filename: The filename being processed.
    :param crf: The CRF in JSON format to process.
    :return: None. It modifies dataset and writes outputs to STDOUT. Disgusting!
    """

    crf_id = get_id_for_heal_crf(filename)
    designation = get_designation(crf)

    biolink_crf = biolink.model.Publication(crf_id, designation,
        type='https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C40988',
        category=['https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C40988']
    )

    # Generate text for the entire form in one go.
    crf_text = designation + "\n"
    for element in crf['formElements']:
        question_text = element['label']
        crf_text += question_text + "\n"

    tokens = ner_via_monarch_api(crf_text)
    logging.info(f"Querying CRF '{designation}' with text: {crf_text}")
    for token in tokens:
        logging.info(f"Found token: {token}")

        if dataset and 'normalized' in token:
            if 'has_part' not in dataset:
                dataset['has_part'] = []

            global association_count
            association_count += 1
            dataset['has_part'].append(biolink.model.Association(
                # category='biolink:InformationContentEntityToNamedThingAssociation',
                id=f'HEALCDE:association_{association_count}',
                subject=crf_id,
                predicate='http://purl.obolibrary.org/obo/IAO_0000142', # IAO:mentions
                object=biolink.model.NamedThing(
                    id=(token['normalized'].get('id') or {'identifier': 'ERROR'}).get('identifier'),
                    name=(token['normalized'].get('id') or {'label': 'ERROR'}).get('label'),
                    category=token['normalized']['type']
                )
            ))

    # for (crf_index, element) in enumerate(crf['formElements']):
    #     question = element['question']
    #     cde = question['cde']
    #
    #     # Additional properties
    #     if 'mapped_id' in mapped_crf and mapped_crf['mapped_id'] != '':
    #         # We have mappings!
    #         if 'related_to' not in crf:
    #             crf['related_to'] = []
    #
    #         publication = biolink.model.Publication(mapped_crf['mapped_id'], mapped_crf['mapped_text'], [
    #             'https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C40988',
    #             'https://loinc.org/panels/'
    #         ])
    #         if 'mapped_copyright' in mapped_crf:
    #             publication['rights'] = mapped_crf.get('mapped_copyright') or ''
    #
    #         # Add the related names as keywords
    #         if 'mapped_related_codes' in mapped_crf:
    #             publication['keywords'] = mapped_crf.get('mapped_related_codes') or []
    #
    #         crf['related_to'].append(publication)

    # Add to dataset.
    if dataset:
        if 'has_part' not in dataset:
            dataset['has_part'] = []

        dataset['has_part'].append(biolink_crf)


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

    # Set up the top-level
    dataset = biolink.model.Dataset('heal_cdes', 'HEALCDE:cdes', [
        'https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C19984'
    ])

    # Read the CSV table.
    cde_mappings = {}
    with open(csv_table_path, 'r') as f:
        for row in csv.DictReader(f):
            cde_mappings[row['Filename']] = row

    # Set up the CSV writer.
    writer = csv.writer(output)
    writer.writerow([
        'filename',
        'filepath',
        'designation',
        'question',
        'permissible_value',
        'child_count',
        'mapped_id',
        'mapped_text',
        'mapped_copyright',
        'mapped_child_count'
    ])

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

                    process_crf(dataset, filename, crf)

    output.close()

    if to_kgx:
        kgx_filename = click.format_filename(to_kgx)
        yaml_dumper.dump(dataset, kgx_filename)

    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )


if __name__ == '__main__':
    main()
