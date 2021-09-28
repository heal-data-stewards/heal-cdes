#!/usr/bin/env python3
#
# match-verifier.py <input.csv>
#
# Given an input CSV file containing proposed CRF or CDE matches, this script
# produces a CSV report that includes:
#   - Every question in the HEAL CDEs compared to the corresponding one in the source
#       - Including comparisons of the permissible values
#   - (Optionally) Classification information at all levels (CRF, CDE, PV)
#   - A score and textual description ("Exact", "Close", "Related", etc.) of the strength of the match.
#       - Separate scores are included for the entity itself (CRF, CDE, PV) and the score for the containing entities.
#   - Warnings of outdated versions being using.
#
# Optionally, it can write out a KGX file with the information from the input.
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
    **dotenv_values(".env.default"), # default configuration
    **dotenv_values(".env"),         # override with user-specific® configuration
    **os.environ,                    # override loaded values with environment variables
}

# How to get an ID for a HEAL CRF
def get_id_for_heal_crf(filename):
    return f'HEALCDE:{urllib.parse.quote(filename)}'

def get_designations(element):
    if 'designations' in element:
        return '; '.join(map(lambda d: d['designation'], element['designations']))
    else:
        return ''

# Retrieve info for a LOINC entry.
loinc_records = {}
def retrieve_data_from_loinc(loinc_id):
    if loinc_id in loinc_records:
        return loinc_records[loinc_id]

    result = requests.get(
        f'https://fhir.loinc.org/Questionnaire/?url=http://loinc.org/q/{loinc_id}',
        auth=(
            config['LOINC_USERNAME'],
            config['LOINC_PASSWORD']
        )
    )

    result_json = result.json()
    result_json['@id'] = f'LOINC:{loinc_id}'
    loinc_records[loinc_id] = result_json

    return result_json


# Retrieve info for a particular URL
def retrieve_url(url_raw: str):
    try:
        url = url_raw.strip()
        if url.startswith('https://loinc.org/'):
            if url.endswith('/'):
                return retrieve_data_from_loinc(url[18:-1])
            else:
                return retrieve_data_from_loinc(url[18:])
        if url.startswith('http://loinc.org/'):
            if url.endswith('/'):
                return retrieve_data_from_loinc(url[17:-1])
            else:
                return retrieve_data_from_loinc(url[17:])
        return {}
    except json.decoder.JSONDecodeError as err:
        logging.error(f'Could not decode response from URL {url_raw}, skipping: {err}')
        return {}


# Retrieve info for a particular row
def retrieve_data(mapped_cde):
    if (mapped_cde.get('Exact match URL') or '') != '':
        data = retrieve_url(mapped_cde.get('Exact match URL'))
        if (data.get('@id') or '') != '':
            return data
    if (mapped_cde.get('Close match URL') or '') != '':
        data = retrieve_url(mapped_cde.get('Close match URL'))
        if (data.get('@id') or '') != '':
            return data
    return {}


# Verify a CRF (no need to iterate into CDEs)
def verify_crf(mapped_cde, crf):
    data = retrieve_data(mapped_cde)

    if (data.get('@id') or '') != '':
        questionnaire = data
        entry = questionnaire['entry'][0]
        resource = entry['resource']
        items = resource.get('item') or []

        return {
            'mapped_id': entry['fullUrl'],
            'mapped_text': resource['title'],
            'mapped_copyright': resource['copyright'],
            'mapped_child_count': len(items)
        }

    return {}

# Add a CRF to KGX
def add_crf_to_kgx(dataset, filename, mapped_cde, crf, mapped_crf):
    crf = biolink.model.Publication(get_id_for_heal_crf(filename), get_designations(crf), [
        'https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C40988'
    ])

    if 'has_part' not in dataset:
        dataset['has_part'] = []

    dataset['has_part'].append(crf)

    return crf


# Verify a CDE (no need to iterate into PVs)
def verify_element(mapped_cde, crf, element):
    data = retrieve_data(mapped_cde)

    if (data.get('@id') or '') != '':
        questionnaire = data
        entry = questionnaire['entry'][0]
        resource = entry['resource']
        logging.info(f"Resource: {resource}")
        items = resource.get('item') or []
        for item in items:
            logging.info(f"Comparing \'{item['text']}\' with \'{element['label']}\'")
            if item['text'] == element['label']:
                result = {
                    'mapped_id': f"{item['code'][0]['system']}/{item['code'][0]['code']}",
                    'mapped_text': item['text']
                }

                options = item.get('answerOption') or []
                if len(options) > 0:
                    result['mapped_child_count'] = len(options)

                return result

    return {}


# Add an element to KGX
def add_element_to_kgx(biolink_crf, filename, crf_index, mapped_cde, crf, element, mapped_element):
    cde = biolink.model.Publication(f'{get_id_for_heal_crf(filename)}#cde_{crf_index}', element['label'], [
        'https://ncithesaurus.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code=C19984'
    ])

    if 'has_part' not in biolink_crf:
        biolink_crf['has_part'] = []

    biolink_crf['has_part'].append(cde)

# Verify a PV
def verify_pv(mapped_cde, crf, element, pv):
    data = retrieve_data(mapped_cde)

    pv_label = pv.get('valueMeaningDefinition') or pv.get('permissibleValue')

    if (data.get('@id') or '') != '':
        questionnaire = data
        entry = questionnaire['entry'][0]
        resource = entry['resource']
        logging.info(f"Resource: {resource}")
        items = resource.get('item') or []
        for item in items:
            logging.info(f"Comparing \'{item['text']}\' with \'{element['label']}\'")
            if item['text'] == element['label']:
                for option in item.get('answerOption'):
                    vc = option['valueCoding']
                    if vc['display'] == pv_label:
                        return {
                            'mapped_id': f"{vc['system']}/{vc['code']}",
                            'mapped_text': vc['display']
                        }

    return {}


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
    dataset = biolink.model.DataSet('heal_cdes', 'HEAL CDEs', [
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

                    crf_result = verify_crf(mapped_cde, crf)
                    crf_in_biolink = add_crf_to_kgx(dataset, filename, mapped_cde, crf, crf_result)

                    designations = crf['designations']
                    last_designation = designations[-1]['designation']

                    writer.writerow([
                        filename,
                        filepath,
                        last_designation,
                        '',
                        '',
                        len(crf.get('formElements') or []),
                        crf_result.get('mapped_id') or '',
                        crf_result.get('mapped_text') or '',
                        crf_result.get('mapped_copyright') or '',
                        crf_result.get('mapped_child_count') or ''
                    ])

                    for (crf_index, element) in enumerate(crf['formElements']):
                        element_result = verify_element(mapped_cde, crf, element)
                        if to_kgx:
                            add_element_to_kgx(crf_in_biolink, filename, crf_index, mapped_cde, crf, element, element_result)

                        id_infos = [] # get_id_infos(element)

                        count_elements += 1
                        question = element['question']
                        cde = question['cde']

                        question_text = element['label']

                        pvs = cde['permissibleValues']
                        if len(pvs) == 0:
                            writer.writerow([
                                filename,
                                filepath,
                                last_designation,
                                question_text,
                                '',
                                0,
                                element_result.get('mapped_id') or '',
                                element_result.get('mapped_text') or '',
                                element_result.get('mapped_copyright') or '',
                                element_result.get('mapped_child_count') or ''
                            ])
                        else:
                            writer.writerow([
                                filename,
                                filepath,
                                last_designation,
                                question_text,
                                '',
                                len(pvs),
                                element_result.get('mapped_id') or '',
                                element_result.get('mapped_text') or '',
                                element_result.get('mapped_copyright') or '',
                                element_result.get('mapped_child_count') or ''
                            ])

                            for pv in pvs:
                                pv_definition = pv.get('valueMeaningDefinition') or pv.get('permissibleValue')
                                pv_result = verify_pv(mapped_cde, crf, element, pv)
                                writer.writerow([
                                    filename,
                                    filepath,
                                    last_designation,
                                    question_text,
                                    pv_definition,
                                    '',
                                    pv_result.get('mapped_id') or '',
                                    pv_result.get('mapped_text') or '',
                                    pv_result.get('mapped_copyright') or '',
                                    pv_result.get('mapped_child_count') or ''
                                ])

    output.close()

    if to_kgx:
        kgx_filename = click.format_filename(to_kgx)
        yaml_dumper.dump(dataset, kgx_filename)

    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )


if __name__ == '__main__':
    main()
