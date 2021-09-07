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

# Python libraries
import json
import click
import csv

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

# Mapping result format:
#   mapped_id: The CDE this CRF/CDE/PV was mapped to.


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

        return {
            'mapped_id': entry['fullUrl'],
            'mapped_text': resource['title'],
            'mapped_copyright': resource['copyright']
        }

    return {}

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
                return {
                    'mapped_id': f"{item['code'][0]['system']}/{item['code'][0]['code']}",
                    'mapped_text': item['text']
                }

    return {}


# Verify a PV
def verify_pv(mapped_cde, crf, element, pv):
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
def main(input_dir, output, cde_mappings_csv):
    input_path = click.format_filename(input_dir)
    csv_table_path = click.format_filename(cde_mappings_csv)

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
        'mapped_id',
        'mapped_text',
        'mapped_copyright'
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

                    designations = crf['designations']
                    last_designation = designations[-1]['designation']

                    writer.writerow([
                        filename,
                        filepath,
                        last_designation,
                        '',
                        '',
                        crf_result.get('mapped_id') or '',
                        crf_result.get('mapped_text') or '',
                        crf_result.get('mapped_copyright') or ''
                    ])

                    for element in crf['formElements']:
                        element_result = verify_element(mapped_cde, crf, element)

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
                                element_result.get('mapped_id') or '',
                                element_result.get('mapped_text') or '',
                                element_result.get('mapped_copyright') or ''
                            ])
                        else:
                            writer.writerow([
                                filename,
                                filepath,
                                last_designation,
                                question_text,
                                '',
                                element_result.get('mapped_id') or '',
                                element_result.get('mapped_text') or '',
                                element_result.get('mapped_copyright') or ''
                            ])

                            for pv in pvs:
                                pv_definition = pv.get('valueMeaningDefinition') or pv.get('permissibleValue')
                                pv_result = verify_pv(mapped_cde, crf, cde, pv)
                                writer.writerow([
                                    filename,
                                    filepath,
                                    last_designation,
                                    question_text,
                                    pv_definition,
                                    pv_result.get('mapped_id') or '',
                                    pv_result.get('mapped_text') or '',
                                    pv_result.get('mapped_copyright') or ''
                                ])

    output.close()

    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )


if __name__ == '__main__':
    main()
