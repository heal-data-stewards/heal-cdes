#!/usr/bin/env python3
#
# id-lookups.py <directory containing HEAL CDEs as JSON files>
#
# HEAL CDEs have external identifiers that point to "CDISC" entities via NCIt identifiers.
# It would be great to use this to enrich the data that we have, but most of the referenced NCIt
# identifiers lack any useful metadata: they only point to a single concept unconnected from any
# other.
#
# For the moment, this script's sole function is to confirm this by using the NCI Thesaurus API
# (i.e. the LexEVS API, e.g. https://lexevscts2.nci.nih.gov/lexevscts2/codesystem/NCI_Thesaurus/entity/C33999)
# to retrieve everything known about that concept in both the NCI Thesaurus (if possible) in the
# NCI Metathesaurus.

# Python libraries
import json
import click
import csv

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

# { 'url',
#        'label',
#        'definition',
#        'type',
#        'mappings_as_str'
# }
def get_ncit_info(id):
    return [{
        'id': id
    }]

# Look up identifiers
#{ 'url',
#        'label',
#        'definition',
#        'type',
#        'mappings_as_str'
#}
def get_id_infos(element):
    question = element['question']
    cde = question['cde']
    ids = cde['ids']

    results = []
    for id in ids:
        if id['source'] == 'NCIT':
            results.extend(get_ncit_info(id['id']))
        else:
            logging.warning(f"No lookup method for ID of type {id['source']}")

    return results

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
def main(input_dir, output):
    input_path = click.format_filename(input_dir)

    # Set up the CSV writer.
    writer = csv.writer(output)
    writer.writerow([
        'filename',
        'filepath',
        'designation',
        'question',
        'pv_count',
        'url',
        'label',
        'definition',
        'semantic_type',
        'mappings'
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

                    designations = crf['designations']
                    last_designation = designations[-1]['designation']

                    for element in crf['formElements']:
                        id_infos = get_id_infos(element)

                        count_elements += 1
                        question = element['question']
                        cde = question['cde']
                        pv_count = len(cde['permissibleValues'])

                        question_text = element['label']

                        if len(id_infos) == 0:
                            writer.writerow([
                                filename,
                                filepath,
                                last_designation,
                                question_text,
                                pv_count
                            ])
                        else:
                            for id_info in id_infos:
                                writer.writerow([
                                    filename,
                                    filepath,
                                    last_designation,
                                    question_text,
                                    pv_count,
                                    id_info.get('url') or '',
                                    id_info.get('label') or '',
                                    id_info.get('definition') or '',
                                    id_info.get('type') or '',
                                    id_info.get('mappings_as_str') or ''
                                ])

    output.close()

    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )


if __name__ == '__main__':
    main()
