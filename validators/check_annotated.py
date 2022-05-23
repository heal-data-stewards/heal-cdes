#
# This script provides some statistics on the annotations generated within the `annotated/`
# directory, and provides warnings of unannotated files (and, optionally, deletes them so
# that they can be regenerated).
#

# Python libraries
import json
import re

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

count_cde_nodes = 0

def validate_nodes(filepath):
    global count_cde_nodes

    count_cdes = 0
    count_concepts = 0

    with open(filepath, 'r') as f:
        for line in f:
            row = json.loads(line)
            if "biolink:Publication" in row['category']:
                count_cdes += 1
                count_cde_nodes += 1
            elif "biolink:NamedThing" in row['category']:
                count_concepts += 1
            else:
                logging.error(f"Unable to classify node in ${filepath}: ${json.dumps(line, indent=2)}")

    logging.info(f"${filepath} contains ${count_cdes} CDEs and ${count_concepts} concepts.")


def validate_edges(filepath):
    pass


def validate_comprehensive(filepath):
    return

    # Load JSON file.
    with open(filepath, 'r') as f:
        count_files += 1

        cde = json.load(f)
        designations = cde['designations']
        last_designation = designations[-1]['designation']
        form_elements = cde['formElements']

        if len(form_elements) == 0:
            logging.error(f'File {filepath} contains no form elements.')

        for index, element in enumerate(form_elements, 1):
            element_type = element['elementType']

            if element_type != 'question':
                count_not_questions += 1
                logging.error(
                    f'File {filepath}, form element {index} is of an unknown type: {element_type}.')
                continue

            count_elements += 1

            element_instructions = element.get('instructions') or {}
            element_instruction = element_instructions.get('value') or ''
            question = element['question']
            cde = question['cde']
            new_cde = cde['newCde']
            cde_definitions = new_cde['definitions']
            cde_designations = new_cde['designations']
            cde_variable_names = list(map(
                lambda d: d['designation'][15:],
                filter(lambda d: d['designation'].startswith('Variable name: '), cde_designations)))


# Process input commands
@click.command()
@click.argument('input-dir', required=True, type=click.Path(
    exists=True,
    file_okay=False,
    dir_okay=True,
    allow_dash=False
))
def main(input_dir):
    input_path = click.format_filename(input_dir)

    count_files = 0
    count_node_files = 0
    count_edge_files = 0
    count_comprehensive_files = 0
    count_unable_to_validate = 0

    iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
    for root, dirs, files in iterator:
        logging.info(f' - Recursing into directory {root}')
        for filename in files:
            filepath = os.path.join(root, filename)
            filename_lower = filename.lower()

            # Ignore non-JSONL files.
            if not filename_lower.endswith('.jsonl'):
                logging.info(f'   - Skipping {filepath}')
                continue

            # Ignore the overall 'output_' files.
            if filename_lower.startswith('output_'):
                logging.info(f'   - Skipping {filepath}')
                continue

            count_files += 1

            # Which of the three JSONL files do we have?
            if filename_lower.endswith('_nodes.jsonl'):
                count_node_files += 1
                validate_nodes(filepath)
            elif filename_lower.endswith('_edges.jsonl'):
                count_edge_files += 1
                validate_edges(filepath)
            elif filename_lower.endswith('_comprehensive.jsonl'):
                count_comprehensive_files += 1
                validate_comprehensive(filepath)
            else:
                count_unable_to_validate += 1
                logging.warning(f"Unable to validate '${filepath}': unable to determine file path.")

    global count_cde_nodes

    logging.info(f'Checked {count_files} files:')
    logging.info(f' - Node files: {count_node_files} containing {count_cde_nodes} nodes')
    logging.info(f' - Edge files: {count_edge_files}')
    logging.info(f' - Comprehensive files: {count_edge_files}')
    logging.info(f' - Unable to classify: {count_unable_to_validate}')


if __name__ == '__main__':
    main()
