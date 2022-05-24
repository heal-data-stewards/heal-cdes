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
    count_not_questions = 0
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

    logging.info(
        f'Found {count_elements} elements ({count_not_questions} not questions) in {count_files} files.'
    )


if __name__ == '__main__':
    main()
