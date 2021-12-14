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

    count_files = 0
    count_elements = 0
    count_not_questions = 0
    count_with_id = 0
    count_without_id = 0
    count_by_source = {}

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

                        if len(cde['ids']) == 0:
                            count_without_id += 1
                        else:
                            count_with_id += 1

                            print(f"{last_designation}:")
                            for id in cde['ids']:
                                source = id['source']
                                from_id = id['id']
                                code = from_id

                                if source not in count_by_source:
                                    count_by_source[source] = 0
                                count_by_source[source] += 1

                                find_code = re.search('Code (C\\d+)', code)
                                if find_code:
                                    code = find_code[1]

                                result = {}
                                if source == 'NCIT' and code.startswith('C'):
                                    url = f"http://www.ebi.ac.uk/ols/api/ontologies/ncit/terms/http%253A%252F%252Fpurl.obolibrary.org%252Fobo%252FNCIT_{code}"
                                    response = requests.get(url)
                                    if not response.ok:
                                        logging.error(f'OLS returned an error for {source}:{code}: {response}')
                                    else:
                                        result = response.json()

                                diff_from_id = ""
                                if code != from_id:
                                    diff_from_id = f" (from '{from_id}')"

                                print(f" - {source}: {code}{diff_from_id}")
                                if result:
                                    print(f"    - {json.dumps(result, indent=4, sort_keys=True)}")



                        # permissible_values = cde['permissibleValues']
                        # if permissible_values is not None and len(permissible_values) != 0:
                        #     # Write permissible values to output.
                        #     for pvalue in permissible_values:
                        #         dirname = os.path.relpath(root, input_path)
                        #         print(f"  - PV: {pvalue}")

    output.close()

    logging.info(
        f'Found {count_elements} elements ({count_not_questions} not questions, {count_with_id} with IDs, {count_without_id} without IDs) in {count_files} files.'
    )
    logging.info(
        f"Elements by source: {count_by_source}"
    )


if __name__ == '__main__':
    main()
