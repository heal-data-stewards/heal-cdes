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
        'question_name',
        'question_variable_name',
        'question_text',
        'question_type',
        'question_ncit_url',
        'question_definition',
        'question_instructions',
        'permissible_value',
        'permissible_value_definition'
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

                    cde = json.load(f)
                    designations = cde['designations']
                    last_designation = designations[-1]['designation']
                    form_elements = cde['formElements']

                    if len(form_elements) == 0:
                        logging.error(f'File {filepath} contains no form elements.')

                    for index, element in enumerate(form_elements, 1):
                        element_type = element['elementType']

                        if element_type != 'question':
                            logging.error(f'File {filepath}, form element {index} is of an unknown type: {element_type}.')
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

                        ncit_urls = list(map(
                            lambda id: id['id'],
                            filter(lambda id: id['source'] == 'NCIT_URL', cde['ids'])))

                        permissible_values = cde['permissibleValues']
                        if permissible_values is None or len(permissible_values) == 0:
                            # Write single row to output.
                            dirname = os.path.relpath(root, input_path)
                            writer.writerow([
                                filename,
                                dirname,
                                last_designation,
                                cde['name'],
                                cde_variable_names[0] if len(cde_variable_names) > 0 else '',
                                element['label'].strip(),
                                cde['datatype'],
                                ncit_urls[0] if len(ncit_urls) > 0 else '',
                                '; '.join(list(map(lambda df: df['definition'], cde_definitions))),
                                element_instruction,
                                '',
                                ''
                            ])
                        else:
                            # Write permissible values to output.
                            for pvalue in permissible_values:
                                dirname = os.path.relpath(root, input_path)
                                writer.writerow([
                                    filename,
                                    dirname,
                                    last_designation,
                                    cde['name'],
                                    cde_variable_names[0] if len(cde_variable_names) > 0 else '',
                                    element['label'].strip(),
                                    cde['datatype'],
                                    ncit_urls[0] if len(ncit_urls) > 0 else '',
                                    '; '.join(list(map(lambda df: df['definition'], cde_definitions))),
                                    element_instruction,
                                    pvalue['permissibleValue'],
                                    pvalue.get('valueMeaningDefinition') or ''
                                ])

    output.close()

    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )


if __name__ == '__main__':
    main()