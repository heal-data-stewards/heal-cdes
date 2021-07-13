# So, our goal is as follows:
#   1. We assume that each file corresponds to a single CRF.
#   2. We search for the form name on several online repositories:
#       - caDSR
#       - LOINC
#       - (additional if needed)
#   3. We compare the list of questions we have with the list of questions.
#       - For now, we make NO further checks, although we'll probably add some eventually.
#   4. We generate a CSV file indicating:
#       - Our CRF and number of questions
#       - The mapped CRF, its identifier, and the number of questions
#       - The number of questions that are verbatim identical to the ones we have

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


# Search for mappings to a particular CDE
def find_mappings(cde):
    return []

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
        'num_questions',
        'mapped_cde_id',
        'mapped_cde_url',
        'mapped_cde_name',
        'mapped_cde_num_questions'
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
                    mappings = find_mappings(cde)

                    designations = cde['designations']
                    last_designation = designations[-1]['designation']
                    form_elements = cde['formElements'] or []
                    questions = list(filter(lambda fe: fe['elementType'] == 'question', form_elements))
                    count_elements += len(questions)

                    if len(mappings) == 0:
                        writer.writerow([
                            filename,
                            filepath,
                            last_designation,
                            len(questions)
                        ])
                    else:
                        for mapping in mappings:
                            writer.writerow([
                                filename,
                                filepath,
                                last_designation,
                                len(questions),
                                mapping['id'] or '',
                                mapping['url'] or '',
                                mapping['name'] or '',
                                len(mapping['questions']) or ''
                            ])

    output.close()

    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )


if __name__ == '__main__':
    main()
