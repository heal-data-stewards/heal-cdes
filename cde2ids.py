# So, our goal is as follows:
#   1. For every CDE in every JSON file, look for a CDE in a local database that are similar to
#      several input resources.
#   2.
#

# Python libraries
import json
import csv
import re

import couchdb
import click

from cdeindexing.tags import Tags

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


# Set up CouchDB
logging.info('Connecting to CouchDB...')
couch = couchdb.Server(config['COUCHDB_URL'])
db = couch[config['COUCHDB_DATABASE']]
logging.info(f"Connected to CouchDB database: {db}")


# Search for mappings to a particular CDE
def find_mappings(all_tags, cde):
    """
    :param cde:
    :return: Should return either an empty list or a list containing a dict in the shape: {
        'id': '???',
        'url': '???',
        'name': '???',
        'pvs': [{
            'name': '???',
            'id': '???',
            'url': '???'
        }, ...]
    """

    question_text = cde['label']

    question_tags = Tags.question_text_to_tags(question_text)

    # Search for all documents with any of these tags.
    rows = db.find({
        "selector": {
            "tags": {
                "$or": list(
                    {
                        "$elemMatch": {
                            "$eq": tag_name
                        }
                    } for tag_name in question_tags
                )
            }
        },
        "fields": [
            "_id",
            "question",
            "tags"
        ]
    })

    sorted_rows = Tags.sort_search_results(all_tags, question_tags, rows)

    if not sorted_rows:
        print(f"Question {question_text} (tags: {', '.join(question_tags)}) -- no matches found.")
    else:
        print(f"Question {question_text} (tags: {', '.join(question_tags)}) -- found matches:")
        for (index, row) in enumerate(sorted_rows[0:5]):
            print(f" - {index + 1}. {row['question']} (tags: {', '.join(row['tags'])}) -> {row['_id']}")

    logging.debug(f'Searching for matches : {list(rows)}')

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

    # Load the tags.
    all_tags = Tags.generate_tag_counts(db, 'question')
    sorted_tags = dict(sorted(all_tags.items(), key=lambda item: item[1]))
    logging.info(f"Tags detected: {sorted_tags}")

    # Set up the CSV writer.
    writer = csv.writer(output)
    writer.writerow([
        'filename',
        'filepath',
        'form_name'
        'question_text',
        'num_values',
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

                    crf = json.load(f)

                    for cde in crf['formElements']:
                        mappings = find_mappings(all_tags, cde)

                        designations = crf['designations']
                        last_designation = designations[-1]['designation']
                        form_elements = crf['formElements'] or []
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
