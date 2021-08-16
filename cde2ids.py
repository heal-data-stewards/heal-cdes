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
    :param all_tags:
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
    query = {
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
        "limit": 100000,
        "fields": [
            "_id",
            "question",
            "tags"
        ]
    }
    logging.debug(f'Query: {query}')
    rows = db.find(query)

    sorted_rows = Tags.sort_search_results(all_tags, question_tags, rows)

    results = []
    if not sorted_rows:
        logging.debug(f"Question: {question_text} (tags: {', '.join(question_tags)}) -- no matches found.")
    else:
        logging.debug(f"Question: {question_text} (tags: {', '.join(question_tags)}) -- found matches:")
        for (index, row) in enumerate(sorted_rows[0:10]):
            logging.debug(f" - {index + 1}. {row['question']} (tags: {', '.join(row['tags'])}) with score {row['score']} -> {row['_id']}")

            url = str(row['_id'])
            if url.startswith('question:'):
                url = url[9:]

            # TODO: add the number of permissible values
            results.append({
                '@id': url,
                'question': row['question'],
                'tags': row['tags'],
                'score': row['score'],
            })

        logging.debug('')

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

    # Load the tags.
    all_tags = Tags.generate_tag_counts(db, 'question')
    sorted_tags = dict(sorted(all_tags.items(), key=lambda item: item[1]))
    logging.debug(f"Tags detected: {sorted_tags}")

    # Set up the CSV writer.
    writer = csv.writer(output)
    header_row = [
        'filename',
        'filepath',
        'form_name',
        'question_text',
        'pv_count'
    ]

    for index in range(1, 6):
        header_row.extend([
            f'match_{index}',
            f'match_{index}_question',
            f'match_{index}_pv_count',
            f'match_{index}_notes'
        ])
    writer.writerow(header_row)

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

                        question = crf['label']

                        if len(mappings) == 0:
                            writer.writerow([
                                filename,
                                filepath,
                                last_designation,
                                question,
                                0
                            ])
                        else:
                            row = [
                                filename,
                                filepath,
                                last_designation,
                                question,
                                ''
                            ]
                            for mapping in mappings[:6]:
                                row.extend([
                                    mapping['@id'] or '',
                                    mapping['question'] or '',
                                    '',
                                    ''
                                ])

                            writer.writerow(row)

    output.close()

    logging.info(
        f'Found {count_elements} elements in {count_files} files.'
    )


if __name__ == '__main__':
    main()
