#!/usr/bin/python
#
# nihcde2couchdb.py - A program for loading NIH CDE data into a CouchDB instance for querying.
#

import re
import uuid
import logging
import json

import click
import nltk
from nltk.corpus import stopwords
import couchdb

# We read config from `.env`.
import os
from dotenv import dotenv_values

from cdeindexing.tags import Tags

config = {
    **dotenv_values(".env.default"), # default configuration
    **dotenv_values(".env"),         # override with user-specific® configuration
    **os.environ,                    # override loaded values with environment variables
}

# Set up logging
logging.basicConfig(level=logging.INFO)

# Set up CouchDB
logging.info('Connecting to CouchDB...')
couch = couchdb.Server(config['COUCHDB_URL'])
db = couch[config['COUCHDB_DATABASE']]
logging.info(f"Connected to CouchDB database: {db}")


# Retrieve an ID from the code.
def get_ids(codes):
    if codes is None or len(codes) == 0:
        return [uuid.UUID.hex]

    ids = []
    for code in codes:
        ids.append(f"{code['system']}/{code['code']}")

    return ids


# Export an NIH CDE question.
def export_question_to_couchdb(question):
    if 'cde' not in question:
        return

    cde = question['cde']

    tiny_id = cde.get('tinyId')
    doc_id = f"https://cde.nlm.nih.gov/deView?tinyId={tiny_id}"
    name = cde.get('name')
    label = cde.get('label') or name

    if not label:
        return

    tags = Tags.question_text_to_tags(label)
    db.update([
        couchdb.Document(
            _id=doc_id,
            source='nihcde',
            question=label,
            tags=tags
        )
    ])
    logging.debug(f" - {doc_id}: {label} -> {tags}")


# Export an NIH CDE entry to a CouchDB for querying.
def export_element_to_couchdb(entry):
    # Is this a question?
    if 'question' in entry:
        export_question_to_couchdb(entry['question'])

    # Do we have form elements?
    if 'formElements' in entry:
        for element in entry['formElements']:
            export_element_to_couchdb(element)


# Export an NIH CDE JSON file to a CouchDB for querying.
def export_json_to_couchdb(json_filename):
    logging.debug(f'export_entries_to_couchdb({json_filename})')

    obj = {}

    with open(json_filename, 'r') as fp:
        obj = json.load(fp)

    # Iterate through all the items in all the entries.
    for entry in obj:
        for element in entry['formElements']:
            export_element_to_couchdb(element)

# Import NIH CDE data from a folder into a CouchDB server.
@click.command()
@click.argument('input', type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=True,
    allow_dash=False
))
def main(input):
    input_path = click.format_filename(input)

    founds_docs = db.find({
        'selector': {
            'source': 'nihcde'
        }
    })
    db.purge(founds_docs)
    logging.info(f"Deleted {len([founds_docs])} existing NIH CDE documents in CouchDB")

    if os.path.isfile(input_path):
        export_json_to_couchdb(input_path)
    else:
        iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
        for root, dirs, files in iterator:
            logging.debug(f' - Recursing into directory {root}')
            for filename in files:
                if filename.lower().endswith('.json'):
                    filepath = os.path.join(root, filename)
                    logging.debug(f'   - Found {filepath}')

                    export_json_to_couchdb(filepath)


if __name__ == '__main__':
    main()
