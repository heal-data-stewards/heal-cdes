#!/usr/bin/python
#
# loinc2couchdb.py - A program for loading LOINC data into a CouchDB instance for querying.
#

import click
import logging
import json
import couchdb

# We read config from `.env`.
import os
from dotenv import dotenv_values
config = {
    **dotenv_values(".env.default"), # default configuration
    **dotenv_values(".env"),         # override with user-specific® configuration
    **os.environ,                    # override loaded values with environment variables
}

# Set up logging
logging.basicConfig(level=logging.INFO)


# Export a LOINC item.
def export_item_to_loinc(entry, item, indent=1, group=None):
    item_type = item['type']
    spaces = '  ' * indent
    print(f"{spaces} - Item of type {item_type}: \"{item['text']}\"")

    if item_type == 'group':
        for inner_item in item['item']:
            export_item_to_loinc(entry, inner_item, indent + 1, group)


# Export a LOINC JSON file to a CouchDB for querying.
def export_entries_to_loinc(json_filename):
    logging.debug(f'export_to_loinc({json_filename})')

    obj = {}

    with open(json_filename, 'r') as fp:
        obj = json.load(fp)

    # Iterate through all the items in all the entries.
    for entry in obj['entry']:
        entry_url = entry['fullUrl']

        resource = entry['resource']

        entry_title = resource.get('title') or resource.get('name') or ''
        print(f' - {entry_url}: "{entry_title}"')

        if 'item' in resource:
            for item in entry['resource']['item']:
                export_item_to_loinc(entry, item)

# Import LOINC data from a folder into a CouchDB server.
@click.command()
@click.argument('input', type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=True,
    allow_dash=False
))
def main(input):
    input_path = click.format_filename(input)

    # Set up CouchDB access.
    logging.info('Connecting to CouchDB...')
    couch = couchdb.Server(config['COUCHDB_URL'])
    db = couch[config['COUCHDB_DATABASE']]
    logging.info(f"Connected to CouchDB database: {db}")

    if os.path.isfile(input_path):
        export_entries_to_loinc(input_path)
    else:
        iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
        for root, dirs, files in iterator:
            logging.debug(f' - Recursing into directory {root}')
            for filename in files:
                if filename.lower().endswith('.json'):
                    filepath = os.path.join(root, filename)
                    logging.debug(f'   - Found {filepath}')

                    export_entries_to_loinc(filepath)


if __name__ == '__main__':
    main()
