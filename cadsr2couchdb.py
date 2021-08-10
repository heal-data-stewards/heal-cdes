#!/usr/bin/python
#
# cadsr2couchdb.py - A program for loading NIH CDE data into a CouchDB instance for querying.
#

import re
import uuid
import logging
import json
import xml.sax

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


# Export an NIH CDE entry to a CouchDB for querying.
def export_element_to_couchdb(element):
    public_id = element.findtext('PUBLICID')
    version = element.findtext('VERSION')
    name = element.findtext('PREFERREDNAME')
    definition = element.findtext('PREFERREDDEFINITION').replace('_', '\n')

    pref_question_texts = []
    alt_question_texts = []

    refitems = element.findall('./REFERENCEDOCUMENTSLIST/REFERENCEDOCUMENTSLIST_ITEM')
    for refitem in refitems:
        doctype = refitem.findtext('DocumentType')
        doctext = refitem.findtext('DocumentText')
        lang = refitem.findtext('Language')
        displayorder = refitem.findall('DisplayOrder')
        if doctype == 'Preferred Question Text':
            pref_question_texts.append(doctext)
        elif doctype == 'Alternate Question Text':
            alt_question_texts.append(doctext)
        else:
            logging.debug(f"  - Ignoring {doctype}: {doctext}@{lang} ({displayorder})")

    question = None
    if len(pref_question_texts) > 0:
        question = pref_question_texts[0]
    elif len(alt_question_texts) > 0:
        question = alt_question_texts[0]
    else:
        question = definition

    url = f"https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#/search?publicId={public_id}&version={version}"

    tags = Tags.question_text_to_tags(question)
    db.update([
        couchdb.Document(
            _id=url,
            source='cadsr',
            question=question,
            tags=tags
        )
    ])
    logging.info(f"- {public_id}v{version}: {question} ({name}) -> {tags}")

# Export an NIH CDE JSON file to a CouchDB for querying.
def export_xml_to_couchdb(xml_filename):
    logging.debug(f'export_xml_to_couchdb({xml_filename})')

    obj = {}

    tree = xml.etree.ElementTree.parse(xml_filename)
    elements = tree.findall('DataElement')
    for element in elements:
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
            'source': 'cadsr'
        }
    })
    db.purge(founds_docs)
    logging.info(f"Deleted {len([founds_docs])} existing caDSR documents in CouchDB")

    if os.path.isfile(input_path):
        export_xml_to_couchdb(input_path)
    else:
        iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
        for root, dirs, files in iterator:
            logging.debug(f' - Recursing into directory {root}')
            for filename in files:
                if filename.lower().endswith('.xml'):
                    filepath = os.path.join(root, filename)
                    logging.info(f'   - Found {filepath}')

                    export_xml_to_couchdb(filepath)


if __name__ == '__main__':
    main()
