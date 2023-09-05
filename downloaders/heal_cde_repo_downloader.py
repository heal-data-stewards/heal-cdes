#
# The HEAL CDE Repository (https://heal.nih.gov/data/common-data-elements-repository) has links to the XLSX files as
# well as other formats. This script downloads all of that information, annotates all the XLSX files, and generates
# all the appropriate files.
#
# Since annotation can take a while, this script needs to be designed so it can be re-run with partially downloaded
# files.
#
import collections
import csv
import json

import click
import logging
import requests

# Configuration
HEAL_CDE_CSV_DOWNLOAD = "https://heal.nih.gov/data/common-data-elements-repository/export?page&_format=csv"

# Configure logging.
logging.basicConfig(level=logging.INFO)


# Download and annotate all the files from the HEAL CDE repository
@click.command()
@click.argument('output', type=click.Path(dir_okay=True, file_okay=False), required=True)
@click.option('--heal-cde-csv-download', '--url', default=HEAL_CDE_CSV_DOWNLOAD,
              help='A URL for downloading the CSV version of the HEAL CDE repository')
def heal_cde_repo_downloader(output, heal_cde_csv_download):
    # Step 1. Download the HEAL CDE CSV file.
    logging.info(f"Downloading HEAL CDE CSV file at {heal_cde_csv_download}.")
    result = requests.get(heal_cde_csv_download)
    if not result.ok:
        logging.error(f"Could not download {heal_cde_csv_download}: {result.status_code} {result.text}")
        exit(1)

    heal_cde_csv = result.text
    heal_cde_csv_reader = csv.DictReader(heal_cde_csv.splitlines())

    heal_cde_entries = collections.defaultdict(list)
    for row in heal_cde_csv_reader:
        title = row['Title']
        lang = 'en'

        # At some point in the future, we'll have unique HEAL CDE identifiers that we can use to figure out which of the
        # files mentioned in heal_cde_csv refer to the same CDE. Until then, we can generate an "id" ourselves based on the
        # unique stem of filenames.
        if title.endswith('-crf.docx'):
            id = title[0:-9]
        elif title.endswith('-crf.pdf'):
            id = title[0:-8]
        elif title.endswith('-spanish-crf.docx'):
            id = title[0:-17]
        elif title.endswith('-crf-pediatric.docx'):
            id = title[0:-19]
        elif title.endswith('-pediatric-crf.docx'):
            id = title[0:-19]
        elif title.endswith('-cde.pdf'):
            id = title[0:-8]
        elif title.endswith('-cde.xlsx'):
            id = title[0:-9]
        elif title.endswith('-cde_.xlsx'):
            id = title[0:-10]
        elif title.endswith('-cdes.xlsx'):
            id = title[0:-10]
        elif title.endswith('-crf.xlsx'):
            id = title[0:-9]
        elif title.endswith('-cde-pediatric.xlsx'):
            id = title[0:-19]
        elif title.endswith('-crf-pediatric-spanish.docx'):
            id = title[0:-27]
        elif title.endswith('-crf-spanish.docx'):
            id = title[0:-17]
            lang = 'es'
        elif title.endswith('-crf-spanish.pdf'):
            id = title[0:-16]
            lang = 'es'
        elif title.endswith('-crf-swedish.docx'):
            id = title[0:-17]
            lang = 'sv'
        elif title.endswith('-copyright-statement.docx'):
            id = title[0:-25]
        elif title.endswith('-copyright-statement_.docx'):
            id = title[0:-26]
        elif title.endswith('-copyright-statment.docx'):
            id = title[0:-24]
        elif title.endswith('-copyright-statement-pediatric.docx'):
            id = title[0:-35]
        else:
            raise RuntimeError(f"Could not generate an ID for CRF titled '{title}'.")

        description = row['Description']
        if row['File Language'] == 'English':
            lang = 'en'
        elif row['File Language'] == 'Spanish':
            lang = 'es'
        elif row['File Language'] == 'Swedish':
            lang = 'sv'
        else:
            # Hopefully we have a clue in the name.
            pass

        url = row['Link to File']

        heal_cde_entries[id].append({
            'id': id,
            'title': title,
            'description': description,
            'lang': lang
        })

    logging.info(json.dumps(heal_cde_entries, indent=2))

# Run heal_cde_repo_downloader() if not used as a library.
if __name__ == "__main__":
    heal_cde_repo_downloader()