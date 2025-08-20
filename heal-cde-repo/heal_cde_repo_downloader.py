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
import dataclasses
import json
import os
from dataclasses import dataclass
from time import sleep

import click
import logging
import requests

import datetime

from downloaders.excel2cde import convert_xlsx_to_json

# MIME-types we will use.
MIME_DOCX = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
MIME_XLSX = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
MIME_PDF = 'application/pdf'

# Configuration
HEAL_CDE_CSV_DOWNLOAD = "https://heal.nih.gov/data/common-data-elements-repository/export?_format=csv"

# Sort order for languages
LANGUAGE_ORDER = {'en': 1, 'es': 2, 'zh-CN': 3, 'zh-TW': 4, 'ja': 5, 'ko': 6, 'sv': 7}
MIME_TYPE_ORDER = {MIME_DOCX: 1, MIME_PDF: 2, MIME_XLSX: 3}

# Configure logging.
logging.basicConfig(level=logging.INFO)


# Case class for storing source information.
@dataclass(frozen=True)
class Source:
    data_source: str
    filename: str
    study_name: str = ''


# Load the HEAL CRF/study usage mappings.
def load_heal_crf_usage_mappings(study_mapping_file):
    study_mapping_filename = study_mapping_file.name

    crf_usage_mappings = dict()
    study_mapping_entries = csv.DictReader(study_mapping_file)

    for entry in study_mapping_entries:
        if 'HDP IDs' in entry:
            hdp_ids = entry['HDP IDs'].split('|')
        elif 'hdp_id' in entry:
            hdp_ids = [entry['hdp_id']]
        else:
            raise RuntimeError(f"No HDP IDs column found in {study_mapping_filename}: {entry.keys()}")

        if 'CRF URLs' in entry:
            crf_urls = entry['CRF URLs'].split('|')
        elif 'crf_ids' in entry:
            crf_urls = entry['crf_ids'].split('|')
        elif 'heal_crf_id' in entry:
            crf_urls = [entry['heal_crf_id']]
        else:
            raise RuntimeError(f"No CRF URLs column found in {study_mapping_filename}: {entry.keys()}")

        source = entry.get('Source', entry.get('filename', ''))

        if not hdp_ids or (len(hdp_ids) == 1 and (hdp_ids[0] == 'NA' or hdp_ids[0] == '')):
            logging.warning(f"No HDP IDs found in row {entry} in {study_mapping_filename}, skipping.")
            continue

        if not crf_urls or (len(crf_urls) == 1 and (crf_urls[0] == 'NA' or crf_urls[0] == '')):
            logging.warning(f"No CRF URLs found in row {entry} in {study_mapping_filename}, skipping.")
            continue

        for crf_url in crf_urls:
            if crf_url not in crf_usage_mappings:
                crf_usage_mappings[crf_url] = collections.defaultdict(set)

            for hdp_id in hdp_ids:
                crf_usage_mappings[crf_url][hdp_id].add(Source(source, study_mapping_filename))

    return crf_usage_mappings


# Download and annotate all the files from the HEAL CDE repository
@click.command()
@click.argument('output', type=click.Path(dir_okay=True, file_okay=False), required=True)
@click.option('--heal-cde-csv-download', '--url', default=HEAL_CDE_CSV_DOWNLOAD,
              help='A URL for downloading the CSV version of the HEAL CDE repository')
@click.option('--heal-cde-csv', '--csv', type=click.File(),
              help='The CSV version of the HEAL CDE repository')
@click.option('--heal-cde-study-mappings', '--mappings', type=click.File(),
              help='A CSV file describing mappings from CRFs/CDEs to studies')
def heal_cde_repo_downloader(
        output,
        heal_cde_csv_download,
        heal_cde_csv,
        heal_cde_study_mappings,
):
    # If we have CDE/study mappings, load them.
    crf_study_mappings = collections.defaultdict(dict)
    unused_crf_urls = set()
    if heal_cde_study_mappings:
        crf_study_mappings = load_heal_crf_usage_mappings(heal_cde_study_mappings)
        unused_crf_urls = set(crf_study_mappings.keys())

    # Step 1. Download the HEAL CDE CSV file.
    heal_cde_download_time = datetime.datetime.now(datetime.timezone.utc)

    if not heal_cde_csv:
        logging.info(f"Downloading HEAL CDE CSV file at {heal_cde_csv_download} at {heal_cde_download_time}.")
        result = requests.get(heal_cde_csv_download)
        if not result.ok:
            logging.error(f"Could not download {heal_cde_csv_download}: {result.status_code} {result.text}")
            exit(1)

        heal_cde_csv = result.text
        heal_cde_csv_reader = csv.DictReader(heal_cde_csv.splitlines())
    else:
        heal_cde_csv_reader = csv.DictReader(heal_cde_csv)

    heal_cde_entries = collections.defaultdict(list)
    for row in heal_cde_csv_reader:
        title = row['Title']
        lang = 'en'

        # At some point in the future, we'll have unique HEAL CDE identifiers that we can use to figure out which of the
        # files mentioned in heal_cde_csv refer to the same CDE. Until then, we can generate an "crf_id" ourselves based on the
        # unique stem of filenames.
        if title.endswith('-spanish-crf.docx'):
            crf_id = title[0:-17]
        elif title.endswith('-crf-pediatric.docx'):
            crf_id = title[0:-19]
        elif title.endswith('-pediatric-crf.docx'):
            crf_id = title[0:-19]
        elif title.endswith('-cde-pediatric.xlsx'):
            crf_id = title[0:-19]
        elif title.endswith('-crf-pediatric-spanish.docx'):
            crf_id = title[0:-27]
        elif title.endswith('-crf-spanish.docx'):
            crf_id = title[0:-17]
            lang = 'es'
        elif title.endswith('-crf-spanish.pdf'):
            crf_id = title[0:-16]
            lang = 'es'
        elif title.endswith('-crf-swedish.pdf'):
            crf_id = title[0:-16]
            lang = 'sv'
        elif title.endswith('-crf-swedish.docx'):
            crf_id = title[0:-17]
            lang = 'sv'
        elif title.endswith('-crf-swedish.pdf'):
            crf_id = title[0:-16]
            lang = 'sv'
        elif title.endswith('-crf-japanese.docx'):
            crf_id = title[0:-18]
            lang = 'ja'
        elif title.endswith('-korean.docx'):
            crf_id = title[0:-12]
            lang = 'ko'
        elif title.endswith('-crf-simplified-chinese.docx'):
            crf_id = title[0:-28]
            lang = 'zh-CN'
        elif title.endswith('-crf-traditional-chinese.docx'):
            crf_id = title[0:-29]
            lang = 'zh-TW'
        elif title.endswith('-copyright-statement.docx'):
            crf_id = title[0:-25]
        elif title.endswith('-copright-statement.docx'):
            crf_id = title[0:-24]
        elif title.endswith('-copyright_statement.docx'):
            crf_id = title[0:-25]
        elif title.endswith('-copyright-statement.pdf'):
            crf_id = title[0:-25]
        elif title.endswith('-copyright_statement.docx'):
            crf_id = title[0:-25]
        elif title.endswith('-copyright-statement.pdf'):
            crf_id = title[0:-24]
        elif title.endswith('-copyright-statement_.docx'):
            crf_id = title[0:-26]
        elif title.endswith('-copyright-statment.docx'):
            crf_id = title[0:-24]
        elif title.endswith('-copyright-statement-pediatric.docx'):
            crf_id = title[0:-35]
        elif title.endswith('-crf.docx'):
            crf_id = title[0:-9]
        elif title.endswith('-cde.docx'):
            crf_id = title[0:-9]
        elif title.endswith('-cde.docx'):
            crf_id = title[0:-9]
        elif title.endswith('-crf.pdf'):
            crf_id = title[0:-8]
        elif title.endswith('-cde.pdf'):
            crf_id = title[0:-8]
        elif title.endswith('-cde.xlsx'):
            crf_id = title[0:-9]
        elif title.endswith('-crf-.xlsx'):
            crf_id = title[0:-10]
        elif title.endswith('-cde_.xlsx'):
            crf_id = title[0:-10]
        elif title.endswith('-cdes.xlsx'):
            crf_id = title[0:-10]
        elif title.endswith('-crf.xlsx'):
            crf_id = title[0:-9]
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

        # Get the URL
        url = row['Link to File']

        # Relative links?
        if url.startswith('/files'):
            url = 'https://heal.nih.gov' + url

        # The format should still be the last part of the url.
        url_lc_parts = url.lower().split('.')
        extension = '.' + url_lc_parts[-1]
        match extension:
            case '.docx':
                mime = MIME_DOCX
            case '.xlsx':
                mime = MIME_XLSX
            case '.pdf':
                mime = MIME_PDF
            case _:
                mime = 'application/octet-stream'
                logging.error(f"Unknown extension in URL {url} for {row}, assuming octet-stream: {extension}")

        cde_json = {
            'crf_id': crf_id,
            'title': title,
            'description': description,
            'lang': lang,
            'extension': extension,
            'mime-type': mime,
            'url': url,
            'studies': {},
            'row': row
        }

        # Add any CDE mappings.
        if url in crf_study_mappings:
            cde_json['studies'] = dict()
            for (key, sources) in crf_study_mappings[url].items():
                cde_json['studies'][key] = list(map(lambda s: dataclasses.asdict(s), sources))
            unused_crf_urls.remove(url)

        heal_cde_entries[crf_id].append(cde_json)

    # Set up the output directory.
    os.makedirs(output, exist_ok=True)

    # Counts.
    count_crfs = 0
    count_xlsx = 0
    count_urls = 0

    # Write to outputs.
    with open(os.path.join(output, 'cdes.json'), 'w') as f:
        json.dump(heal_cde_entries, f, indent=2)

    # Confirm the mapping of input rows to identifiers.
    logging.debug(json.dumps(heal_cde_entries, indent=2))

    # For each identifier, download the XLSX file if that's an option.
    for crf_id in heal_cde_entries:
        count_crfs += 1
        logging.info(f"Processing {crf_id}")
        files = heal_cde_entries[crf_id]

        titles = list(set(map(lambda f: f['title'], files)))
        descriptions = list(set(map(lambda f: f['description'], files)))

        crf_dir = os.path.join(output, crf_id)
        os.makedirs(crf_dir, exist_ok=True)

        # At the moment we only support a single XLSX file.
        xlsx_files = list(filter(lambda f: f['extension'] == '.xlsx', files))
        if len(xlsx_files) == 0:
            logging.warning(f"{crf_id} contains no XLSX files, skipping.")
            continue
        elif len(xlsx_files) > 1:
            raise RuntimeError(
                f"CRF {crf_id} contains more than one XLSX file, which is not currently supported: {xlsx_files}"
            )

        xlsx_file = xlsx_files[0]
        xlsx_file_url = xlsx_file['url']

        # Step 1. Download XLSX file.
        attempt_count = 0
        xlsx_file_path = os.path.join(crf_dir, 'crf.xlsx')
        while True:
            attempt_count += 1
            logging.info(f"  Downloading XLSX file for {crf_id} from {xlsx_file_url} ... (attempt {attempt_count}/10)")
            xlsx_file_req = requests.get(xlsx_file_url, stream=True)
            if not xlsx_file_req.ok:
                logging.error(f"  COULD NOT DOWNLOAD {xlsx_file_url}: {xlsx_file_req.status_code} {xlsx_file_req.text}")
                continue

            with open(xlsx_file_path, 'wb') as fd:
                count_xlsx += 1
                for chunk in xlsx_file_req.iter_content(chunk_size=128):
                    fd.write(chunk)

            # Check if the file size of xlsx_file_path is 0.
            if os.path.getsize(xlsx_file_path) >= 100:
                break

            # We got a near-empty file! If attempt_count <= 10, we'll try again.
            logging.error(f"  XLSX file for {crf_id} at {xlsx_file_path} is near empty, retrying.")
            if attempt_count > 10:
                raise RuntimeError(f"Could not download XLSX file for {crf_id} from {xlsx_file_url} after 10 attempts.")
            else:
                sleep(10 * attempt_count)
                continue

        file_size = os.path.getsize(xlsx_file_path)
        logging.info(f"  Downloaded {xlsx_file_url} to {xlsx_file_path} (file size: {file_size} bytes).")

        # Step 2. Convert to JSON.
        logging.info(f"  Converting {xlsx_file_path} to JSON ...")
        json_data = convert_xlsx_to_json(xlsx_file_path)
        if not json_data:
            json_data = dict()

        # Add titles and descriptions.
        json_data['titles'] = titles
        json_data['descriptions'] = descriptions

        # Add categories and topics
        categories = set()
        for f in files:
            if 'row' in f and 'Core or Supplemental' in f['row']:
                for cat in f['row']['Core or Supplemental'].split(', '):
                    categories.add(cat)
            if 'row' in f and 'CDE Topics' in f['row']:
                for topic in f['row']['CDE Topics'].split(', '):
                    categories.add(topic)

        json_data['categories'] = list(sorted(categories))

        # Step 3. Reorder the files in order of LANGUAGE_ORDER and FILE_TYPE_ORDER (in that order).
        def sort_key(file_entry):
            return LANGUAGE_ORDER[file_entry['lang']], MIME_TYPE_ORDER[file_entry['mime-type']]

        sorted_files = sorted(files, key=sort_key)
        urls = list(map(lambda f: f['url'], sorted_files))
        logging.info(f"Sorted URLs: {'; '.join(urls)}")
        json_data['urls'] = urls
        count_urls += len(urls)

        json_data['sorted_files'] = sorted_files

        # Save the JSON file for annotation.
        with open(os.path.join(crf_dir, 'crf.json'), 'w') as jsonf:
            json.dump(json_data, jsonf, indent=2)

    logging.info(f"Download complete: {count_crfs} CRFs, {count_xlsx} XLSX files, {count_urls} URLs downloaded.")

# Run heal_cde_repo_downloader() if not used as a library.
if __name__ == "__main__":
    heal_cde_repo_downloader()
