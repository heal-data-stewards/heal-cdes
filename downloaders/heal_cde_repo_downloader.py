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
import os

import click
import logging
import requests

import datetime

from kgx.graph.nx_graph import NxGraph
from kgx.transformer import Transformer
from kgx.source import graph_source
from kgx.sink import jsonl_sink

from excel2cde import convert_xlsx_to_json
from annotators.scigraph.scigraph_api_annotator import process_crf

# Configuration
HEAL_CDE_CSV_DOWNLOAD = "https://heal.nih.gov/data/common-data-elements-repository/export?page&_format=csv"

# Configure logging.
logging.basicConfig(level=logging.INFO)


# Download and annotate all the files from the HEAL CDE repository
@click.command()
@click.argument('output', type=click.Path(dir_okay=True, file_okay=False), required=True)
@click.option('--heal-cde-csv-download', '--url', default=HEAL_CDE_CSV_DOWNLOAD,
              help='A URL for downloading the CSV version of the HEAL CDE repository')
@click.option('--heal-cde-csv', '--csv', type=click.File(),
              help='The CSV version of the HEAL CDE repository')
@click.option('--add-cde-count-to-description', type=bool, default=True)
@click.option('--export-files-as-nodes', type=bool, default=False)
def heal_cde_repo_downloader(output, heal_cde_csv_download, heal_cde_csv, add_cde_count_to_description, export_files_as_nodes):
    # Step 1. Download the HEAL CDE CSV file.
    heal_cde_download_time = datetime.datetime.now(datetime.timezone.utc)
    heal_cde_source = f'HEAL CDE Repository, downloaded at {heal_cde_download_time}'

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
        elif title.endswith('-copyright-statement.docx'):
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
                mime = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            case '.xlsx':
                mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            case '.pdf':
                mime = 'application/pdf'
            case _:
                mime = 'application/octet-stream'
                logging.error(f"Unknown extension in URL {url} for {row}, assuming octet-stream: {extension}")


        heal_cde_entries[crf_id].append({
            'crf_id': crf_id,
            'title': title,
            'description': description,
            'lang': lang,
            'extension': extension,
            'mime-type': mime,
            'url': url,
            'row': row
        })

    # Set up the output directory.
    os.makedirs(output, exist_ok=True)

    # Write to outputs.
    with open(os.path.join(output, 'list.json'), 'w') as f:
        json.dump(heal_cde_entries, f, indent=2)

    # Confirm the mapping of input rows to identifiers.
    logging.debug(json.dumps(heal_cde_entries, indent=2))

    # For each identifier, download the XLSX file if that's an option.
    for crf_id in heal_cde_entries:
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
        logging.info(f"  Downloading XLSX file for {crf_id} from {xlsx_file_url} ...")
        xlsx_file_req = requests.get(xlsx_file_url, stream=True)
        if not xlsx_file_req.ok:
            logging.error(f"  COULD NOT DOWNLOAD {xlsx_file_url}: {xlsx_file_req.status_code} {xlsx_file_req.text}")
            continue

        xlsx_file_path = os.path.join(crf_dir, crf_id + '.xlsx')
        with open(xlsx_file_path, 'wb') as fd:
            for chunk in xlsx_file_req.iter_content(chunk_size=128):
                fd.write(chunk)

        logging.info(f"  Downloaded {xlsx_file_url} to {xlsx_file_path}.")

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

        # Step 3. Convert JSON to KGX.
        # Set up the KGX graph
        graph = NxGraph()
        kgx_file_path = os.path.join(crf_dir, crf_id)  # Suffixes are added by the KGX tools.
        comprehensive = process_crf(graph, 'HEALCDE:' + crf_id, json_data, heal_cde_source, add_cde_count_to_description=add_cde_count_to_description)

        # Add files. To do this, we'll provide references to URLs to the CDE, and then later provide metadata about those URLs
        # directly in the graph.
        if export_files_as_nodes:
            graph.add_node_attribute('HEALCDE:' + crf_id, 'has_download', list(map(lambda x: x['url'], files)))

        # Create nodes for each download.
        files_urls = set()
        files_by_lang = collections.defaultdict(set)
        files_urls.add(xlsx_file_url)
        for file in files:
            url = file['url']

            if export_files_as_nodes:
                graph.add_node(url)
                graph.add_node_attribute(url, 'category', ['biolink:WebPage'])
                graph.add_node_attribute(url, 'language', file['lang'])
                graph.add_node_attribute(url, 'format', file['mime-type'])
                graph.add_node_attribute(url, 'description', file['description'])
                graph.add_node_attribute(url, 'provided_by', heal_cde_source)
            else:
                files_urls.add(url)
                files_by_lang[file['lang']].add(url)

        if not export_files_as_nodes:
            graph.add_node_attribute('HEALCDE:' + crf_id, 'files', list(files_urls))
            for lang in files_by_lang:
                graph.add_node_attribute('HEALCDE:' + crf_id, f"files-{lang}", list(files_by_lang[lang]))

        # Step 4. Write KGX files.
        t = Transformer()
        t.process(
            source=graph_source.GraphSource(owner=t).parse(graph),
            sink=jsonl_sink.JsonlSink(owner=t, filename=kgx_file_path)
        )


# Run heal_cde_repo_downloader() if not used as a library.
if __name__ == "__main__":
    heal_cde_repo_downloader()
