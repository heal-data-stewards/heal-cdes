#
# The HEAL CDE Repository (https://heal.nih.gov/data/common-data-elements-repository) has links to the XLSX files as
# well as other formats. This script downloads all of that information, annotates all the XLSX files, and generates
# all the appropriate files.
#
# Since annotation can take a while, this script needs to be designed so it can be re-run with partially downloaded
# files.
#
import collections
import json
import os
from dataclasses import dataclass
from time import time_ns

import click
import logging

import datetime

from humanfriendly import format_timespan
from kgx.graph.nx_graph import NxGraph
from kgx.transformer import Transformer
from kgx.source import graph_source
from kgx.sink import jsonl_sink

from annotators.bagel.bagel_annotator import annotate_crf

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
logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s: %(message)s')

# Configuration
heal_cde_download_time = datetime.datetime.now(datetime.timezone.utc)
heal_cde_source = f'HEAL CDE Repository, downloaded at {heal_cde_download_time}'

# Case class for storing source information.
@dataclass(frozen=True)
class Source:
    data_source: str
    filename: str
    study_name: str


# Download and annotate all the files from the HEAL CDE repository
@click.command()
@click.argument('downloads', type=click.Path(dir_okay=True, file_okay=False), required=True)
@click.option('--bagel-url', default='https://bagel.apps.renci.org')
@click.option('--add-cde-count-to-description', type=bool, default=True)
@click.option('--export-files-as-nodes', type=bool, default=False)
def heal_cde_repo_annotator(
        downloads,
        bagel_url,
        add_cde_count_to_description,
        export_files_as_nodes
):
    # Counts
    heal_cde_study_mapping_edge_count = 0

    # Load the CDE information.
    with open(os.path.join(downloads, 'cdes.json'), 'r') as f:
        heal_cde_entries = json.load(f)

    # For each identifier, load the details.
    total_crfs = len(heal_cde_entries)
    logging.info(f"Processing {total_crfs} CRFs")
    count_crfs = 0
    time_started = time_ns()
    for crf_id in heal_cde_entries:
        # Calculate the rate and estimated time of completion.
        count_crfs += 1
        time_elapsed_seconds = (time_ns() - time_started) / 1E9 + 0.001
        logging.info(f"Annotating {crf_id} ({count_crfs}/{total_crfs} or {count_crfs/total_crfs*100:.2f}%)")
        logging.info(f" - Current rate: {count_crfs/time_elapsed_seconds:.4f} CRFs/second or {time_elapsed_seconds/count_crfs:.4f} seconds/CRF.")

        remaining_crfs = total_crfs - count_crfs
        time_remaining_seconds = (time_elapsed_seconds / count_crfs * remaining_crfs)
        logging.info(f" - Estimated time remaining: {format_timespan(time_remaining_seconds)}")

        # Load the CRF.
        crf_dir = os.path.join(downloads, crf_id)
        crf_json_file = os.path.join(crf_dir, 'crf.json')

        # Before we load the CRF to annotate: do we already have a nodes and edges file?
        # If so, we'll skip it.
        #
        # TODO: we should add an 'errors.json' file so that we don't try to re-annotate files that already failed.
        nodes_file = os.path.join(crf_dir, 'nodes.json')
        edges_file = os.path.join(crf_dir, 'edges.json')
        if os.path.exists(nodes_file) and os.path.exists(edges_file):
            logging.info(f"Nodes and edges files already exist for {crf_id}, skipping.")
            continue

        # No CRF JSON file? Let's skip it.
        if not os.path.exists(crf_json_file):
            logging.warning(f"CRF JSON file not found for {crf_id}, skipping.")
            continue

        # Let's read in the comprehensive file.
        with open(crf_json_file, 'r') as jsonf:
            json_data = json.load(jsonf)

        files = json_data['sorted_files']

        # Start filling out the graph.
        graph = NxGraph()
        kgx_file_path = os.path.join(crf_dir, crf_id)  # Suffixes are added by the KGX tools.

        # Has this file already been annotated? If so, we can load those annotation results.
        comprehensive_file_path = os.path.join(crf_dir, 'comprehensive.json')
        if os.path.exists(comprehensive_file_path):
            with open(comprehensive_file_path, 'r') as jsonf:
                comprehensive = json.load(jsonf)
        else:
            comprehensive = annotate_crf(bagel_url, graph, 'HEALCDE:' + crf_id, json_data, heal_cde_source, add_cde_count_to_description=add_cde_count_to_description)
            with open(comprehensive_file_path, 'w') as jsonf:
                json.dump(comprehensive, jsonf, indent=2)

        # Are there any errors?
        if '_ner' in comprehensive and 'bagel' in comprehensive['_ner'] and 'errors' in comprehensive['_ner']['bagel']:
            with open(os.path.join(crf_dir, 'errors.json'), 'w') as jsonf:
                json.dump(json_data['errors'], jsonf, indent=2)
            error_str = json.dumps(comprehensive['_ner']['bagel']['errors'], indent=2)
            logging.error(f"Errors found when trying to run NER on {crf_id}: {error_str}")

        # Add files. To do this, we'll provide references to URLs to the CDE, and then later provide metadata about those URLs
        # directly in the graph.
        if export_files_as_nodes:
            graph.add_node_attribute('HEALCDE:' + crf_id, 'has_download', list(map(lambda x: x['url'], files)))

        # Create nodes for each download.
        files_urls = list()
        files_by_lang = collections.defaultdict(list)
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
                files_urls.append(url)
                files_by_lang[file['lang']].append(url)

        if not export_files_as_nodes:
            graph.add_node_attribute('HEALCDE:' + crf_id, 'files', list(files_urls))
            for lang in files_by_lang:
                graph.add_node_attribute('HEALCDE:' + crf_id, f"files-{lang}", list(files_by_lang[lang]))

        # Step 5. Add studies.
        for study_mappings in map(lambda f: f['studies'], files):
            for (hdp_id, sources) in study_mappings.items():
                # Create the HEAL CDE STUDY mapping edges.
                heal_cde_study_mapping_edge_count += 1
                edge_id = f'HEALCDESTUDYMAPPING:edge_{heal_cde_study_mapping_edge_count}'
                graph.add_edge('HEALCDE:' + crf_id, 'HEALDATAPLATFORM:' + hdp_id, edge_id)
                graph.add_edge_attribute('HEALCDE:' + crf_id, 'HEALDATAPLATFORM:' + hdp_id, edge_id, 'predicate', 'HEALCDESTUDYMAPPING:crf_used_by_study')
                graph.add_edge_attribute('HEALCDE:' + crf_id, 'HEALDATAPLATFORM:' + hdp_id, edge_id, 'predicate_label', 'HEAL CRF used by HEAL study')
                graph.add_edge_attribute('HEALCDE:' + crf_id, 'HEALDATAPLATFORM:' + hdp_id, edge_id, 'subject', 'HEALCDE:' + crf_id)
                graph.add_edge_attribute('HEALCDE:' + crf_id, 'HEALDATAPLATFORM:' + hdp_id, edge_id, 'object', 'HEALDATAPLATFORM:' + hdp_id)

                # Source/provenance information.
                sources = [source for source in sources]
                data_sources = list(map(lambda s: s['data_source'], sources))
                study_names = list(map(lambda s: s['study_name'], sources))
                filenames = list(map(lambda s: s['filename'], sources))

                graph.add_edge_attribute('HEALCDE:' + crf_id, 'HEALDATAPLATFORM:' + hdp_id, edge_id, 'sources', data_sources + filenames + study_names)
                graph.add_edge_attribute('HEALCDE:' + crf_id, 'HEALDATAPLATFORM:' + hdp_id, edge_id, 'knowledge_source', data_sources[0])

                # Create the HEAL CDE node.
                graph.add_node('HEALDATAPLATFORM:' + hdp_id)
                graph.add_node_attribute('HEALDATAPLATFORM:' + hdp_id, 'name', study_names[0])
                graph.add_node_attribute('HEALDATAPLATFORM:' + hdp_id, 'url', 'https://healdata.org/portal/discovery/' + hdp_id)
                graph.add_node_attribute('HEALDATAPLATFORM:' + hdp_id, 'category', ['biolink:Study'])
                graph.add_node_attribute('HEALDATAPLATFORM:' + hdp_id, 'provided_by', data_sources)

        # Step 5. Write KGX files.
        t = Transformer()
        t.process(
            source=graph_source.GraphSource(owner=t).parse(graph),
            sink=jsonl_sink.JsonlSink(owner=t, filename=kgx_file_path)
        )


# Run heal_cde_repo_downloader() if not used as a library.
if __name__ == "__main__":
    heal_cde_repo_annotator()
