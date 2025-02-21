#!/usr/bin/python
#
# finalize-json.py
#
# SYNOPSIS
#   finalize-json.py [directory or file to summarize]
#
# A hacky script written to help combine and clean-up CDE annotations, combine them with study/CRF and variable/CDE
# mappings, and produce KGX files for ingest into HSS.
#
# Furthermore, this will all be rewritten once we get all this mapping information from the HEAL Data Platform,
# so it will probably be hacky and messy, but it would be great to keep an eye on separating things such that
# how we rewrite this will be obvious in the future.
#
import csv
import os
import json
import logging
from collections import defaultdict

import click

logging.basicConfig(level=logging.INFO)

# Global indexes.
nodes_by_type = defaultdict(list)
edges = []


@click.command()
@click.argument('input-dir', required=True, type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=True,
    allow_dash=False
))
@click.option('--heal-crf-mappings', required=False, default=[], multiple=True, type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=False,
))
def finalize_json(input_dir, heal_crf_mappings):
    input_path = click.format_filename(input_dir)
    logging.info(f'Looking for KGX files in {input_path}')

    count_nodes = 0
    count_edges = 0
    for root, dirs, files in os.walk(input_path):
        for filename in files:
            filepath = os.path.join(root, filename)
            if filename.lower().endswith('_nodes.jsonl'):
                logging.info(f'Found nodes file {filepath}')
                count_nodes += 1

                with open(filepath, 'r') as f:
                    for line in f:
                        node = json.loads(line)
                        for category in node.get('category', ['None']):
                            nodes_by_type[category].append(node)

            elif filename.lower().endswith('_edges.jsonl'):
                logging.info(f'Found edges file {filepath}')
                count_edges += 1

                with open(filepath, 'r') as f:
                    for line in f:
                        edge = json.loads(line)
                        edges.append(edge)

            else:
                logging.debug(f'Skipping file {filepath}, neither nodes nor edges file.')

    # Identify CRFs.
    crfs = list(filter(lambda n: n['id'].startswith('HEALCDE:'), nodes_by_type['biolink:Publication']))
    logging.info(f'Found {len(crfs)} CRFs.')

    # Add CRF mappings.
    hdp_id_to_heal_crf_mappings = defaultdict(set)
    unique_heal_crf_ids = set()
    for mapping_file in heal_crf_mappings:
        mapping_path = click.format_filename(mapping_file)
        logging.info(f'Adding CRF mappings from {mapping_path}')
        with open(mapping_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                heal_crf_ids = row['heal_crf_id'].split('|')
                hdp_ids = row['hdp_id'].split('|')

                for heal_crf_id in heal_crf_ids:
                    for hdp_id in hdp_ids:
                        hdp_id_to_heal_crf_mappings[hdp_id].add(heal_crf_id)
                        unique_heal_crf_ids.add(heal_crf_id)

    logging.info(f"Found mappings from {len(hdp_id_to_heal_crf_mappings)} HDP IDs to {len(unique_heal_crf_ids)} HEAL CRFs.")

    # Final step: get rid of nodes that we've pruned out.
    # TODO

    logging.info(f'Processed {count_nodes} nodes and {count_edges} edges files.')

if __name__ == "__main__":
    finalize_json()
