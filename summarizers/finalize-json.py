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
nodes = []
nodes_by_id = defaultdict(list)
nodes_by_type = defaultdict(list)
edges = []


@click.command()
@click.argument('input-dir', required=True, type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=True,
    allow_dash=False
))
@click.option('--output-nodes', required=True, default='annotated_nodes.jsonl', type=click.Path(
    exists=False,
    file_okay=True,
    dir_okay=False,
    allow_dash=True
))
@click.option('--output-edges', required=True, default='annotated_edges.jsonl', type=click.Path(
    exists=False,
    file_okay=True,
    dir_okay=False,
    allow_dash=True
))
@click.option('--heal-crf-mappings', required=False, default=[], multiple=True, type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=False,
))
def finalize_json(input_dir, output_nodes, output_edges, heal_crf_mappings):
    input_path = click.format_filename(input_dir)
    logging.info(f'Looking for KGX files in {input_path}')

    count_nodes = 0
    count_edges = 0
    for root, dirs, files in os.walk(input_path):
        for filename in files:
            filepath = os.path.join(root, filename)
            if filename.lower().endswith('_nodes.jsonl'):
                logging.info(f'Found nodes file {filepath}')

                with open(filepath, 'r') as f:
                    for line in f:
                        node = json.loads(line)
                        count_nodes += 1
                        nodes.append(node)
                        nodes_by_id[node['id']].append(node)
                        for category in node.get('category', ['None']):
                            nodes_by_type[category].append(node)

            elif filename.lower().endswith('_edges.jsonl'):
                logging.info(f'Found edges file {filepath}')

                with open(filepath, 'r') as f:
                    for line in f:
                        edge = json.loads(line)
                        count_edges += 1
                        edges.append(edge)

            else:
                logging.debug(f'Skipping file {filepath}, neither nodes nor edges file.')

    # Identify CRFs.
    crfs = list(filter(lambda n: n['id'].startswith('HEALCDE:'), nodes_by_type['biolink:Publication']))
    logging.info(f'Found {len(crfs)} CRFs: {json.dumps(list(map(lambda c: c["id"] + ": " + c.get("summary", ""), crfs)), indent=2)}')

    # Add CRF mappings.
    hdp_id_study_name = dict()
    hdp_id_to_heal_crf_mappings = dict()
    unique_heal_crf_ids = set()
    for mapping_file in heal_crf_mappings:
        hdp_id_to_heal_crf_mappings[mapping_file] = defaultdict(set)
        mapping_path = click.format_filename(mapping_file)
        logging.info(f'Adding CRF mappings from {mapping_path}')
        with open(mapping_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                heal_crf_ids = row['heal_crf_id'].split('|')
                hdp_ids = row['hdp_id'].split('|')

                for heal_crf_id in heal_crf_ids:
                    for hdp_id in hdp_ids:
                        hdp_id_to_heal_crf_mappings[mapping_path][hdp_id].add(heal_crf_id)
                        unique_heal_crf_ids.add(heal_crf_id)

                        if hdp_id != 'NA' and 'project_title' in row and row['project_title']:
                            if hdp_id in hdp_id_study_name and row['project_title'] != hdp_id_study_name[hdp_id]:
                                logging.warning(f"Multiple project titles found for {hdp_id}: '{row['project_title']}' and '{hdp_id_study_name.get(hdp_id, '')}'.")
                            hdp_id_study_name[hdp_id] = row['project_title']

    logging.info(f"Found mappings from {len(hdp_id_to_heal_crf_mappings)} HDP IDs to {len(unique_heal_crf_ids)} HEAL CRFs.")

    # Add edges for the mappings.
    hdp_ids_to_add = set()
    for source in hdp_id_to_heal_crf_mappings.keys():
        for hdp_id_without_prefix, heal_crf_ids in hdp_id_to_heal_crf_mappings[source].items():
            for heal_crf_id in heal_crf_ids:
                hdp_id = f"HEALDATAPLATFORM:{hdp_id_without_prefix}"
                if heal_crf_id == "HEALCDE:NA":
                    continue
                if heal_crf_id not in nodes_by_id:
                    raise RuntimeError(f"Could not look up HEAL CRF ID {heal_crf_id} in nodes.")
                edges.append({
                    "predicate": "HEALCDESTUDYMAPPING:crf_used_by_study",
                    "predicate_label": "HEAL CRF used by HEAL study",
                    "subject": heal_crf_id,
                    "object": hdp_id,
                    "sources": [
                        source
                    ],
                    "knowledge_source": "HEAL CDE Usage"
                })
                hdp_ids_to_add.add(hdp_id_without_prefix)

    # Add nodes for the HDP studies we've included.
    for hdp_id_without_prefix in sorted(hdp_ids_to_add):
        if hdp_id_without_prefix in hdp_id_study_name:
            nodes.append({
                "id": f"HEALDATAPLATFORM:{hdp_id_without_prefix}",
                "url": f"https://healdata.org/portal/discovery/{hdp_id_without_prefix}",
                "name": hdp_id_study_name[hdp_id_without_prefix],
                "category": [
                    "biolink:Study"
                ],
                "provided_by": ["HEAL CDE finalize-json.py"],
            })

    # Final step: get rid of nodes that we've pruned out.
    # TODO

    with open(click.format_filename(output_nodes), 'w') as fnodes:
        for node in nodes:
            fnodes.write(json.dumps(node) + '\n')

    with open(click.format_filename(output_edges), 'w') as fedges:
        for edge in edges:
            fedges.write(json.dumps(edge) + '\n')

    logging.info(f'Wrote out {len(nodes)} nodes (original {count_nodes}) and {len(edges)} edges (original {count_edges}) files.')

if __name__ == "__main__":
    finalize_json()
