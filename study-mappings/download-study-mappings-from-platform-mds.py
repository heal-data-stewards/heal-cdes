#!/usr/bin/env python3
import csv
import json
import os
import re
import sys
import logging
from collections import defaultdict
import datetime

import click
import requests

DEFAULT_CRF_IDS_MAPPINGS_FILE = os.path.join(os.path.dirname(__file__), "../mappings/heal-crf-ids/heal-crf-ids.csv")
DEFAULT_MDS_URL = "https://healdata.org/mds/metadata"
PLATFORM_MDS_BATCH_SIZE = 1000

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s: %(message)s')

@click.command()
@click.option('--output', '-o', required=False, type=click.File('w'), default=sys.stdout, help='Output file to write mappings to.')
@click.option('--mds-url', required=False, default=DEFAULT_MDS_URL, help='URL of the Platform MDS.')
@click.option('--heal-crf-ids-mappings', '--mappings', default=DEFAULT_CRF_IDS_MAPPINGS_FILE, required=True, type=click.Path(exists=True), help='File containing the HEAL CRF IDs to CDE and HDPCDE IDs mappings.')
def download_study_mappings_from_platform_mds(output, mds_url, heal_crf_ids_mappings):
    """
    Download study mappings from the HEAL Platform MDS.
    \f

    :param mds_url: The Platform MDS URL.
    :param heal_crf_ids_mappings: The file containing the HEAL CRF IDs to CDE and HDPCDE IDs mappings.
    """

    # Step 1. Load the HEAL CRF IDs to CDE and HDPCDE IDs mappings.
    map_hdpcde_id_to_cde_id = defaultdict(set)
    map_name_to_cde_id = defaultdict(set)
    with open(heal_crf_ids_mappings, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            heal_data_platform_curie = row['HEAL Data Platform CURIE']
            if not heal_data_platform_curie or heal_data_platform_curie == 'NA':
                continue

            heal_cde_curie = row['HEAL CDE CURIE']
            if not heal_cde_curie or heal_cde_curie == 'NA':
                continue

            if 'CRF Name' in row and row['CRF Name'] is not None and row['CRF Name'] != '':
                map_name_to_cde_id[row['CRF Name']].add(heal_cde_curie)

            if 'Other Names' in row and row['Other Names'] is not None and row['Other Names'] != '' and row['Other Names'] != 'NA':
                for name in row['Other Names'].split('|'):
                    map_name_to_cde_id[name].add(heal_cde_curie)

            map_hdpcde_id_to_cde_id[heal_data_platform_curie].add(heal_cde_curie)
    logging.info(f"Loaded {len(map_hdpcde_id_to_cde_id)} HEAL HDPCDE IDs to CDE ID mappings from {heal_crf_ids_mappings}.")
    logging.info(f"Loaded {len(map_name_to_cde_id)} CDE labels to CDE ID mappings from {heal_crf_ids_mappings}.")

    # Step 2. Download all the discovery_metadata from the Platform MDS.
    logging.debug(f"Querying all discovery_metadata from the Platform MDS at {mds_url}.")
    discovery_metadata = dict()
    offset = 0
    while True:
        response = requests.get(mds_url, {
            "data": True,
            "_guid_type": "discovery_metadata",
            "offset": offset,
            "limit": PLATFORM_MDS_BATCH_SIZE,
        })
        response.raise_for_status()
        batch = response.json()
        logging.info(f"Got {len(batch)} discovery_metadata records (offset = {offset}, limit = {PLATFORM_MDS_BATCH_SIZE}).")
        discovery_metadata.update(batch)
        if len(batch) < PLATFORM_MDS_BATCH_SIZE:
            # We're done! Break out.
            break
        # We're not done! Increment offset and go again.
        offset += PLATFORM_MDS_BATCH_SIZE
    logging.info(f"Got {len(discovery_metadata)} total discovery_metadata records.")

    # Step 3. Identify studies with variable_level_metadata.
    hdp_ids_with_vlm = [hdp_id for hdp_id, study_data in discovery_metadata.items() if 'variable_level_metadata' in study_data]
    logging.info(f"Found {len(hdp_ids_with_vlm)} studies with variable_level_metadata: {sorted(hdp_ids_with_vlm)}.")

    # Step 4. There are two kinds of VLMD that a study can have:
    #   - data_dictionaries: dictionary of DD label against HDPDD ID.
    #       Our assumption is that these are NOT standardized identifiers, so we'll try to map them,
    #       but won't care if we can't.
    #   - common_data_elements: dictionary of variable name against HDPCDE ID.
    #       Our assumption is that these are standardized identifiers, so if we can't find a match,
    #       we will produce an error.
    writer = csv.DictWriter(output, [
        'hdp_id',
        'crf_ids',
        'source'
    ])
    writer.writeheader()
    source = f"Downloaded from Platform MDS at {datetime.datetime.now(datetime.UTC).isoformat()}"

    count_dds = 0
    count_cdes = 0
    for hdp_id in hdp_ids_with_vlm:
        study_data = discovery_metadata[hdp_id]
        vlmd = study_data['variable_level_metadata']
        if 'data_dictionaries' in vlmd:
            for dd_label, hdpdd_id in vlmd['data_dictionaries'].items():
                # We don't really care if we don't map data dictionaries or not -- we don't expect them to be CDEs.
                if hdpdd_id.startswith('HDPCDE'):
                    raise RuntimeError(f"Found HDPCDE ID in data_dictionaries: {hdpdd_id}")
                if dd_label in map_name_to_cde_id:
                    if 'NA' not in map_name_to_cde_id[dd_label]:
                        writer.writerow({
                            'hdp_id': hdp_id,
                            'crf_ids': '|'.join(map_name_to_cde_id[dd_label]),
                            'source': source,
                        })
                        count_dds += 1
                else:
                    logging.info(f"Data dictionary '{dd_label}' ({hdpdd_id}) in study {hdp_id} does not appear to be a CDE, skipping.")

        if 'common_data_elements' in vlmd:
            found_cde_mapping = False
            for crf_name, hdpcde_id in vlmd['common_data_elements'].items():
                # These must always be mapped.
                if hdpcde_id in map_hdpcde_id_to_cde_id:
                    if 'NA' not in map_hdpcde_id_to_cde_id[hdpcde_id]:
                        writer.writerow({
                            'hdp_id': hdp_id,
                            'crf_ids': '|'.join(map_hdpcde_id_to_cde_id[hdpcde_id]),
                            'source': source,
                        })
                        count_cdes += 1
                    found_cde_mapping = True

                # crf_name is always in the format "<Drupal ID> <CRF Name>". We'll take out the Drupal ID
                # and try to match the CRF name.
                m = re.match(r'^\d+\s+(.*)\s*$', crf_name)
                if m:
                    crf_name_filtered = m.group(1)
                else:
                    crf_name_filtered = crf_name.strip()

                if crf_name_filtered in map_name_to_cde_id:
                    writer.writerow({
                        'hdp_id': hdp_id,
                        'crf_ids': '|'.join(map_name_to_cde_id[crf_name_filtered]),
                        'source': source,
                    })
                    count_cdes += 1
                    found_cde_mapping = True

            if found_cde_mapping is False:
                raise RuntimeError(f"Could not find CDE ID for CRF name '{crf_name}' ('{crf_name_filtered}', {hdpcde_id}) for study {hdp_id}.")

    logging.info(f"Successfully wrote {count_dds} DD label to CDE ID mappings and {count_cdes} CDE label to {output}.")


if __name__ == "__main__":
    download_study_mappings_from_platform_mds()
