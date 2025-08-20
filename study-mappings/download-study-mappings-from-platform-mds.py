#!/usr/bin/env python3

import json
import sys
import logging

import click
import requests

DEFAULT_MDS_URL = "https://healdata.org/mds/metadata"
PLATFORM_MDS_COUNT = 100

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(asctime)s: %(message)s')

@click.command()
@click.option('--mds-url', required=False, default=DEFAULT_MDS_URL, help='URL of the Platform MDS.')
def download_study_mappings_from_platform_mds(mds_url):
    """
    Download study mappings from the HEAL Platform MDS.
    \f

    :param mds_url: The Platform MDS URL.
    """

    # Get all the studies.



if __name__ == "__main__":
    download_study_mappings_from_platform_mds()
