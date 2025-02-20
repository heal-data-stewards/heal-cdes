#!/usr/bin/python
#
# extract-study-mappings-from-heal-cde-team-export.py
#
# SYNOPSIS
#   extract-study-mappings-from-heal-cde-team-export.py [file containing HEAL CDE team export]
#
# A hacky script written to extract study/CRF mappings from the HEAL CDE team.
#
import csv
import os
import json
import logging
from collections import defaultdict

import click

logging.basicConfig(level=logging.INFO)

# Global indexes.
rows_by_record_id = defaultdict(list)

@click.command()
@click.argument('input-file', required=True, type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=False,
    allow_dash=True
))
def extract_study_mappings(input_file):
    input_path = click.format_filename(input_file)
    logging.info(f'Reading HEAL CDE team export from {input_path}')

    count_rows = 0
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        logging.info(f"Input file has headers: {reader.fieldnames}")
        for row in reader:
            count_rows += 1
            rows_by_record_id[row["Record ID"]].append(row)

            

    logging.info(f'Read {len(rows_by_record_id.keys())} records covering {count_rows} rows.')


if __name__ == "__main__":
    extract_study_mappings()
