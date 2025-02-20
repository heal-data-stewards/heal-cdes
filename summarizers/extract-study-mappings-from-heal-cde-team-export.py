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

    # Read rows from file.
    count_rows = 0
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        logging.info(f"Input file has headers: {reader.fieldnames}")
        for row in reader:
            count_rows += 1
            rows_by_record_id[row["Record ID"]].append(row)
    logging.info(f'Read {len(rows_by_record_id.keys())} records covering {count_rows} rows.')

    # For each record, we need to collect three kinds of information:
    # 1. Each record should have EXACTLY ONE "Project Number" and "Project Title  &nbsp; "
    # 2. Some columns record some common measures.
    # 3. Collect all the measures listed by name under "Measure Name".
    project_crfs = defaultdict(dict)
    unique_crf_names = set()
    count_crfs = 0
    for record_id in rows_by_record_id.keys():
        rows = rows_by_record_id[record_id]

        project_number = None
        project_title = None
        measure_names = set()
        for row in rows:
            # Figure out the project number
            if row["Project Number"].strip():
                if project_number is not None:
                    raise RuntimeError(f"Found multiple project numbers for record {record_id}")
                project_number = row["Project Number"]

            # Figure out the project title.
            if row["Project Title  &nbsp; "].strip():
                if project_title is not None:
                    raise RuntimeError(f"Found multiple project numbers for record {record_id}")
                project_title = row["Project Title  &nbsp; "]

            # Look for measure names.
            measure_name_rows = {
                'Measure Name',
                'Name of Measure',
                'Name of Other Measure'
            }
            for measure_name_row in measure_name_rows:
                if row[measure_name_row].strip():
                    measure_names.add(row[measure_name_row].strip())

        # No point recording anything without a project number.
        if project_number is None:
            logging.warning(f"Record {record_id} is missing a project number, skipping.")
            continue

        # Add this to the list of project CRFs.
        if project_number in project_crfs:
            logging.warning(f"Found multiple records for project {project_number}, previous record will be overwritten: {json.dumps(project_crfs[project_number], indent=2)}")
            project_crfs[project_number] = {}

        project_crfs[project_number]["project_number"] = project_number
        project_crfs[project_number]["record_id"] = record_id
        project_crfs[project_number]["rows"] = rows

        project_crfs[project_number]["project_title"] = project_title
        project_crfs[project_number]["crfs"] = sorted(measure_names)
        unique_crf_names.update(measure_names)
        count_crfs += len(measure_names)

    logging.info(f'Found {count_crfs} CRFs ({len(unique_crf_names)} unique) listed for {len(project_crfs.keys())} projects.')


if __name__ == "__main__":
    extract_study_mappings()
