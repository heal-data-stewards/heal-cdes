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
from importlib.metadata import requires

import click

HEAL_CDE_PREFIX = "HEALCDE:"

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
@click.option('--study-to-hdpid', required=True, type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=False,
))
@click.option('--measure-to-heal-cde-id', required=True, type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=False,
))
def extract_study_mappings(input_file, study_to_hdpid, measure_to_heal_cde_id):
    input_path = click.format_filename(input_file)
    study_to_hdpid_path = click.format_filename(study_to_hdpid)
    measure_to_heal_cde_id_path = click.format_filename(measure_to_heal_cde_id)
    logging.info(f'Reading HEAL CDE team export from {input_path}')

    # Read rows from file.
    count_rows = 0
    with open(input_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        study_mapping_headers = list(reader.fieldnames)
        logging.info(f"Input file has headers: {reader.fieldnames}")
        for row in reader:
            count_rows += 1
            rows_by_record_id[row["Record ID"]].append(row)
    logging.info(f'Read {len(rows_by_record_id.keys())} records covering {count_rows} rows.')

    # Load the study to HDP ID mappings.
    project_number_to_hdp_id = dict()
    with open(study_to_hdpid_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            project_number_to_hdp_id[row['Project Number'].strip()] = row['HDP_ID'].strip()

    # Load the measure to HEAL CDE ID mappings.
    crf_to_heal_cde_id = dict()
    with open(measure_to_heal_cde_id_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            crf_to_heal_cde_id[row['Measure Name'].strip()] = row['HEAL CDE ID'].strip()

    # Column name mappings.
    header_crf_mappings = {
        "Demographics (choice=Adult Demographics)": "adult-demographics",
        "Pain Intensity (choice=PEG (for chronic pain))": "peg",
        "Pain Intensity (choice=BPI Pain Severity (for acute pain))": "bpi-pain-severity",
        "Pain Interference (choice=PEG (for chronic pain))": "peg",
        "Pain Interference (choice=BPI Pain Interference (Copyrighted) (for acute pain))": "bpi-pain-interference",
        "Pain Catastrophizing (choice=Pain Catastrophizing Scale (PCS) - 6 item)": "NA",
        "Pain Catastrophizing (choice=Pain Catastrophizing Scale (PCS) - 13 item*)": "NA",
        "Physical Functioning/QOL (choice=PROMIS Physical Functioning Short Form 6b)": "physical-function-6b",
        "Sleep (choice=PROMIS Sleep Disturbance 6a)": "sleep-disturbance-6a",
        "Sleep (choice=Sleep Duration Question)": "sleep-duration",
        "Global Satisfaction with Treatment (choice=PGIC)": "pgic",
        "Substance Use Screener (choice=TAPS 1)": "taps-1",
        "Prescription Opioid Use (choice=Compute Opioid MME using either the REDCap Data Dictionary in the HEAL CDE repository or the web tool)": "opioid-mme",
        "Quality of Life (choice=World Health Organization Quality of Life (WHOQOL) - 2 item)": "whoqol-2",
        "Quality of Life (choice=World Health Organization Quality of Life (WHOQOL) - 26 item*)": "whoqol-bref",
        "Demographics (choice=Child Demographics)": "pediatric-demographic",
        "Pain Intensity (choice=BPI Pain Severity)": "bpi-pain-severity",
        "Pain Interference (choice=BPI Pain Interference (Copyrighted))": "bpi-pain-interference",
        "Sleep (choice=AWS-10 + Sleep Duration Items)": "sleep-asws",
        "Pain Catastrophizing (Administered to Child) (choice=Pain Catastrophizing Scale for Children)": "pcs-child",
        "Pain Catastrophizing (Administered to Parent) (choice=Pain Catastrophizing)": "pcs-parent",
        "Substance Abuser Screener (choice=NIDA Modified Assist Tool-2)": "nida-modified-assist",
        "Prescription Opioid Use (choice=Compute Opioid MME using either the REDCap Data Dictionary in the HEAL CDE repository or the web tool)": "opioid-mme",
        "PedsQL Inventory Age Range(s) (choice=2-4 year old)": "pedsql-sickle-cell-parent-report-ages-2-4",
        "PedsQL Inventory Age Range(s) (choice=5-7 year old)": "pedsql-sickle-cell-child-report-ages-5-7",
        "PedsQL Inventory Age Range(s) (choice=8-12 year old)": "pedsql-sickle-cell-parent-report-ages-8-12",
        "PedsQL Inventory Age Range(s) (choice=13-18 year old)": "pedsql-sickle-cell-parent-report-ages-13-18",
        "PHQ (choice=Patient Health Questionnaire (PHQ) - 2 item)": "phq2",
        "PHQ (choice=Patient Health Questionnaire (PHQ) - 8 item*)": "patient-health-questionnaire-8",
        "PHQ (choice=Patient Health Questionnaire (PHQ) - 9 item*)": "patient-health-questionnaire-9",
        "PHQ Ped (choice=Patient Health Questionnaire (PHQ) - 2 item)": "phq2",
        "PHQ Ped (choice=Patient Health Questionnaire (PHQ) - 8 item*)": "patient-health-questionnaire-8",
        "PHQ Ped (choice=Patient Health Questionnaire (PHQ) - 9 item*)": "patient-health-questionnaire-9",
        "GAD (choice=Generalized Anxiety Disorder (GAD) - 2 item)": "gad2",
        "GAD (choice=Generalized Anxiety Disorder (GAD) - 7 item*)": "generalized-anxiety-disorder-7",
        "GAD Ped (choice=Generalized Anxiety Disorder (GAD) - 2 item)": "gad2",
        "GAD Ped (choice=Generalized Anxiety Disorder (GAD) - 7 item*)": "generalized-anxiety-disorder-7",
        "PGIC Ped (choice=Patient Global Impression of Change (PGIC))": "pgic",
        "PGIC Ped (choice=Patient Global Impression of Severity (PGIS))": "pgic",
        "Recommended Measures: (choice=Alcohol Use Disorders Identification Test - AUDIT *Full [IMPOWR])": "NA",
        "Recommended Measures: (choice=Bank v1.0 Alcohol: Alcohol Use SF 7a [PROMIS])": "promis-alcohol-use-7a",
        "Recommended Measures: (choice=Brief Spirituality Scale [IMPOWR])": "NA",
        "Recommended Measures: (choice=Coronavirus Pandemic Measures 7 item)": "NA",
        "Recommended Measures: (choice=Optional Demographics [IMPOWR])": "NA",
        "Recommended Measures: (choice=Health Services Utilization/Health Economics l [IMPOWR])": "NA",
        "Recommended Measures: (choice=Multidimensional Scale of Perceived Social Support- MSPSS)": "mspss",
        "Recommended Measures: (choice=Narcan Usage [IMPOWR])": "NA",
        "Recommended Measures: (choice=Pain Conditions [IMPOWR])": "NA",
        "Recommended Measures: (choice=PTSD Checklist for DSM 5 - PCL 5)": "pcl-5",
        "Recommended Measures: (choice=Severity of Substance Use *Past 30 days v1.0 Short Form 7a [PROMIS])": "promis-severity-of-substance-use-7a",
        "Recommended Measures: (choice=Staff Survey)": "NA",
        "Recommended Measures: (choice=Stigma/Discrimination Questions [IMPOWR])": "NA",
        "Recommended Measures: (choice=Substance Use [IMPOWR])": "NA",
        "Recommended Measures: (choice=The Brief Assessment of Recovery Capital- BARC 10)": "barc-10",
        "Recommended Measures: (choice=The Brief Pain Inventory 24 Hour- BPI (Copyrighted))": "NA",
        "Recommended Measures: (choice=Treatment Satisfaction  [IMPOWR])": "NA"
    }

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
            if row["Project Number"].strip() and row["Project Number"].strip() != "n/a":
                if project_number is not None:
                    raise RuntimeError(f"Found multiple project numbers for record {record_id}")
                project_number = row["Project Number"].strip()

            # Figure out the project title.
            if row["Project Title  &nbsp; "].strip():
                if project_title is not None:
                    raise RuntimeError(f"Found multiple project numbers for record {record_id}")
                project_title = row["Project Title  &nbsp; "].strip()

            # Look for measure names.
            measure_name_rows = {
                'Measure Name',
                'Name of Measure',
                'Name of Other Measure'
            }
            for measure_name_row in measure_name_rows:
                if row[measure_name_row].strip():
                    measure_names.add(row[measure_name_row].strip())

            # TODO: figure out "How does your study  intend to monitor opioid use?"

            # Look for headers that are really CRFs.
            for measure_header in header_crf_mappings.keys():
                if row[measure_header].strip():
                    value = row[measure_header].strip()
                    if value.lower() == "checked":
                        logging.debug(f"Found checked measure '{measure_header}' in row {row}")
                        measure_names.add(measure_header)
                    elif value.lower() == "unchecked":
                        logging.debug(f"Found unchecked measure '{header_crf_mappings[measure_header]}' in row {row}")
                    else:
                        raise RuntimeError(f"Unexpected value '{value}' for CRF header {measure_header} in row {row}")

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

    # Write it to stdout.
    writer = csv.DictWriter(
        click.get_text_stream('stdout'),
        fieldnames=[
            'project_number',
            'hdp_id',
            'project_title',
            'crf_name',
            'heal_crf_id',
            'source',
            'input_path'
        ]
    )
    writer.writeheader()
    for project_number in project_crfs.keys():
        if project_number not in project_number_to_hdp_id:
            raise RuntimeError(f"Project '{project_number}' is missing from the study to HDP ID mappings.")

        for crf_name in project_crfs[project_number]["crfs"]:
            if crf_name in crf_to_heal_cde_id:
                heal_crf_id = HEAL_CDE_PREFIX + crf_to_heal_cde_id[crf_name]
                mapping_source = measure_to_heal_cde_id_path
            elif crf_name in header_crf_mappings:
                heal_crf_id = HEAL_CDE_PREFIX + header_crf_mappings[crf_name]
                mapping_source = "Internal header_crf_mappings column mappings"
            else:
                raise RuntimeError(f"CRF '{crf_name}' is missing from the measure to HEAL CDE ID mappings.")

            if heal_crf_id == HEAL_CDE_PREFIX + 'NA':
                # NA! Ignore.
                continue

            writer.writerow({
                'project_number': project_number,
                'hdp_id': project_number_to_hdp_id[project_number],
                'project_title': project_crfs[project_number].get("project_title", ""),
                'crf_name': crf_name,
                'heal_crf_id': heal_crf_id,
                'source': mapping_source,
                'input_path': input_path,
            })


if __name__ == "__main__":
    extract_study_mappings()
