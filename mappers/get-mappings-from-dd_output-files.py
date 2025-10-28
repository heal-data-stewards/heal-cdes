#!/usr/bin/python
#
# get-mappings-from-dd_output-files.py - Extract mappings from the dd_output files in the HEAL CDE Repository
#
# SYNOPSIS
#   python mappers/get-mappings-from-dd_output-files.py -o mappings/heal-cde-to-id-mappings/dd_output-mappings.csv ../heal-data-dictionaries/data-dictionaries
#
# DD_OUTPUT FILES
#   These are Excel files created by Liezl's workflow on identifying candidates for mapping variables in data dictionaries,
#   which can be identified by (1) they are in `CDEs` directories and (2) their name starts with `DD_*`, sometimes `DD_output_*`.
#   This Excel file MUST have an "EnhancedDD" tab with the following columns:
#   - "Manual Validation": The manually validated CRF match. Will be "No HEAL CRF match" if none could be found.
#   They may optionally have the following columns as well:
#   - module: The subsection of the data dictionary
#   - name: The variable that each row of the file refers to.
#   - ???: The variable name from the HEAL CDE definition of this CRF.
#
#   In order to find the HDP identifiers for this data dictionary, we will need to go to the directory above the `CDEs`
#   directory, and then look for `vlmd/` directories with a `metadata.yaml` file containg a `Project`/`HDP_ID` field.
#
# MAPPING FILES
#   The mapping file produced by this script are CSV files that will be used by summarizers/finalize-json.py to add
#   mappings to the final KGX files. These MUST contain the following columns to support Study/CRF mappings:
#   - heal_crf_id (e.g. "HEALCDE:NA"): the ID used to refer to these CDEs. For now this will be our internal identifiers
#     generated from the HEAL CDE Repository, but eventually these will be HEAL CDE IDs from the Platform MDS.
#   - hdp_id (e.g. "HDP00980|HDP00125|HDP00337"): A list of HDP IDs that these CRFs are associated with.
#
#   In the future, we will use two other fields to support Variable/CDE mappings:
#   - variable_name: The variable name from the data dictionary
#   - heal_cde_variable_name: The variable name from the HEAL CDE definition of this CRF, or the numerical index value
#     of the CDE within the CRF (i.e. 1, 2, 3, ...).
import csv
import os
import logging
from collections import defaultdict
from dataclasses import dataclass

import click
import openpyxl
import yaml
import pandas

# Configuration
MAX_EXCEL_ROWS = 200_000
MANUAL_VALIDATION_NA_VALUES = {
    "No CRF match",
    "No HEAL CRF match",
    "No HEAL CRF Match",
    "No HEAL CRF match, but related",
    "No HEAL CRF Match, but related",
    "No HEAL CRF Match, related topic",
    "but related",
    "related topic",
    "but close"
}

def is_candidate_mappings_file(filename):
    filename_lower = filename.lower()

    if filename.startswith('~$'):
        # Excel temporary file, definitely not.
        return False

    if filename_lower.startswith('dd_') and filename_lower.endswith('.xlsx'):
        # First round DD_output file.
        return True
    elif '_matches_confirmed' in filename_lower:
        # New style DD_output file (as of 2025aug14 or https://github.com/uc-cdis/heal-data-dictionaries/pull/529)
        return True
    else:
        return False

# Set up logging.
logging.basicConfig(level=logging.INFO)

@dataclass
class StudyCRFMapping:
    xlsx_filename: str
    hdp_ids: set[str]
    crf_name: str
    crf_ids: list[str]
    form_name: str
    variable_names: set[str]

def extract_mappings_from_dd_output_xlsx_file(xlsx_filename, hdp_ids, name_to_crf_ids) -> list[StudyCRFMapping]:
    """
    Extract mappings from a DD_output-formatted XLSX file.

    :param xlsx_filename: The full path to the DD_output XLSX file to extract mappings from.
    :param hdp_ids: A set of HDP IDs associated with this DD_output file.
    :param name_to_crf_ids: A dictionary mapping CRF names to their corresponding CRF IDs.
    :return: A list of Mappings extracted from this Excel file.
    :raises ValueError: If the Excel file is not in the expected format.
    """

    sheet_names = ['EnhancedDD', 'VLMD_Results']
    found_sheet = False
    for sheet_name in sheet_names:
        try:
            df = pandas.read_excel(xlsx_filename, sheet_name=sheet_name, nrows=MAX_EXCEL_ROWS + 1)
            found_sheet = True
            break
        except ValueError as e:
            if "Worksheet named 'EnhancedDD' not found" in str(e):
                continue
            raise RuntimeError(f'Could not read Excel file {xlsx_filename}: ({type(e)}) {e}')

    if not found_sheet:
        # TODO: raise exception.
        workbook = openpyxl.load_workbook(xlsx_filename)
        raise RuntimeError(f'Could not find sheets {sheet_names} in {xlsx_filename}, sheets: {workbook.sheetnames}')

    # Too many rows?
    if len(df) > MAX_EXCEL_ROWS:
        # TODO: raise exception.
        logging.error(f'Found at least {len(df)} rows in {xlsx_filename}, which is more than the maximum of {MAX_EXCEL_ROWS}.')
        return []

    # Record unique form-CRF mappings.
    crf_by_form_name = defaultdict(dict)

    rows = df.to_dict(orient='records')
    for row in rows:
        logging.debug(f"{xlsx_filename}: {row}")

        # Make sure we actually have all three columns we need.
        if 'Form Name' in row:
            form_name = row.get('Form Name')
        elif 'module' in row:
            form_name = row.get('module')
        elif 'section' in row:
            form_name = row.get('section')
        else:
            raise ValueError(f"Missing required column in {xlsx_filename} (no form name column found): {row.keys()}")

        if 'Variable / Field Name' in row:
            variable_name = row.get('Variable / Field Name')
        elif 'name' in row:
            variable_name = row.get('name')
        else:
            raise ValueError(f"Missing required column in {xlsx_filename} (no variable name column found): {row.keys()}")

        CRF_NAME_COLUMNS = {'Manual Validation', 'Manual Verification', 'HEAL Core CRF Match'}
        crf_names = None
        for crf_name_column in CRF_NAME_COLUMNS:
            if crf_name_column in row:
                crf_names = row.get(crf_name_column)
                break
        if crf_names is None:
            raise ValueError(f"Missing required column in {xlsx_filename} (one of: {CRF_NAME_COLUMNS} must be present): {row.keys()}")

        # Skip any NA values.
        if crf_names in MANUAL_VALIDATION_NA_VALUES:
            continue

        # Any commas in crf_names?
        if ',' not in crf_names:
            crf_names = [crf_names]
        else:
            crf_names = map(lambda x: x.strip(), crf_names.split(','))

        for crf_name in crf_names:
            # Skip any NA values (some might show up here after splitting by comma)
            if crf_name in MANUAL_VALIDATION_NA_VALUES:
                continue

            crf_info = crf_by_form_name[crf_name]
            if form_name not in crf_info:
                logging.debug(f"Found new {form_name} for {crf_name} in {xlsx_filename}.")
                crf_info[form_name] = set()

            if variable_name is not None:
                crf_info[form_name].add(variable_name)

    # Translate crf_by_form_name into mappings.
    mappings = []
    for crf_name, form_name_to_variable_names in crf_by_form_name.items():
        for form_name in form_name_to_variable_names.keys():
            crf_ids = list(filter(lambda x: x, map(lambda x: x.get('HEAL CDE CURIE', '').strip(), name_to_crf_ids.get(crf_name, []))))

            if not crf_ids:
                # TODO: raise exception
                logging.error(f"Could not find any CRF IDs for '{crf_name}' in {xlsx_filename}.")

            if len(crf_ids) == 1 and crf_ids[0] == 'NA':
                # We don't have a mapping for this CRF yet -- skip it.
                # This is primarily for the Brief Pain Inventory (BPI).
                continue

            mappings.append(StudyCRFMapping(
                xlsx_filename=xlsx_filename,
                hdp_ids=hdp_ids,
                form_name=form_name,
                crf_name=crf_name,
                crf_ids=crf_ids,
                variable_names=form_name_to_variable_names[form_name]
            ))

    logging.info(f'Found {len(mappings)} mappings in {xlsx_filename}.')

    return mappings

def get_metadata_files_in_project_directory(project_dir):
    for root, _, files in os.walk(project_dir):
        for filename in files:
            file_path = os.path.join(root, filename)
            if '/vlmd/' in file_path and filename.lower() == 'metadata.yaml':
                yield file_path

@click.command()
@click.argument('input-dir', required=True, type=click.Path(dir_okay=True, file_okay=False))
@click.option('--crf-id-file', '-i', required=True, type=click.File('r'))
@click.option('--output-file', '-o', help='Output file to write mappings to.', type=click.File('w'), default='-')
def get_mappings_from_dd_output_files(input_dir, crf_id_file, output_file):
    count_candidate_files = 0
    count_candidate_files_without_metadata = 0

    # Load HEAL CRF IDs.
    crf_id_mappings = []
    name_to_crf_ids = defaultdict(list)

    crf_reader = csv.DictReader(crf_id_file)
    for row in crf_reader:
        crf_id_mappings.append(row)

        names = {row['CRF Name']}
        other_names = map(lambda n: n.strip(), row.get('Other Names', '').split('|'))
        if other_names:
            names.update(other_names)

        for name in names:
            name_to_crf_ids[name].append(row)

    # Tally up mappings.
    mappings = []

    # We need to recurse into input_dir and find (1) all `CDEs` directories and (2) corresponding `vlmd/*/metadata.yaml` files.
    for root, _, files in os.walk(input_dir):
        for filename in files:
            file_path = os.path.join(root, filename)
            if '/CDEs/' in file_path and is_candidate_mappings_file(filename):
                # Candidate file!
                #
                # Can we find VLMD files?
                project_dir = os.path.dirname(os.path.dirname(file_path))
                metadata_yaml_files = list(get_metadata_files_in_project_directory(project_dir))

                if metadata_yaml_files:
                    count_candidate_files += 1

                    hdp_ids = set()
                    for metadata_yaml_file in metadata_yaml_files:
                        with open(metadata_yaml_file, 'r') as yamlf:
                            document = yaml.safe_load(yamlf)
                            try:
                                hdp_id = document.get('Project', {}).get('HDP_ID')
                            except KeyError:
                                raise ValueError(f'Could not find HDP_ID in {metadata_yaml_file}')
                            hdp_ids.add(hdp_id)

                    logging.info(f'Found candidate DD_output file {file_path} with HDP IDs: {hdp_ids}.')
                    mappings.extend(extract_mappings_from_dd_output_xlsx_file(file_path, hdp_ids, name_to_crf_ids))

                else:
                    # TODO: change this into an exception.
                    logging.error(f'Found candidate DD_output file {file_path} WITHOUT metadata files.')
                    count_candidate_files_without_metadata += 1

    logging.info(f'Found {len(mappings)} mappings in {count_candidate_files} DD_output files and {count_candidate_files_without_metadata} without metadata files.')

    # Write mappings into the output file.
    writer = csv.DictWriter(output_file, fieldnames=['filename', 'hdp_id', 'crf_ids', 'crf_name', 'form_name', 'variable_names'])
    writer.writeheader()
    count_rows = 0
    for mapping in mappings:
        for hdp_id in mapping.hdp_ids:
            variable_names = mapping.variable_names
            if not variable_names or variable_names is None:
                variable_names = {}

            writer.writerow({
                'filename': mapping.xlsx_filename,
                'hdp_id': hdp_id,
                'crf_ids':  "|".join(sorted(mapping.crf_ids)),
                'crf_name': mapping.crf_name,
                'form_name': mapping.form_name,
                'variable_names': "|".join(sorted(variable_names)),
            })
            count_rows += 1

    logging.info(f'Wrote {count_rows} rows to {output_file.name}.')

if __name__ == "__main__":
    get_mappings_from_dd_output_files()
