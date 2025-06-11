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
import os
import logging
from dataclasses import dataclass

import click
import yaml

# Set up logging.
logging.basicConfig(level=logging.INFO)

@dataclass
class Mapping:
    heal_crf_id: str
    hdp_id: str
    module: str = ""
    variable_name: str = ""
    heal_cde_variable_name: str = ""

def extract_mappings_from_dd_output_xlsx_file(xlsx_filename, hdp_ids) -> list[Mapping]:
    """
    Extract mappings from a DD_output-formatted XLSX file.

    :param xlsx_filename: The full path to the DD_output XLSX file to extract mappings from.
    :return: A list of Mappings extracted from this Excel file.
    :raises ValueError: If the Excel file is not in the expected format.
    """
    pass

def get_metadata_files_in_project_directory(project_dir):
    for root, _, files in os.walk(project_dir):
        for filename in files:
            file_path = os.path.join(root, filename)
            if '/vlmd/' in file_path and filename.lower() == 'metadata.yaml':
                yield file_path

@click.command()
@click.argument('input-dir', required=True, type=click.Path(dir_okay=True, file_okay=False))
@click.option('--output-file', '-o', help='Output file to write mappings to.', type=click.File('w'), default='-')
def get_mappings_from_dd_output_files(input_dir, output_file):
    count_candidate_files = 0
    count_candidate_files_without_metadata = 0

    # We need to recurse into input_dir and find (1) all `CDEs` directories and (2) corresponding `vlmd/*/metadata.yaml` files.
    for root, _, files in os.walk(input_dir):
        for filename in files:
            file_path = os.path.join(root, filename)
            if '/CDEs/' in file_path and filename.lower().startswith('dd_') and file_path.lower().endswith('.xlsx'):
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

                else:
                    logging.warning(f'Found candidate DD_output file {file_path} WITHOUT metadata files.')
                    count_candidate_files_without_metadata += 1



    logging.info(f'Found {count_candidate_files} DD_output files and {count_candidate_files_without_metadata} without metadata files.')

if __name__ == "__main__":
    get_mappings_from_dd_output_files()