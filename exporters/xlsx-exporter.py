#!/usr/bin/env python3

#
# This script can be used to export CDEs as XLSX files using an existing template.
#
# SYNOPSIS
#   python3 exporters/xlsx-exporter.py -c config.yaml ./output/json -o ./output/xlsx
#
# The YAML file should be in the format:
#   template: ./cde-template.xlsx
#   crf:
#     date_of_submission: 'Cover Sheet'!C7
#     submitted_by_firstname: 'Cover Sheet'!C10
#     submitted_by_lastname: 'Cover Sheet'!C11
#   cde:
#

import json
import re
import os
import logging

import openpyxl
import yaml

import click

# Set default logging level.
logging.basicConfig(level=logging.INFO)


def set_excel_cell(wb: openpyxl.Workbook, ref: str, value: str, offset:int=0, default_ws:str=''):
    """
    Modify a cell using an Excel cell reference (e.g. "'Sheet 1'!A1").

    :param wb: The OpenPyXL workbook to modify.
    :param ref: The Excel cell reference (e.g. "'Sheet 1'!A1").
    :param value: The value to set this cell to.
    :param default_ws: The default workstreet name to use if one is not included in the cell reference.
    """

    if '!' in ref:
        sheet_name = ref[0:ref.index('!')]
        if sheet_name.startswith("'") and sheet_name.endswith("'"):
            sheet_name = sheet_name[1:-1]
        cell_ref = ref[ref.index('!')+1:]
    elif default_ws != '':
        sheet_name = default_ws
        cell_ref = ref
    else:
        raise RuntimeError(f'No sheet name provided to set_excel_cell({wb}, {ref}, {value}, {default_ws})')

    if offset > 0:
        # What is the row number of the reference?
        m = re.match('^(\\w+)(\\d+)$', cell_ref)
        if not m:
            raise RuntimeError(f"Could not offset cell ref {cell_ref} by {offset}: could not parse cell ref")
        cell_ref = f"{m.group(1)}{int(m.group(2)) + offset}"

    wb.get_sheet_by_name(sheet_name)[cell_ref] = value


# Process individual files
def translate_file(config_path, input_file, output_path):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    logging.debug(f"Configuration: {config}")
    logging.info(f"Input file: {input_file}")
    logging.info(f"Output file: {output_path}")

    template_file_path = os.path.join(os.path.dirname(config_path), config['template'])
    logging.info(f"Template file: {template_file_path}")

    wb = openpyxl.load_workbook(template_file_path)
    logging.info(f'Loaded workbook with names: {wb.sheetnames}')

    # Set some values that are common for all inputs
    values = config.get('values', {})
    for key in values.keys():
        if key in config['crf']:
            set_excel_cell(wb, config['crf'][key], values[key])
        else:
            logging.warning(f"Location '{key}' is not defined in configuratino file.")

    # Load input file.
    with open(input_file, 'r') as f:
        crf = json.load(f)

    # Write some top-level metadata into the file.
    filename, ext = os.path.splitext(os.path.basename(input_file))
    set_excel_cell(wb, config['crf']['name_and_version_of_submission_package'], filename)

    filtered_designations = []
    for designation in crf['designations']:
        d = designation['designation']
        if d.startswith('File path: ') or d.startswith('Filename: '):
            pass
        else:
            filtered_designations.append(d)

    set_excel_cell(wb, config['crf']['source'], '|'.join(filtered_designations))

    # Read formElements from JSON file into Excel template.
    for index, element in enumerate(crf['formElements']):
        question = element['question']
        cde = question['cde']
        new_cde = cde['newCde']

        # Write to Excel sheets.
        set_excel_cell(wb, config['cde']['cde_name'], cde['name'], offset=index)
        set_excel_cell(wb, config['cde']['preferred_question_text'], element['label'], offset=index)

        # Determine data type
        data_type_mappings = {
            'value list': 'Value List',
            'derived using the tables in the user manual or by using automated software (e.g. healthmeasures or redcap)': 'Value List',
            'numeric value': 'Number',
            'numeric  value': 'Number',
            'numeric values': 'Number',
            'numerical values': 'Number',
            'numeric': 'Number',
            'calculated': 'Number',
            'numeric - calculated': 'Number',
            'calculated as the sum of all responses': 'Number',
            'free-form entry': 'Text',
            'n/a (descriptive)': 'Text',
            'text': 'Text',
            'time': 'Time',
            'date': 'Date',
            'datetime': 'Datetime',
            'date and time': 'Datetime',
            'date/time': 'Datetime',
            'alphanumeric': 'Other: alphanumeric',
            'alphanumeric value': 'Other: alphanumeric',
            'alphanumeric values': 'Other: alphanumeric',
            '': 'Text',
        }
        datatype = cde['datatype'].lower().strip()
        if datatype in data_type_mappings:
            set_excel_cell(wb, config['cde']['cde_data_type'], data_type_mappings[datatype], offset=index)
        else:
            logging.error(f"Unknown datatype {datatype}, using 'Text' instead.")
            set_excel_cell(wb, config['cde']['cde_data_type'], 'Text', offset=index)
            # raise RuntimeError(f"Unknown data type: '{datatype}'")

        # Definition
        definitions = new_cde['definitions']
        # We only want to the first definition -- this should skip the short description included in the input file.
        source = filename
        if len(definitions) == 0:
            definition = ""
        else:
            definition = definitions[0]['definition']
            sources = definitions[0].get('sources') or []
            if len(sources) > 0:
                source = sources[0]
        set_excel_cell(wb, config['cde']['cde_definition'], definition, offset=index)
        # set_excel_cell(wb, config['cde']['cde_source'], source, offset=index)
        set_excel_cell(wb, config['cde']['references_for_bundles'], source, offset=index)

        # DEC identifier and terminology source
        ids = cde['ids']
        dec_id = ""
        dec_source = ""
        for id in ids:
            if id['source'] == 'NCIT':
                dec_id = id['id']
                dec_source = id['source']

                result = re.search('(?:Code )?(C\\d+)', dec_id)
                if result:
                    logging.info(f"  - Identified NCIT code {dec_id} in text: {id['id']}")
                    dec_id = result.group(1)

        set_excel_cell(wb, config['cde']['dec_identifier'], dec_id, offset=index)
        set_excel_cell(wb, config['cde']['dec_concept_source'], dec_source, offset=index)

        # NLM Identifier for NIH CDE Repository
        # Mapping to Other Groups
        # CDE Type
        # Name of Composite or Bundle

        # PV labels and definitions
        pv_labels = []
        pv_definitions = []

        flag_has_at_least_one_definition = False
        for pv in cde['permissibleValues']:
            pv_label = pv['permissibleValue']
            pv_definition = pv.get('valueMeaningDefinition')
            if pv_definition:
                flag_has_at_least_one_definition = True
            else:
                pv_definition = ""

            # PV definitions sometimes start with the values, which we probably don't want to repeat.
            label_prefix = f"{pv_label} ="
            if pv_definition.startswith(label_prefix):
                pv_definition = pv_definition[len(label_prefix):].strip()

            pv_labels.append(pv_label)
            pv_definitions.append(pv_definition)

        set_excel_cell(wb, config['cde']['pv_labels'], '|'.join(pv_labels), offset=index)
        set_excel_cell(wb, config['cde']['pv_definitions'], '|'.join(pv_definitions), offset=index)

        if flag_has_at_least_one_definition:
            # If we have PVs, then the output data type must ALWAYS be 'Value List'
            set_excel_cell(wb, config['cde']['cde_data_type'], 'Value List', offset=index)

        # Permissible Value (PV) Concept Identifiers
        # Permissible Value (PV) Terminology Sources
        # Codes for Permissible Value
        # Permissible Value Code Systems
        # References (only for bundles)

        # License/Copyright Owner
        # License/Copyright Permission


    # Create directory and write output
    os.makedirs(os.path.dirname(os.path.normpath(output_path)), exist_ok=True)
    wb.save(output_path)


# Process input commands
@click.command()
@click.argument('input', type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=True,
    allow_dash=False
))
@click.option('--output', '-o', type=click.Path(
    exists=False,
    file_okay=False,
    dir_okay=True,
    allow_dash=False
))
@click.option('--config', '-c', type=click.Path(
    exists=True,
    file_okay=True,
    dir_okay=False,
    allow_dash=True
))
def main(input, output, config):
    input_path = click.format_filename(input)
    output_path = click.format_filename(output)
    config_path = click.format_filename(config)

    count_files = 0
    if os.path.isfile(input_path):
        # If the input is a single file, process just that one file.
        translate_file(config_path, input_path, output_path)
        count_files = 1
    else:
        # If it is a directory, then recurse through that directory looking for input files.
        iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
        for root, subdirs, files in iterator:
            logging.info(f' - Recursing into directory {root}')
            for filename in files:
                if filename.lower().endswith('.json'):
                    input_filepath = os.path.join(root, filename)
                    output_filepath_json = os.path.join(output_path, os.path.relpath(root, input_path), filename)
                    output_filepath_base, ext = os.path.splitext(output_filepath_json)
                    output_filepath = output_filepath_base + '.xlsx'
                    translate_file(config_path, input_filepath, output_filepath)
                    count_files += 1

    logging.info(
        f'Translated {count_files} files to {output_path}.'
    )


if __name__ == '__main__':
    main()
