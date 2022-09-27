# Python libraries
import json
import datetime
import re
import subprocess

# For accessing Excel files.
import pylightxl

# Add logging support
import logging

# For command line arguments
import click

# We read config from `.env`.
import os
from dotenv import dotenv_values
config = {
    **dotenv_values("../.env.default"), # default configuration
    **dotenv_values("../.env"),         # override with user-specific® configuration
    **os.environ,                    # override loaded values with environment variables
}

# Set up logging.
logging.basicConfig(level=logging.INFO)

# Read this program's "version" from git describe.
version = subprocess.check_output(["git", "describe", "--all"]).strip()


# Helper method: the Excel files refer to the same columns by different names.
# We handle the retrieval process here.
def get_value(row: dict[str, str], key: str):
    """
    Return the value for a particular key in a particular row.
    Since some files use alternate spellings of a particular name, we test those here.

    :param row: The row from which the value should be retrieved.
    :param key: The string key value to use to retrieve the value.
    :return: The value corresponding to that key in the input row.
    """

    if key in row:
        return row[key]

    if key == 'External Id CDISC':
        return row.get('External ID CDISC')

    if key == 'CDE Name':
        return row.get('Data Element Name')

    return None


def convert_permissible_values(row):
    """
    Convert permissible values into a list of permissible values as

    :param row:
    :return:
    """
    values = str(row.get('Permissible Values')).strip()
    descriptions = str(row.get('PV Description')).strip()

    pvs = []

    if values != '':
        pv_regex = re.compile(r'\s*;\s*')
        split_values = pv_regex.split(values)
        split_descriptions = [None] * len(split_values)
        if descriptions != '':
            split_descriptions = pv_regex.split(descriptions)

        for value, description in zip(split_values, split_descriptions):
            pv = { 'permissibleValue': value }
            if description is not None:
                pv['valueMeaningDefinition'] = description
            pvs.append(pv)

    return pvs


# Translate a question into formElements.
def convert_question_to_formelement(row):
    """
    Convert an individual question to a form element.

    :param row: The row containing the question to be converted.
    :return: The form element as a dictionary representing this question, or None if none could be converted.
    """

    # Fields being dropped:
    #   - CRF Question #

    # Skip the CDISC warning line.
    if get_value(row, 'CDE Name').startswith('This CDE detail form is not CDISC compliant.'):
        return None

    definitions = []
    if row.get('Definition') is not None and row.get('Definition') != '':
        if row.get('Disease Specific References') is not None and row.get('Disease Specific References') != '':
            definitions.append({
                'definition': row.get('Definition'),
                'sources': [
                    row.get('Disease Specific References')
                ]
            })
        else:
            definitions.append({
                'definition': row.get('Definition'),
            })

    if row.get('Short Description') is not None and row.get('Short Description') != '':
        definitions.append({
            'definition': f"Short description: {row.get('Short Description')}",
        })

    designations = []
    if row.get('Variable Name') is not None and row.get('Variable Name') != '':
        designations.append({
            'designation': f"Variable name: {row.get('Variable Name')}"
        })
    if row.get('Additional Notes (Question Text)') is not None and row.get('Additional Notes (Question Text)') != '':
        designations.append({
            'designation': f"Additional notes (question text): {row.get('Additional Notes (Question Text)')}"
        })

    ids = []
    external_id_cdisc = get_value(row, 'External Id CDISC')
    if external_id_cdisc is not None and external_id_cdisc != '':
        cdisc_id = external_id_cdisc
        ids.append({
            'source': 'NCIT',
            'id': cdisc_id
        })
        if re.compile(r'^C\d+$').match(cdisc_id):
            ids.append({
                'source': 'NCIT_URL',
                'id': f"https://ncit.nci.nih.gov/ncitbrowser/ConceptReport.jsp?dictionary=NCI_Thesaurus&ns=ncit&code={cdisc_id}"
            })

    form_element = {
        'elementType': 'question',
        'label': row.get('Additional Notes (Question Text)', ''),
        'question': {
            'cde': {
                'name': get_value(row, 'CDE Name'),
                'newCde': {
                    'definitions': definitions,
                    'designations': designations
                },
                'datatype': row.get('Data Type'),
                'ids': ids,
                'permissibleValues': convert_permissible_values(row)
            }
        }
    }

    if row.get('Disease Specific Instructions') is not None and row.get('Disease Specific Instructions') != '':
        form_element['instructions'] = {
            'value': row.get('Disease Specific Instructions'),
            'valueFormat': 'text'
        }

    return form_element


# Code to convert an XLSX file to JSON.
def convert_xlsx_to_json(input_filename, input_dir, output_dir) -> None:
    """
    Convert an XLSX file to a JSON file. We generate the JSON filename based on the command line arguments.

    :param input_filename: The XLSX filename to convert.
    :return: Nothing.
    """

    basename = os.path.basename(input_filename)
    if basename.startswith('~$'):
        # Temporary file, skip.
        return

    try:
        db = pylightxl.readxl(fn=input_filename)
    except TypeError as ex:
        logging.error(f'Could not read {input_filename} as XLSX: {ex}')
        return

    if len(db.ws_names) > 1:
        logging.error(f'Skipping {input_filename}: too many sheets ({db.ws_names})')
        return

    sheet1: pylightxl.pylightxl.Worksheet = db.ws(db.ws_names[0])

    if sheet1.address(address='A1') == 'Locating Supplemental Questionnaires on the NIH Box Account':
        logging.warning(f'Skipping {input_filename}: this is the list of supplemental questionnaires')
        return

    rel_input_filename = os.path.relpath(input_filename, input_dir)
    output_filename = os.path.join(output_dir, os.path.splitext(rel_input_filename)[0] + '.json')
    dirname = os.path.dirname(output_filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    logging.info(f'Writing {input_filename} to {output_filename}')

    cols = None
    rows = []
    for row in sheet1.rows:
        if cols is None:
            cols = row
        else:
            data = dict(zip(cols, row))
            cde_name = get_value(data, 'CDE Name')
            if cde_name is not None and cde_name != '':
                rows.append(data)

    if len(rows) == 0:
        logging.warning(f'No form elements found in {input_filename}')

    # Write the entire thing to JSON for now.
    # We use this schema: https://cde.nlm.nih.gov/schema/form
    with open(output_filename, 'w') as f:
        logging.info(f'Wrote {len(rows)} rows to {output_filename}')

        form_data = {
            'source': f'Generated from HEAL CDE source file by cde2json.py {version}: {rel_input_filename}',
            'created': datetime.datetime.now().astimezone().replace(microsecond=0).isoformat(),
            'designations': [{
                'designation': f"Filename: {os.path.basename(output_filename)}",
            }, {
                'designation': f"File path: {os.path.dirname(rel_input_filename)}"
            }],
            'formElements': list(filter(lambda e: e is not None, map(convert_question_to_formelement, rows))),
        }

        form_data['designations'].extend(list(map(lambda name: {'designation': name}, list(set([row['CRF Name'] for row in rows if row['CRF Name'] != ''])))))

        json.dump(form_data, f, indent=2)


# Process input commands
@click.command()
@click.argument('input-dir', required=True, type=click.Path(
    exists=True,
    file_okay=False,
    dir_okay=True,
    allow_dash=False
))
@click.option('--output', default='output/json', type=click.Path(
    exists=False,
    file_okay=False,
    dir_okay=True,
    allow_dash=False
))
def excel2cde(input_dir, output):
    input_dir = click.format_filename(input_dir)
    output_dir = click.format_filename(output)

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    logging.debug(f'Input directory: {input_dir}')
    logging.debug(f'Output directory: {output_dir}')

    iterator = os.walk(input_dir, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
    for root, dirs, files in iterator:
        logging.debug(f' - Recursing into directory {root}')
        for filename in files:
            if filename.lower().endswith('.xlsx') or filename.lower().endswith('.csv'):
                filepath = os.path.join(root, filename)
                convert_xlsx_to_json(filepath, input_dir, output_dir)


if __name__ == '__main__':
    excel2cde()
