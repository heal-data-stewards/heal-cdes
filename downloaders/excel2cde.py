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

    result = ''

    if key in row:
        result = row[key]
    elif key == 'External Id CDISC':
        result = row.get('External ID CDISC', '')
    elif key == 'CDE Name':
        result = row.get('Data Element Name', '')

    return result.strip()


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
            pv = { 'value': str(value) }
            if description is not None:
                pv['description'] = str(description)
            pvs.append(pv)

    return pvs


# Translate a question into formElements.
def convert_question_to_formelement(row, crf_curie, colname_varname='CDE Name'):
    """
    Convert an individual question to a form element.

    :param row: The row containing the question to be converted.
    :return: The form element as a dictionary representing this question, or None if none could be converted.
    """

    # Fields being dropped:
    #   - CRF Question #

    # Skip the CDISC warning line.
    if get_value(row, colname_varname).startswith('This CDE detail form is not CDISC compliant.'):
        return None

    if get_value(row, colname_varname).startswith("This CDE detail form\xa0is not CDISC compliant."):
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

    # Check if the id/name/description are blank.
    element_id = get_value(row, 'Variable Name')
    element_name = get_value(row, colname_varname)
    element_description = get_value(row, 'Definition')
    if element_id == '':
        # If the name and description is also blank, then we can
        # assume that this is a blank row, and we can skip it.
        # Otherwise, we complain.
        if element_name == '' and element_description == '':
            return None

        raise ValueError(f"Found Excel row missing element_id ('Variable Name'): {row}")

    # For the Dug Data model, we should split up the permissible values into a list of permissible values (`enum`)
    # and mappings from values to strings (`permissible_values`).
    permissible_values_list = convert_permissible_values(row)
    enum_values = []
    permissible_values = {}
    for pv in permissible_values_list:
        value = pv['value']
        if value in enum_values:
            logging.warning(f'Duplicate value {value} in permissible values for {element_name}')
        enum_values.append(value)

        description = pv.get('description', '').strip()

        # If the description includes the value, remove them.
        if description.startswith(value + ' = ') or description.startswith(value + ' - '):
            description = description[len(str(value)) + 3:]
        elif description.startswith(value + '= '):
            description = description[len(str(value)) + 2:]
        elif description.startswith(value + '='):
            description = description[len(str(value)) + 1:]


        if description:
            # Don't write out a description if it's empty.
            permissible_values[value] = description

    # Figure out minimum and maximum values if possible.
    min_value = None
    max_value = None
    if len(enum_values) == 1 and 'to' in enum_values[0]:
        range = re.match(r'^(\d+) to (\d+)$', enum_values[0])
        if range:
            min_value = int(range.group(1))
            max_value = int(range.group(2))

    # Figure out the data type.
    row_data_type = row.get('Data Type').lower().strip()
    data_type = 'string'
    if row_data_type in {'alphanumeric values.', 'alphanumeric'}:
        # We don't have a specific alphanumeric value, so let's go with string.
        data_type = 'string'
    elif row_data_type in {'free text', 'free-form entry'}:
        data_type = 'string'
    elif row_data_type in {'datetime'}:
        data_type = 'datetime'
    elif row_data_type in {'date'}:
        data_type = 'date'
    elif row_data_type in {'time'}:
        data_type = 'time'
    elif row_data_type in {
        'numeric value',
        'numeric values',
        'numerical values',
        'numericvalues',
        'numeric',
        # Calculated values.
        'numeric (calculated)',
        # No type provided
        ''
    } or row_data_type.startswith('calculate') or row_data_type.startswith('derive'):
        # Is this an enumerated type?
        if not enum_values:
            data_type = 'number'
        else:
            # Check if every value in enum_values is an integer.
            if all(map(lambda x: x.isdigit(), enum_values)):
                data_type = 'integer'
            else:
                data_type = 'number'
    else:
        raise ValueError(f"Unknown data type '{row_data_type}' in {row} in CRF {crf_curie}")

    # Compile metadata, including optional values.
    metadata = {
        'short_description': str(row.get('Short Description')),
        'crf_name': str(row.get('CRF Name')),
        'question_text': str(row.get('Additional Notes (Question Text)')),
        'references': str(row.get('Disease Specific References')),

        # These fields get interpreted as numeric values by ElasticSearch, so we need to stop producing them
        # until we actually need them.
        # 'question_number': str(row.get('CRF Question #')),
    }
    if enum_values:
        metadata['enum'] = enum_values
    if permissible_values:
        metadata['permissible_values'] = permissible_values
    if min_value:
        metadata['minimum'] = min_value
    if max_value:
        metadata['maximum'] = max_value

    disease_specific_instructions = str(row.get('Disease Specific Instructions')).strip()
    if disease_specific_instructions is not None and disease_specific_instructions != '':
        metadata['instructions'] = disease_specific_instructions

    # Should conform to DugVariable
    form_element = {
        'type': 'variable',
        'id': str(element_id),
        'name': str(element_name),
        'description': str(element_description),
        'data_type': data_type,
        'is_cde': True,
        'parents': [
            crf_curie
        ],
        'metadata': metadata,
    }

    return form_element


# Code to convert an XLSX file to JSON.
def convert_xlsx_to_json(input_filename, crf_curie):
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
        logging.warning(f'{input_filename} has too many sheets ({db.ws_names}), defaulting to first sheet {db.ws_names[0]}.')

    sheet1: pylightxl.pylightxl.Worksheet = db.ws(db.ws_names[0])

    if sheet1.address(address='A1') == 'Locating Supplemental Questionnaires on the NIH Box Account':
        logging.warning(f'Skipping {input_filename}: this is the list of supplemental questionnaires')
        return

    cols = None
    rows = []
    colname_varname = None
    for row in sheet1.rows:
        if cols is None:
            cols = row
            # Make sure we have a `CDE Name` column.
            if 'CDE Name' in cols:
                colname_varname = 'CDE Name'
            elif 'Variable Name' in cols:
                colname_varname = 'Variable Name'
            elif 'Data Element Name' in cols:
                colname_varname = 'Data Element Name'
            else:
                raise RuntimeError(f'Missing required column "CDE Name" in {input_filename}: {cols}')
        else:
            data = dict(zip(cols, row))
            cde_name = get_value(data, colname_varname)
            if cde_name is not None and cde_name != '':
                rows.append(data)
            elif data['Variable Name'].startswith('This CDE detail form is not CDISC compliant.'):
                # We can silently skip this.
                pass
            elif data['Variable Name'].startswith("This CDE detail form\xa0is not CDISC compliant."):
                # We can silently skip this.
                pass
            elif data['CRF Name'] == '' and data['Variable Name'] == '' and data['CRF Question #'] == '':
                # We can silently skip this.
                pass
            else:
                logging.warning(f'Found row with no CDE Name/variable name (column {colname_varname}): {data}')

    if len(rows) == 0:
        logging.warning(f'No form elements found in {input_filename}')

    # Write the entire thing to JSON for now.
    # We use this schema: https://cde.nlm.nih.gov/schema/form
    logging.info(f'Generated {len(rows)} rows as JSON')

    form_elements = list(filter(lambda e: e is not None, [convert_question_to_formelement(row, crf_curie, colname_varname) for row in rows]))

    form_data = {
        'source': f'Generated from HEAL CDE source file by cde2json.py {version}: {input_filename}',
        'designations': [], # TODO: need to replace with URLs and suchlike
        'created': datetime.datetime.now().astimezone().replace(microsecond=0).isoformat(),
        'formElements': form_elements,
    }

    form_data['designations'].extend(list(map(lambda name: {'designation': name}, list(set([row['CRF Name'] for row in rows if row['CRF Name'] != ''])))))

    return form_data


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
@click.option('--crf-id', default='HEALCDE:NONE', type=str)
def excel2cde(input_dir, output, crf_id):
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
                convert_xlsx_to_json(filepath, crf_id)


if __name__ == '__main__':
    excel2cde()
