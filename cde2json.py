# Python libraries
import json
import datetime
import subprocess

# For accessing Excel files.
import pylightxl

# Add logging support
import logging
logging.basicConfig(level=logging.INFO)

# We read config from `.env`.
import os
from dotenv import dotenv_values
config = {
    **dotenv_values(".env.default"), # default configuration
    **dotenv_values(".env"),         # override with user-specific® configuration
    **os.environ,                    # override loaded values with environment variables
}

# Read this program's "version" from git describe.
version = subprocess.check_output(["git", "describe", "--all"]).strip()

# We need an input directory -- we recurse through this
# directory and process all XLSX files in that directory.
input_dir = config['INPUT_DIR']
logging.debug(f'Input directory: {input_dir}')

# Prepare output directory.
output_dir = config['OUTPUT_DIR']
if not os.path.exists(output_dir):
    os.mkdir(output_dir)
logging.debug(f'Output directory: {output_dir}')

# Code to convert an XLSX file to JSON.
def convert_xlsx_to_json(input_filename):
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

    rel_input_filename = os.path.relpath(input_filename, input_dir)
    output_filename = os.path.join(output_dir, os.path.splitext(rel_input_filename)[0] + '.json')
    dirname = os.path.dirname(output_filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    logging.info(f'Writing {input_filename} to {output_filename}')

    sheet1 = db.ws_names[0]
    cols = None
    rows = []
    for row in db.ws(sheet1).rows:
        if cols is None:
            cols = row
        else:
            data = dict(zip(cols, row))
            cde_name = data.get('CDE Name')
            if cde_name is not None and cde_name != '':
                rows.append(data)

    # Write the entire thing to JSON for now.
    # We use this schema: https://cde.nlm.nih.gov/schema/form
    with open(output_filename, 'w') as f:
        logging.info(f'Wrote {len(rows)} rows to {output_filename}')
        json.dump({
            'source': f'Generated from HEAL CDE source file by cde2json.py {version}: {rel_input_filename}',
            'created': datetime.datetime.now().astimezone().replace(microsecond=0).isoformat(),
            'formElements': rows,
        }, f, indent=2)


iterator = os.walk(input_dir, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True)
for root, dirs, files in iterator:
    logging.debug(f' - Recursing into directory {root}')
    for filename in files:
        if filename.lower().endswith('.xlsx') or filename.lower().endswith('.csv'):
            filepath = os.path.join(root, filename)
            convert_xlsx_to_json(filepath)
