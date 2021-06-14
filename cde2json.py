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

# We need an input directory -- we recurse through this
# directory and process all XLSX files in that directory.
input_dir = config['INPUT_DIR']
logging.debug(f'Input directory: {input_dir}')

# Prepare output directory.
output_dir = config['OUTPUT_DIR']
if not os.path.exists(output_dir):
    os.mkdir(output_dir)
logging.debug(f'Output directory: {output_dir}')

def convert_xlsx_to_json(input_filename):
    rel_input_filename = os.path.relpath(input_filename, input_dir)
    output_filename = os.path.join(output_dir, os.path.splitext(rel_input_filename)[0] + '.json')
    dirname = os.path.dirname(output_filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    logging.info(f'Writing {input_filename} to {output_filename}')


for root, dirs, files in os.walk(input_dir, onerror=lambda err: logging.error(f'Error reading file: {err}'), followlinks=True):
    logging.debug(f' - Recursing into directory {root}')
    for filename in files:
        if filename.lower().endswith('.xlsx') or filename.lower().endswith('.csv'):
            filepath = os.path.join(root, filename)
            convert_xlsx_to_json(filepath)
