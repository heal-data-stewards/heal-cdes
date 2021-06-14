# A helper program to validate all the forms in the output directory.

# Libraries
import json
import os
import glob

# Carry out JSON Schema validation.
import jsonschema

# Active logging.
import logging
logging.basicConfig(level=logging.INFO)

# We read config from `.env`.
from dotenv import dotenv_values
config = {
    **dotenv_values(".env.default"), # default configuration
    **dotenv_values(".env"),         # override with user-specific® configuration
    **os.environ,                    # override loaded values with environment variables
}
output_dir = config['OUTPUT_DIR']

with open(config['FORM_JSON_SCHEMA']) as f:
    form_json_schema = json.load(f)

jsonschema.Draft7Validator.check_schema(form_json_schema)
validator = jsonschema.Draft7Validator(form_json_schema)

valid_count = 0
for filename in glob.iglob(output_dir + '/**/*.json', recursive=True):
    logging.info(f'Validating {filename}')
    with open(filename) as f:
        data = json.load(f)
        validator.validate(data)
        valid_count += 1

logging.info(f'Validated {valid_count} JSON files.')