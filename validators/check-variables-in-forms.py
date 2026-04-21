#
# This script reads all Dug Data Model v2 JSON files from a directory and produces:
#   1. A CSV listing every variable found across all forms, sorted alphabetically.
#   2. A report (to stderr via logging) of variables that appear in multiple forms.
#

import json
import csv
import os
import sys

import click
import logging
logging.basicConfig(level=logging.INFO)


@click.command()
@click.argument('input-dir', required=True, type=click.Path(
    exists=True,
    file_okay=False,
    dir_okay=True,
    allow_dash=False
))
@click.option('--output', '-o', type=click.File(mode='w'), default='-',
              help='CSV output file (default: stdout)')
def main(input_dir, output):
    """Read Dug Data Model v2 JSON files from INPUT_DIR and write a CSV of all variables.

    Also reports (to stderr) how many variables appear in more than one form.
    """
    input_path = click.format_filename(input_dir)

    # variable_id -> metadata dict
    variables: dict[str, dict] = {}

    # variable_id -> set of form ids that list this variable
    variable_to_forms: dict[str, set[str]] = {}

    count_files = 0
    count_sections = 0
    count_variable_objects = 0

    iterator = os.walk(input_path, onerror=lambda err: logging.error(f'Error reading directory: {err}'), followlinks=True)
    for root, dirs, files in iterator:
        for filename in sorted(files):
            if not filename.lower().endswith('.json'):
                continue

            filepath = os.path.join(root, filename)
            logging.debug(f'Processing {filepath}')

            with open(filepath, 'r') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    logging.error(f'Failed to parse {filepath}: {e}')
                    continue

            if not isinstance(data, list):
                logging.warning(f'{filepath}: expected a JSON array at top level, got {type(data).__name__}')
                continue

            count_files += 1

            # First pass: collect variable metadata
            for obj in data:
                obj_type = obj.get('type')
                if obj_type == 'variable':
                    var_id = obj.get('id')
                    if not var_id:
                        logging.warning(f'{filepath}: variable missing "id" field, skipping')
                        continue
                    count_variable_objects += 1
                    if var_id not in variables:
                        variables[var_id] = {
                            'variable_id': var_id,
                            'variable_name': obj.get('name', ''),
                            'data_type': obj.get('data_type', ''),
                            'description': obj.get('description', ''),
                        }
                    if var_id not in variable_to_forms:
                        variable_to_forms[var_id] = set()

            # Second pass: map variables to forms via section's variable_list
            for obj in data:
                if obj.get('type') == 'section':
                    form_id = obj.get('id')
                    if not form_id:
                        logging.warning(f'{filepath}: section missing "id" field, skipping')
                        continue
                    count_sections += 1
                    for var_id in obj.get('variable_list', []):
                        if var_id not in variable_to_forms:
                            variable_to_forms[var_id] = set()
                        variable_to_forms[var_id].add(form_id)
                        # Register variable even if we didn't see a variable object for it
                        if var_id not in variables:
                            logging.warning(f'{filepath}: form "{form_id}" references unknown variable "{var_id}"')
                            variables[var_id] = {
                                'variable_id': var_id,
                                'variable_name': '',
                                'data_type': '',
                                'description': '',
                            }

    logging.info(f'Processed {count_files} files: {count_sections} forms, {count_variable_objects} variable objects, {len(variables)} unique variables.')

    # Write CSV sorted alphabetically by variable_id
    fieldnames = ['variable_id', 'variable_name', 'data_type', 'form_count', 'form_ids', 'description']
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator='\n')
    writer.writeheader()

    for var_id in sorted(variables.keys()):
        meta = variables[var_id]
        form_ids = sorted(variable_to_forms.get(var_id, set()))
        writer.writerow({
            'variable_id': var_id,
            'variable_name': meta['variable_name'],
            'data_type': meta['data_type'],
            'form_count': len(form_ids),
            'form_ids': '; '.join(form_ids),
            'description': meta['description'],
        })

    output.close()

    # Report variables in multiple forms
    multi_form = {
        var_id: sorted(forms)
        for var_id, forms in variable_to_forms.items()
        if len(forms) > 1
    }
    single_form = sum(1 for forms in variable_to_forms.values() if len(forms) == 1)
    no_form = sum(1 for forms in variable_to_forms.values() if len(forms) == 0)

    logging.info(f'Variables in exactly 1 form: {single_form}')
    logging.info(f'Variables in 0 forms (orphaned): {no_form}')
    logging.info(f'Variables in 2+ forms: {len(multi_form)}')

    if multi_form:
        logging.info('Variables appearing in multiple forms:')
        for var_id, forms in sorted(multi_form.items()):
            logging.info(f'  {var_id}: {", ".join(forms)}')


if __name__ == '__main__':
    main()
