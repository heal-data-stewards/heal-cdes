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

import os
import logging
import json
import yaml

import click

# Set default logging level.
logging.basicConfig(level=logging.INFO)


# Process individual files
def translate_file(config_path, input_file, output_path):
    config = yaml.safe_load(config_path)
    logging.info(f"Configuration: {config}")
    logging.info(f"Input file: {input_file}")
    logging.info(f"Output file: {output_path}")


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
                    output_filepath = os.path.join(output_path, os.path.relpath(root, input_path), filename)
                    translate_file(config_path, input_filepath, output_filepath)
                    count_files += 1

    logging.info(
        f'Translated {count_files} files to {output_path}.'
    )


if __name__ == '__main__':
    main()
