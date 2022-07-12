# HEAL CDE Tools

## Setting up virtual environment

You should run this program in a virtual environment.

```shell
$ python -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

## Converting HEAL files from Excel to JSON

If the HEAL files are in e.g. `/Users/gaurav/Library/CloudStorage/Box-Box/NIH\ HEAL\ Common\ Data\ Elements`,
then you can convert these files into JSON by running:

```shell
$ mkdir -p output/json
$ INPUT_DIR=/Users/gaurav/Library/CloudStorage/Box-Box/NIH\ HEAL\ Common\ Data\ Elements OUTPUT_DIR=output/json python excel2cde.py
```

## Converting JSON files to Excel templates

Excel template generation can be configured with the `input/cde-template-locations.yaml`
file. Note particularly the `template` variable, which should be set to the location
of the XLSX template (`input/cde-template.xlsx` by default). You should then run:

```shell
$ python exporters/xlsx-exporter.py -c input/cde-template-locations.yaml -o output/xlsx output/json
```

