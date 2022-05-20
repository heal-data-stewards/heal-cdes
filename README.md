# Scripts for managing HEAL CDEs

This repository contains several scripts for working with [HEAL CDEs].
Primarily, it converts the Excel representation of these HEAL CDEs into
a JSON representation based on the data model used by the 
[NIH CDE Repository] (see JSON Schemas for [Data Elements] and [Forms]),
and then converting these JSON files into other formats for use in
downstream tools.

## How to use

### Getting started

We use venv to maintain the list of packages.

```shell
$ python -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

### Generating JSON files

The script generators/excel2cde.py recursively converts Excel files in the
expected format into JSON files in the output directory (by default, to the
`output/json` directory).

```shell
$ python generators/excel2cde.py [input-directory] [--output output_directory]
```

  [HEAL CDEs]: https://heal.nih.gov/data/common-data-elements
  [NIH CDE Repository]: https://cde.nlm.nih.gov/
  [Data Elements]: https://cde.nlm.nih.gov/schema/de
  [Forms]: https://cde.nlm.nih.gov/schema/form