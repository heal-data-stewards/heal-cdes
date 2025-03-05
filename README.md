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

### Generating HEAL CDE information

```shell
$ mkdir output-2024apr27
$ source venv/bin/activate
$ python downloaders/heal_cde_repo_downloader.py output-2024apr27
```

### Incorporate HEAL CDE exports

```shell
$ python summarizers/extract-study-mappings-from-heal-cde-team-export.py mappings/study-crf-mappings/from-heal-cde-team/HEALCommonDataElemen_DATA_LABELS_2025-03-03_1124.csv --study-to-hdpid mappings/study-crf-mappings/from-heal-cde-team/study-hdp-ids.csv --measure-to-heal-cde-id mappings/study-crf-mappings/from-heal-cde-team/crf-heal-cde-ids.csv > mappings/study-crf-mappings/study-crf-mappings.csv
```

### Generating JSON files

The script generators/excel2cde.py recursively converts Excel files in the
expected format into JSON files in the output directory (by default, to the
`output/json` directory).

```shell
$ python generators/excel2cde.py [input-directory] [--output output_directory]
```

## Converting JSON files to Excel templates

Excel template generation can be configured with the `input/cde-template-locations.yaml`
file. Note particularly the `template` variable, which should be set to the location
of the XLSX template (`input/cde-template.xlsx` by default). You should then run:

```shell
$ python exporters/xlsx-exporter.py -c input/cde-template-locations.yaml -o output/xlsx output/json
```

### Annotating JSON files

Annotation generally requires sending the HEAL CDE text content to an
online annotation process, following by using the Translator [Node Normalization]
service to filter and standardize the resulting annotations. This reliance
on online services causes several possible points of failure. To mitigate
this, the annotation workflow is intended to be run through a [Rakefile].
The [Rakefile in this repo] contains instructions for building the annotated
KGX output into the `annotated/` directory.

```shell
$ rake
$ python validators/check_annotated.py annotated
$ mv annotated annotated/year-month-day
```


  [HEAL CDEs]: https://heal.nih.gov/data/common-data-elements
  [NIH CDE Repository]: https://cde.nlm.nih.gov/
  [Data Elements]: https://cde.nlm.nih.gov/schema/de
  [Forms]: https://cde.nlm.nih.gov/schema/form
  [Node Normalization]: nodenormalization-sri.renci.org/
  [Rakefile]: https://ruby.github.io/rake/doc/rakefile_rdoc.html
  [Rakefile in this repo]: ./Rakefile
