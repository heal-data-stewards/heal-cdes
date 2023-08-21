# Snakemake file for transforming all annotations

configfile: "config.yaml"

INPUT_FILES = glob_wildcards(config["INPUT_DIR"] + '/{relative_input}.json')

#rule all:
#  output:
#    expand('{output_dir}/{input_file}.json', output_dir=config["OUTPUT_DIR"], input_file=INPUT_FILES.relative_input)

rule scigraph:
  input:
    meta_csv=config["META_CSV"],
    file=expand('{input_dir}/{input_file}.json', input_dir=config["INPUT_DIR"], input_file=INPUT_FILES.relative_input)
  output:
    expand('{output_dir}/{input_file}.json', output_dir=config["OUTPUT_DIR"], input_file=INPUT_FILES.relative_input)
  shell:
    "pipenv run python annotators/scigraph/scigraph-api-annotator.py {input.file} {input.meta_csv} --to-kgx {output}"

rule clean:
  shell:
    """
    rm -rf {config.OUTPUT_DIR}
    mkdir {config.OUTPUT_DIR}
    """
