#
# The Rakefile is used to perform annotation, but we now need to prepare and convert several study/CRF mappings (and,
# eventually, variable/CDEs mappings), so I'll put those instructions into this Makefile. Ideally, this Makefile
# will call the Rakefile at the right time to generate these outputs.
#
# Yes, I know this is a mess. I'll clean it up later.
#

# CONFIGURATION
# These will be updated every time this script is run.
OUTPUT_DIR = ./output
MAPPINGS_DIR = ./mappings
HEAL_CDE_EXPORT_FILE=$(MAPPINGS_DIR)/study-crf-mappings/from-heal-cde-team/HEALCommonDataElemen_DATA_LABELS_2025-07-24_0948.csv
HEAL_CDE_STUDY_HDPID_MAPPING_FILE=$(MAPPINGS_DIR)/study-crf-mappings/from-heal-cde-team/study-hdp-ids.csv
HEAL_CDE_HEAL_CDE_IDS_MAPPING_FILE=mappings/study-crf-mappings/from-heal-cde-team/crf-heal-cde-ids.csv

# Remote directories
# - Where is the HEAL Data Dictionaries repository?
HEAL_DATA_DICTIONARIES_DIR=../heal-data-dictionaries

# Additional inputs
HEAL_CRF_ID_CSV = $(MAPPINGS_DIR)/heal-crf-ids/heal-crf-ids.csv

# Overall targets
# TODO: add $(OUTPUT_DIR)/done
all: $(MAPPINGS_DIR)/done

clean:
	rm -rf $(OUTPUT_DIR)

.PHONY: all clean

# MAPPINGS
# We generate mappings from three sources:
$(MAPPINGS_DIR)/done: $(HEAL_CRF_ID_CSV) $(MAPPINGS_DIR)/heal-data-dictionaries-mappings/dd_output-mappings.csv $(MAPPINGS_DIR)/study-crf-mappings/study-crf-mappings.csv

# MAPPING SOURCE 1: the dd_output files in the HEAL Data Dictionaries
$(MAPPINGS_DIR)/heal-data-dictionaries-mappings/dd_output-mappings.csv: $(HEAL_DATA_DICTIONARIES_DIR) $(HEAL_CRF_ID_CSV)
	mkdir -p $(MAPPINGS_DIR)/heal-data-dictionaries-mappings
	python mappers/get-mappings-from-dd_output-files.py $< -o $@ --crf-id-file $(HEAL_CRF_ID_CSV)

# MAPPING SOURCE 2: the mappings from the latest HEAL CDE team REDCap export.
$(MAPPINGS_DIR)/study-crf-mappings/study-crf-mappings.csv: $(HEAL_CDE_EXPORT_FILE) $(HEAL_CDE_STUDY_HDPID_MAPPING_FILE)
	mkdir -p $(MAPPINGS_DIR)/heal-readcap-export
	python study-mappings/extract-study-mappings-from-heal-cde-team-export.py $< --study-to-hdpid $(HEAL_CDE_HPDID_MAPPING_FILE) --measure-to-heal-cde-id $(HEAL_CDE_HEAL_CDE_IDS_MAPPING_FILE) > $@

# MAPPING SOURCE 3: the mappings from the HEAL MDS
