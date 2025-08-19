#
# The Rakefile is used to perform annotation, but we now need to prepare and convert several study/CRF mappings (and,
# eventually, variable/CDEs mappings), so I'll put those instructions into this Makefile. Ideally, this Makefile
# will call the Rakefile at the right time to generate these outputs.
#
# Yes, I know this is a mess. I'll clean it up later.
#

# CONFIGURATION
# Remote directories
# - Where is the HEAL Data Dictionaries repository?
HEAL_DATA_DICTIONARIES_DIR=../heal-data-dictionaries

# Our directories
OUTPUT_DIR = ./output
MAPPINGS_DIR = ./mappings

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
$(MAPPINGS_DIR)/done: $(HEAL_CRF_ID_CSV) $(MAPPINGS_DIR)/heal-data-dictionaries-mappings/dd_output-mappings.csv

# MAPPING SOURCE 1: the dd_output files in the HEAL Data Dictionaries
$(MAPPINGS_DIR)/heal-data-dictionaries-mappings/dd_output-mappings.csv: $(HEAL_DATA_DICTIONARIES_DIR) $(HEAL_CRF_ID_CSV)
	mkdir -p $(MAPPINGS_DIR)/heal-data-dictionaries-mappings
	python mappers/get-mappings-from-dd_output-files.py $< -o $@ --crf-id-file $(HEAL_CRF_ID_CSV)