#
# The Rakefile is used to perform annotation, but we now need to prepare and convert several study/CRF mappings (and,
# eventually, variable/CDEs mappings), so I'll put those instructions into this Makefile. Ideally, this Makefile
# will call the Rakefile at the right time to generate these outputs.
#
# Yes, I know this is a mess. I'll clean it up later.
#

# CONFIGURATION
# These will be updated every time this script is run.
PYTHON = uv run python
OUTPUT_DIR = ./output-2026apr23-3
LOGFILE = "$(OUTPUT_DIR)/logs/log.txt"
MAPPINGS_DIR = ./mappings
HEAL_CDE_EXPORT_FILE=$(MAPPINGS_DIR)/study-crf-mappings/from-heal-cde-team/Heal_CDE_2026-04-06T130104/Heal_CDE_data.csv
HEAL_CDE_STUDY_HDPID_MAPPING_FILE=$(MAPPINGS_DIR)/study-crf-mappings/from-heal-cde-team/study-hdp-ids.csv
HEAL_CDE_HEAL_CDE_IDS_MAPPING_FILE=mappings/study-crf-mappings/from-heal-cde-team/crf-heal-cde-ids.csv

# Remote directories
# - Where is the HEAL Data Dictionaries repository?
HEAL_DATA_DICTIONARIES_DIR=../heal-data-dictionaries/
HEAL_CDE_MAPPINGS=../heal-cde-mappings/

# Additional inputs
HEAL_CRF_ID_CSV = $(MAPPINGS_DIR)/heal-crf-ids/heal-crf-ids.csv
HEAL_CDE_LIST_CSV  = $(MAPPINGS_DIR)/heal-cde-list/heal-cde-list.csv
HEAL_CDE_CORE_CSV  = $(MAPPINGS_DIR)/heal-cde-list/heal-cde-list-with-core.csv
HEAL_CDE_FINAL_CSV = $(MAPPINGS_DIR)/heal-cde-list/heal-cde-list-final.csv

# Overall targets
all: $(OUTPUT_DIR)/download_done $(OUTPUT_DIR)/logs/errors.txt $(OUTPUT_DIR)/logs/warnings.txt

clean:
	rm -f $(OUTPUT_DIR)/download_done
	rm -f $(HEAL_CDE_LIST_CSV)
	rm -f $(HEAL_CDE_CORE_CSV)
	rm -f $(HEAL_CDE_FINAL_CSV)
	rm -f $(MAPPINGS_DIR)/study-crf-mappings/study-crf-mappings.csv
	rm -f $(MAPPINGS_DIR)/platform-mds-mappings/platform-mds-mappings.csv
	rm -f $(MAPPINGS_DIR)/heal-data-dictionaries-mappings/heal-data-dictionaries.csv
	rm -f $(MAPPINGS_DIR)/heal-data-dictionaries-mappings/heal-cde-mappings.csv
	rm -rf $(OUTPUT_DIR)

.PHONY: all clean

# STEP 0. LOGGING
$(LOGFILE):
	# $(OUTPUT_DIR)/reports is only created after downloaders/heal_cde_repo_downloader.py has finished downloading
	# all its file and is almost done, so it's a good way to check whether there is existing content inside $(OUTPUT_DIR).
	@if [ -d "$(OUTPUT_DIR)/reports" ]; then \
		echo "ERROR: output directory has not been cleaned: you should run 'make clean'." >&2; \
		exit 1; \
	fi
	mkdir -p $(OUTPUT_DIR)
	mkdir -p "$(OUTPUT_DIR)/logs"
	rm -rf $@
	touch $@

# STAGE I. EXTRACT MAPPINGS
# We generate mappings from four sources:

# STEP I.1: the dd_output files in the HEAL Data Dictionaries
$(MAPPINGS_DIR)/heal-data-dictionaries-mappings/heal-data-dictionaries.csv: $(HEAL_DATA_DICTIONARIES_DIR) $(HEAL_CRF_ID_CSV) | $(LOGFILE)
	mkdir -p $(MAPPINGS_DIR)/heal-data-dictionaries-mappings
	@set -o pipefail; ${PYTHON} mappers/get-mappings-from-dd_output-files.py $< -o "$@.tmp" --crf-id-file $(HEAL_CRF_ID_CSV) 2>&1 | tee -a $(LOGFILE) && mv "$@.tmp" $@

# STEP I.2: the study/CRF and variable/CDE files in the HEAL CDE Mappings
$(MAPPINGS_DIR)/heal-data-dictionaries-mappings/heal-cde-mappings.csv: $(HEAL_CDE_MAPPINGS) $(HEAL_CRF_ID_CSV) | $(LOGFILE)
	mkdir -p $(MAPPINGS_DIR)/heal-data-dictionaries-mappings
	@set -o pipefail; ${PYTHON} mappers/get-mappings-from-dd_output-files.py $< -o "$@.tmp" --crf-id-file $(HEAL_CRF_ID_CSV) 2>&1 | tee -a $(LOGFILE) && mv "$@.tmp" $@

# STEP I.3: the mappings from the latest HEAL CDE team REDCap export
$(MAPPINGS_DIR)/study-crf-mappings/study-crf-mappings.csv: $(HEAL_CDE_EXPORT_FILE) $(HEAL_CDE_STUDY_HDPID_MAPPING_FILE) | $(LOGFILE)
	mkdir -p $(MAPPINGS_DIR)/study-crf-mappings
	@set -o pipefail; ${PYTHON} study-mappings/extract-study-mappings-from-heal-cde-team-export.py $< --study-to-hdpid $(HEAL_CDE_STUDY_HDPID_MAPPING_FILE) --measure-to-heal-cde-id $(HEAL_CDE_HEAL_CDE_IDS_MAPPING_FILE) --output "$@.tmp" 2>&1 | tee -a $(LOGFILE) \
		&& mv "$@.tmp" $@

# STEP I.4: the mappings from the HEAL MDS
$(MAPPINGS_DIR)/platform-mds-mappings/platform-mds-mappings.csv: $(HEAL_CRF_ID_CSV) | $(LOGFILE)
	mkdir -p $(MAPPINGS_DIR)/platform-mds-mappings
	@set -o pipefail; ${PYTHON} study-mappings/download-study-mappings-from-platform-mds.py --mappings $(HEAL_CRF_ID_CSV) --output "$@.tmp" 2>&1 | tee -a $(LOGFILE) && mv "$@.tmp" $@

# STAGE II. BUILD CDE LIST

# STEP II.1: Download the list of CDEs from the HEAL CDE website
$(HEAL_CDE_LIST_CSV): | $(LOGFILE)
	mkdir -p $(MAPPINGS_DIR)/heal-cde-list
	@set -o pipefail; ${PYTHON} downloaders/get_heal_cde_list.py "$@.tmp" 2>&1 | tee -a $(LOGFILE) && mv "$@.tmp" $@

# STEP II.2: Backstop core CDEs from the core taxonomy listing
$(HEAL_CDE_CORE_CSV): downloaders/get_heal_cde_taxonomy.py $(HEAL_CDE_LIST_CSV) | $(LOGFILE)
	@set -o pipefail; ${PYTHON} downloaders/get_heal_cde_taxonomy.py "$@.tmp" \
		--input-csv $(HEAL_CDE_LIST_CSV) \
		--taxonomy-url "https://www.nih.gov/taxonomy/term/971" \
		--default-category "Core" 2>&1 | tee -a $(LOGFILE) && mv "$@.tmp" $@

# STEP II.3: Backstop supplemental CDEs from the supplemental taxonomy listing
$(HEAL_CDE_FINAL_CSV): downloaders/get_heal_cde_taxonomy.py $(HEAL_CDE_CORE_CSV) | $(LOGFILE)
	@set -o pipefail; ${PYTHON} downloaders/get_heal_cde_taxonomy.py "$@.tmp" \
		--taxonomy-url "https://www.nih.gov/taxonomy/term/1441" \
		--input-csv $(HEAL_CDE_CORE_CSV) 2>&1 | tee -a $(LOGFILE) && mv "$@.tmp" $@

# STAGE III. DOWNLOAD

# STEP III.1: Download data dictionaries
$(OUTPUT_DIR)/download_done: downloaders/heal_cde_repo_downloader.py $(HEAL_CDE_FINAL_CSV) $(MAPPINGS_DIR)/heal-data-dictionaries-mappings/heal-data-dictionaries.csv $(MAPPINGS_DIR)/heal-data-dictionaries-mappings/heal-cde-mappings.csv $(MAPPINGS_DIR)/study-crf-mappings/study-crf-mappings.csv $(MAPPINGS_DIR)/platform-mds-mappings/platform-mds-mappings.csv | $(LOGFILE)
	mkdir -p $(OUTPUT_DIR)
	PYTHONPATH=. set -o pipefail; ${PYTHON} downloaders/heal_cde_repo_downloader.py $(OUTPUT_DIR) \
		--heal-cde-csv $(HEAL_CDE_FINAL_CSV) \
		--mappings $(MAPPINGS_DIR)/heal-data-dictionaries-mappings/heal-data-dictionaries.csv \
		--mappings $(MAPPINGS_DIR)/heal-data-dictionaries-mappings/heal-cde-mappings.csv \
		--mappings $(MAPPINGS_DIR)/study-crf-mappings/study-crf-mappings.csv \
		--mappings $(MAPPINGS_DIR)/platform-mds-mappings/platform-mds-mappings.csv \
		--cde-corrections $(MAPPINGS_DIR)/heal-crf-ids/heal-cde-corrections.csv 2>&1 | tee -a $(LOGFILE) && touch $@

# STAGE IV. REPORTING

$(OUTPUT_DIR)/logs/warnings.txt: $(LOGFILE) $(OUTPUT_DIR)/download_done
	@grep -i "warning" $< > $@ || true
	@echo "- Found $$(wc -l < $@) warnings during make."

$(OUTPUT_DIR)/logs/errors.txt: $(LOGFILE) $(OUTPUT_DIR)/download_done
	@grep -i "error" $< > $@ || true
	@echo "- Found $$(wc -l < $@) errors during make."
