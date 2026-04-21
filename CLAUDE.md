# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

`heal-cdes` is a data pipeline that downloads HEAL (Helping to End Addiction Long-term) Common Data Elements (CDEs) from the NIH CDE Repository, converts them to JSON, maps them to HEAL Data Platform (HDP) studies and CRFs, and outputs Knowledge Graph Exchange (KGX) format files for use in HEAL Semantic Search (HSS).

## Commands

**Package manager:** `uv` (not pip). Run scripts with `uv run python <script>`.

**Full pipeline:**
```bash
make all      # Run entire pipeline; output goes to ./output-2026apr20
make clean    # Remove generated outputs before re-running
```

The `OUTPUT_DIR` in the `Makefile` must be updated when starting a new run (it's date-stamped).

**Run individual steps manually:**
```bash
# Step 1: Generate study/CRF mappings from a HEAL CDE team REDCap export
uv run python study-mappings/extract-study-mappings-from-heal-cde-team-export.py <export.csv> \
    --study-to-hdpid mappings/study-crf-mappings/from-heal-cde-team/study-hdp-ids.csv \
    --measure-to-heal-cde-id mappings/study-crf-mappings/from-heal-cde-team/crf-heal-cde-ids.csv \
    --output <output.csv>

# Step 2: Download CDEs and generate KGX output
uv run python downloaders/heal_cde_repo_downloader.py <output_dir> \
    --mappings mappings/heal-data-dictionaries-mappings/heal-data-dictionaries.csv \
    --mappings mappings/study-crf-mappings/study-crf-mappings.csv \
    --mappings mappings/platform-mds-mappings/platform-mds-mappings.csv \
    --cde-corrections mappings/heal-crf-ids/heal-cde-corrections.csv

# Validate output
uv run python validators/check_json.py <output_dir>
```

**No automated test framework** — validation is done via scripts in `validators/`.

## Architecture

### Pipeline Stages

```
Mapping Sources (4 types)
    ↓ [mappers/, study-mappings/]
CSV mapping files in mappings/
    ↓ [downloaders/heal_cde_repo_downloader.py]
JSON CDEs per CRF (NIH CDE Repository schema)
    ↓ [summarizers/finalize-json.py]
KGX JSONL output (nodes + edges per CRF)
```

### Key Directories

- **`downloaders/`** — Main orchestrator (`heal_cde_repo_downloader.py`) and `excel2cde.py` (converts HEAL CDE Excel → JSON following NIH CDE schema).
- **`mappers/`** — Reads HEAL Data Dictionaries and HEAL CDE Mappings repos (sibling directories `../heal-data-dictionaries/` and `../heal-cde-mappings/`) to produce CSV mapping files.
- **`study-mappings/`** — Extracts study↔CRF associations from HEAL CDE team REDCap exports and HEAL Platform MDS API.
- **`summarizers/`** — `finalize-json.py` merges annotations with study/CRF/variable mappings to produce final KGX output.
- **`exporters/`** — `xlsx-exporter.py` converts JSON CDEs back to Excel templates.
- **`validators/`** — Validation scripts for JSON and KGX output.
- **`mappings/`** — Input and generated CSV mapping files; `mappings/heal-crf-ids/heal-crf-ids.csv` is the master list of CRF IDs.
- **`schemas/`** — NIH CDE Repository JSON schema (`form.schema.fixed.json` is the patched version used for validation).

### Key Data Structures

- **JSON CDEs**: Follow NIH CDE Repository schema. Each CRF produces one JSON file per form element. Contains `designations`, `definitions`, `formElements` (questions), and `valueDomain` (permissible values).
- **KGX nodes/edges**: Biolink-compliant graph. CDEs become `biolink:Publication` nodes; studies become `biolink:Study` nodes. Edges use predicates like `biolink:part_of`.
- **Mapping CSVs**: Link HDP IDs → HEAL CRF IDs → variable names → CDE IDs.

### External Dependencies

- **`../heal-data-dictionaries/`** — Sibling repo with HEAL data dictionary Excel files.
- **`../heal-cde-mappings/`** — Sibling repo with manual CDE mappings.
- **NIH CDE Repository API** — Downloaded live during `make all`.
- **HEAL Platform MDS API** — Queried by `download-study-mappings-from-platform-mds.py`.

### Logs

`make all` writes logs to `$(OUTPUT_DIR)/logs/log.txt`; errors and warnings are extracted to separate files automatically.
