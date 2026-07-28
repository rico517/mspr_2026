
# Municipal Election Data Cleaning and Integration

## Description

This project cleans, restructures, and integrates municipal election results (2014 and 2020) into a MySQL database from raw Excel files. The main script produces a clean CSV file and populates the relational database.

## Project Structure

- `data/` : Source Excel files, organized by year and round
- `output/` : Cleaned output (`cleaned_municipal_data.csv`)
- `db/` : SQL scripts and database management modules
- `utils/` : Utility functions (export, debug, etc.)
- `candidates_map.py` : Mapping of candidates to political sides
- `clean_municipal_files.py` : Main processing script

## Requirements

- Python 3.10 or higher
- MySQL Server (local or remote)

## Setup

This module is part of the `mspr_2026` project and shares a single virtual environment and a `common/` package with `national_treatment/`. See the [root README](../README.md) for environment setup and database configuration (`common/db.py`, overridable via `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`/`DB_NAME` environment variables).

## Running the Data Processing

Run the main script from the repository root (required so that `common` resolves):

```sh
python -m municipal_treatment.clean_municipal_files
```

- The cleaned CSV export is disabled by default; uncomment the `export_dataset_to_csv(...)` call in `clean_municipal_files.py` to generate `output/cleaned_municipal_data.csv`.
- Data is inserted into the configured MySQL database via `common.db.export_dataset_to_db`.
