# mspr_2026

## Project Overview

This project processes and analyzes French election data at both municipal and national levels. It includes scripts for data cleaning, transformation, and database integration.

## Project Structure

- `common/` - shared code used by both pipelines: database connection & insertion (`db.py`), the long-form output schema (`schema.py`), CSV export helpers (`export.py`), and debug logging (`logging.py`).
- `municipal_treatment/` - cleans municipal election results (2014, 2020) from raw Excel files.
- `national_treatment/` - cleans national (Presidentielle) election results for Paris (2017, 2022) from raw CSV files.
- `compare_party_models.py` - trains and compares classification models on the data stored in MySQL (see [README_compare_party_models.md](README_compare_party_models.md)).
- `script_bdd_election_mspr.sql` - creates the MySQL schema used by every pipeline.

Both cleaning pipelines produce the same long-form schema (`NOM, PRENOM, BORD_POL, ANNEE, TOUR, NUM_CIRC, NB_INSCR, NB_VOTANT, NB_EXPRIM, NB_BL_NUL, NB_VOIX`) and share the same `common.db.export_dataset_to_db` / `common.db.clear_database` functions - only the scrutin type ("Municipales" vs "Presidentielle") differs.

## Setup

1. Create and activate a single virtual environment at the root of the project:
   ```sh
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # Mac/Linux
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Start MySQL (and phpMyAdmin) with Docker:
   ```sh
   docker compose up -d
   ```
4. Load the database schema:
   ```sh
   mysql -h 127.0.0.1 -P 3306 -u root -p < script_bdd_election_mspr.sql
   ```

## How to Use the Project

### Main entry point

Run every cleaning step (national + municipal) from the root of the repository:

```sh
bash clean_all_data.sh
```

This resets the database first (`python -m common.reset_db`), then runs both pipelines, so each run starts from a clean slate - the `votes` table has no uniqueness constraint, so re-inserting into a non-empty database creates duplicate rows.

Or run the individual steps as modules, from the repository root (required so that `common` resolves):

```sh
python -m common.reset_db                        # wipe every election table
python -m national_treatment.clean_national_files
python -m municipal_treatment.clean_municipal_files
```

### Database connection configuration

Connection settings live in `common/db.py` and default to the docker-compose setup (`localhost:3306`, `root`/`root`, database `elections_db`). Override them with environment variables if needed:

- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
