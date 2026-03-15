# mspr_2026

## Project Overview

This project processes and analyzes French election data at both municipal and national levels. It includes scripts for data cleaning, transformation, and database integration.

## How to Use the Project

### 1. Main Entry Point

- The main entry point for cleaning and preparing all data is the `clean_all_data.sh` script located at the root of the repository.
- Run this script from the terminal:

```bash
bash clean_all_data.sh
```

This will execute all necessary cleaning steps for both municipal and national datasets.

### 2. Database Connection Configuration

#### Municipal Treatment

- The database connection parameters are set in the file:
	- `municipal_treatment/db/db_cnx.py`
- You can modify the following variables to match your MySQL setup:
	- `DB_HOST`
	- `DB_PORT`
	- `DB_USER`
	- `DB_PASSWORD`
	- `DATABASE`

#### National Treatment

- The database connection parameters are set directly in the Jupyter notebook:
	- `national_treatment/traitement.ipynb`
- Update the following variables in the notebook to match your MySQL setup:
	- `db_host`
	- `db_port`
	- `db_user`
	- `db_password`
	- `database`