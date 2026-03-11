
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

1. **Create a virtual environment**
	```sh
	python -m venv venv
	```

2. **Activate the virtual environment**
	- On Windows:
	  ```sh
	  venv\Scripts\activate
	  ```
	- On Mac/Linux:
	  ```sh
	  source venv/bin/activate
	  ```

3. **Install dependencies**
	```sh
	pip install -r requirements.txt
	```

4. **Configure database connection**
	- Edit the file `db/db_cnx.py` to set your MySQL host, user, password, and database name if needed.

## Running the Data Processing

Run the main script to clean the data, generate the CSV, and populate the database:

```sh
python clean_municipal_files.py
```

- The cleaned file will be generated at `output/cleaned_municipal_data.csv`.
- Data will be inserted into the configured MySQL database.
