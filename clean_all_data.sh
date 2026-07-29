#!/usr/bin/env bash

# This script cleans both national and municipal data and inserts it into the
# database. It expects a single virtual environment at the root of the project
# (see README.md) and runs each pipeline as a module from the repository root,
# so that they can both import the shared `common` package.

echo "Starting data cleaning process..."

if [ ! -d ".venv" ]; then
    echo "Creating .venv..."
    python -m venv .venv
    echo ".venv created"
fi
source .venv/Scripts/activate

echo
echo "Installing requirements..."
python -m pip install -r requirements.txt
echo "Installation done"

echo
echo "Resetting database..."
python -m common.reset_db
echo "Database reset done"

echo
echo "================================================================="

echo
echo "Cleaning national data..."
python -m national_treatment.clean_national_files
echo "National data cleaning process done"

echo
echo "================================================================="

echo
echo "Cleaning municipal data..."
python -m municipal_treatment.clean_municipal_files
echo "Municipal data cleaned and inserted into db with success"

echo
echo "================================================================"

echo
echo "Cleaning socio data..."
python -m socio_treatment.clean_socio_files
echo "Socio data cleaned and inserted into db with success"
