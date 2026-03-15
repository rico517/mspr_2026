#!/usr/bin/env bash

# This script clean both national and municipal data and insert it into the database
# For each part the script follow this steps : 
# - Move into the folder
# - Create and activate a virtual environnment
# - Install dependencies
# - Run the code at the entry point

NATIONAL_REPO="./national_treatment"
MUNICIPAL_REPO="./municipal_treatment"

echo "Starting data cleaning process..."

# region municipal data treatment
echo
echo "Cleaning municipal data..."
if [ -d "$MUNICIPAL_REPO" ]; then
    cd "$MUNICIPAL_REPO"
else
    echo "Invalid path for municipal data treatment : {$MUNICIPAL_REPO}"
    exit
fi

# Define PYTHONPATH to be able to import modules from the root of the project
export PYTHONPATH=$(pwd)

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

# Clear the database before adding new data
echo
python db/clear_db.py

echo
python clean_municipal_files.py

deactivate

echo
echo "Municipal data cleaned and inserted into db with success"
#endregion

# Move back to the root of the project (not the groot)
cd ..
echo
echo =================================================================

# region national data treatment
echo
echo "Cleaning national data..."
if [ -d "$NATIONAL_REPO" ]; then
    cd "$NATIONAL_REPO"
else
pwd
    echo "Invalid path for national data treatment : {$NATIONAL_REPO}"
    exit
fi

# Define PYTHONPATH to be able to import modules from the root of the project
export PYTHONPATH=$(pwd)

if [ ! -d ".venv" ]; then
    echo "Creating .venv..."
    python -m venv .venv
    echo ".venv created"
fi
source .venv/Scripts/activate

echo
echo "Installing requirements..."
python -m pip install jupyter nbconvert ipykernel pyzmq
python -m pip install -r requirements.txt
echo "Installation done"

echo

# Wip : For now the national cleaning process fail.
# Need to fix it, seems to be SQL related.
# Note that I choosed to clean municipal first because I had a database cleaning script in it.

echo "Starting data cleaning process..."
python -mjupyter nbconvert --to notebook --execute --inplace traitement.ipynb
echo "Process done"

deactivate

echo "National data cleaning process done"
#endregion