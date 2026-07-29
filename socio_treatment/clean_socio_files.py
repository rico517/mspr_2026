import os
from pathlib import Path
import pandas as pd
from common.db import connect_to_database, export_socio_data_to_db
from common.export import export_dataset_to_csv
from common.logging import debug_print
import re

BASE_DIR    = Path(__file__).resolve().parent
data_path   = BASE_DIR / "data"
output_path = BASE_DIR / "output"
file_name = "indic-stat-circonscriptions-legislatives-2022.xlsx"

def process_all_data():
    """
    Load, clean and reshape socio files into a single dataframe.
    """
    path = data_path / file_name
    if not path.is_file():
        debug_print(f"File not found: {path}", level=1)
        return pd.DataFrame()

    debug_print(f"\nProcessing socio file ({path})", level=1)
    df = load_and_reshape_path(path)
    if df.empty:
        debug_print("No data found in the socio file.", level=1)
        return pd.DataFrame()

    return df


# ---------------------------------------------------------------------------

def load_and_reshape_path(path):
    """
    Read xls file in data path, reshape it, and return a DataFrame.
    """
    try:
        debug_print(f"  Reading {path}", level=2)
        raw_df = pd.read_excel(path, header=5)
        reshaped_df = reshape_file(raw_df)
        return reshaped_df
    except Exception as e:
        debug_print(f"Error loading file {path}: {e}", level=1)
        return pd.DataFrame()

def reshape_file(df):
    """
    Edit the DataFrame to keep only relevant columns and reshape it as needed.
    """

    df = df.copy()

    df = df.pipe(keep_only_paris_circonscriptions)\
      .pipe(extract_useful_columns)

    return df

# ---------------------------------------------------------------------------

def keep_only_paris_circonscriptions(df):
    """
    Keep only rows corresponding to Paris circonscriptions.
    """
    df = df.copy()

    df = df[df["Nom de la circonscription"].str.contains("Paris", na=False)]

    return df

def extract_useful_columns(df):
    """
    Keep only the columns that are useful for further processing.
    """
    df = df.copy()
    streamlined_df = pd.DataFrame()

    streamlined_df["id_circonscription"] = df["Nom de la circonscription"].apply(get_circonscription_number)
    streamlined_df["population_totale"] = df["pop_légal_19"]
    streamlined_df["age_moyen"] = df["age_moyen"]
    streamlined_df["taux_emploi"] = df["actemp"]
    streamlined_df["taux_chomage"] = df["actcho"]
    streamlined_df["taux_cadres"] = df["act_cad"]
    streamlined_df["taux_ouvriers"] = df["act_ouv"]
    streamlined_df["taux_diplomes_sup"] = df["actdip_BAC5"]
    streamlined_df["taux_peu_diplomes"] = df["actdip_PEU"]
    streamlined_df["taux_pauvrete"] = df["tx_pauvrete60_diff"]
    streamlined_df["taux_proprietaires"] = df["proprio"]

    return streamlined_df

def get_circonscription_number(circonscription_name):
    """
    Extract the circonscription number from the circonscription name.
    """
    # The circonscription name pattern is like this : "Paris  - 1re circonscription"
    match = re.search(r"Paris\s*-\s*(\d+)(?:re|e|er)?\s+circonscription", circonscription_name)
    if match:
        return int(match.group(1))

    debug_print(f"Warning: Could not extract circonscription number from '{circonscription_name}'", level=1)
    return -1

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    debug_print("Starting data cleaning process...", level=1)

    cnx = connect_to_database()

    debug_print("\nProcessing socio files...", level=1)
    cleaned_df = process_all_data()

    debug_print(f"\nFinal dataset: {len(cleaned_df)} rows, {len(cleaned_df.columns)} columns", level=1)
    debug_print(f"Columns: {list(cleaned_df.columns)}", level=1)

    # export_dataset_to_csv(cleaned_df, "cleaned_socio_data.csv", output_path)
    export_socio_data_to_db(cleaned_df, cnx)

    if cnx is not None:
        cnx.close()

    debug_print("\nData cleaning process completed.", level=1)