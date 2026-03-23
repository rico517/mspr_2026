"""
Input : 
xlsx files containing municipal election data for the years 2014 and 2020, with two turns each.
Also a candidates_map.py file containing a mapping of candidate names to their political sides, manually filled.

Output : 
A single cleaned dataset with the following columns:
- NOM: the last name (uppercase) of the candidate
- PRENOM: the first name of the candidate
- BORD_POL: the political side of the candidate
- ANNEE: the year of the election 
- TOUR: the turn number of the election
- NUM_CIRC: the number of the electoral district
- NB_INSCR: the number of registered voters
- NB_VOTANT: the number of voters         
- NB_EXPRIM: the number of valid votes 
- NB_BL_NUL: the number of blank/null votes 
- NB_VOIX: the number of votes for the candidate

Each row represents one candidate in one year, one turn, and one circonscription.
"""

import pandas as pd
import os
from candidates_map import candidates_map
from utils.export import export_dataset_to_csv
from utils.debug import debug_print
from db.fill_db import export_dataset_to_db
from db.clear_db import clear_database
from db.db_cnx import connect_to_database

# ---------------------------------------------------------------------------

data_path   = "./data"
output_path = "./output"

# Context columns that are present in the raw files and should be kept in the output
CONTEXT_COLS = ['ANNEE', 'TOUR', 'NUM_CIRC']
# Stats columns that are present in the raw files and should be kept in the output
STATS_COLS   = ['NB_INSCR', 'NB_VOTANT', 'NB_EXPRIM', 'NB_BL_NUL']
# Metadata columns that are present in the raw files but not needed in the output
META_COLS    = ['ID_BVOTE', 'SCRUTIN', 'DATE', 'NUM_QUARTIER',
                'NUM_ARROND', 'NUM_BUREAU', 'NB_PROCU', 'NB_EMARG']
# Final output columns, in the desired order
OUTPUT_COLS  = ['NOM', 'PRENOM', 'BORD_POL', 'ANNEE', 'TOUR',
                'NUM_CIRC', 'NB_INSCR', 'NB_VOTANT', 'NB_EXPRIM',
                'NB_BL_NUL', 'NB_VOIX']

# ---------------------------------------------------------------------------

def process_all_data():
    """
    Load, clean and reshape all election files from all years and turns into
    a single long-form DataFrame (one row per candidate × circonscription).
    """
    frames = []
    for year in ('2014', '2020'):
        for turn in (1, 2):
            path = f"{data_path}/{year}/{turn}"
            if not os.path.isdir(path):
                continue
            debug_print(f"\nProcessing {year} – tour {turn} ({path})", level=1)
            df = load_and_reshape_path(path)
            if not df.empty:
                frames.append(df)

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLS)

    result = pd.concat(frames, ignore_index=True)
    return result[OUTPUT_COLS]

# ---------------------------------------------------------------------------

def load_and_reshape_path(path):
    """
    Read every xls/xlsx file in *path*, reshape each one, and concatenate.
    """
    frames = []
    for file in sorted(os.listdir(path)):
        if file.endswith(".xls") or file.endswith(".xlsx"):
            debug_print(f"  Reading {file}", level=2)
            raw_df = pd.read_excel(f"{path}/{file}")
            reshaped = reshape_file(raw_df)
            frames.append(reshaped)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def reshape_file(df):
    """
    Transform a raw election file into the long-form target structure.

    Steps
    -----
    1. Merge NB_NUL + NB_BLANC → NB_BL_NUL  (2020 files only)
    2. Drop metadata columns that are not needed in the output
    3. Clean candidate column names  (strip «M. »/«Mme » prefix and whitespace)
    4. Aggregate rows by (ANNEE, TOUR, NUM_CIRC) – sum all numeric columns
       so that the 10 bureau-de-vote rows collapse to one per circonscription
    5. Melt candidate columns → long format (one row per candidate × circ)
    6. Derive NOM, PRENOM, BORD_POL from the candidate name
    """
    df = df.copy()

    # Harmonise blank/null column
    df = merge_nul_blanc(df)

    # Drop metadata
    df.drop(columns=[c for c in META_COLS if c in df.columns], inplace=True)

    # Clean candidate column names
    df, candidate_cols = clean_candidate_columns(df)

    # Aggregate bureaux → circonscription
    agg_cols = STATS_COLS + candidate_cols
    agg_cols = [c for c in agg_cols if c in df.columns]
    grouped = (
        df.groupby(CONTEXT_COLS, as_index=False)[agg_cols].sum()
    )

    # Melt candidates to long format
    long_df = grouped.melt(
        id_vars=CONTEXT_COLS + [c for c in STATS_COLS if c in grouped.columns],
        value_vars=[c for c in candidate_cols if c in grouped.columns],
        var_name='CANDIDAT',
        value_name='NB_VOIX',
    )

    # Derive candidate metadata
    long_df[['NOM', 'PRENOM']] = long_df['CANDIDAT'].apply(
        lambda x: pd.Series(split_candidate_name(x))
    )
    long_df['BORD_POL'] = long_df['CANDIDAT'].map(candidates_map)
    long_df.drop(columns=['CANDIDAT'], inplace=True)

    long_df = harmonize_duplicate_candidates(long_df)

    return long_df

# ---------------------------------------------------------------------------

def merge_nul_blanc(df):
    """Merge NB_NUL + NB_BLANC → NB_BL_NUL when the split form is present."""
    df = df.copy()
    if 'NB_NUL' in df.columns and 'NB_BLANC' in df.columns:
        df['NB_BL_NUL'] = df['NB_NUL'] + df['NB_BLANC']
        df.drop(columns=['NB_NUL', 'NB_BLANC'], inplace=True)
    return df


def clean_candidate_columns(df):
    """
    Strip «M. »/«Mme » prefixes and trailing whitespace from every column that
    is not a known context or stats column.

    Returns the renamed DataFrame and the list of (cleaned) candidate column names.
    """
    df = df.copy()
    known = set(CONTEXT_COLS + STATS_COLS + META_COLS + ['NB_NUL', 'NB_BLANC'])
    rename_map = {}
    candidate_cols = []

    for col in df.columns:
        if col in known:
            continue
        clean = col
        if clean.startswith("M. "):
            clean = clean[3:]
        elif clean.startswith("Mme "):
            clean = clean[4:]
        clean = clean.strip()
        rename_map[col] = clean
        candidate_cols.append(clean)

    df = df.rename(columns=rename_map)
    return df, candidate_cols

def harmonize_duplicate_candidates(df):
    """
    Some candidates appear under slightly different names across files.
    This function applies manual rules to harmonize them
    """
    df = df.copy()

    df["NOM"] = df["NOM"].replace({
        "BÜRKLI": "BURKLI",
        "BUZIN": "BUZYN",
        "TIBERI": "TIBÉRI",
    })

    return df


def split_candidate_name(full_name):
    """
    Split a full candidate name into (NOM, PRENOM).

    Convention: fully-uppercase tokens are the family name (NOM),
    mixed-case tokens are the given name (PRENOM).

    Example: "HIDALGO Anne" → ("HIDALGO", "Anne")
    """
    full_name = str(full_name).strip()
    if not full_name:
        return "", ""

    nom_parts, prenom_parts = [], []
    for word in full_name.split():
        if word.isupper():
            nom_parts.append(word)
        else:
            prenom_parts.append(word)

    return " ".join(nom_parts), " ".join(prenom_parts)

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    debug_print("Starting data cleaning process...", level=1)

    cnx = connect_to_database()

    # clear_database(cnx)

    debug_print("\nProcessing election files...", level=1)
    cleaned_df = process_all_data()

    debug_print(f"\nFinal dataset: {len(cleaned_df)} rows, {len(cleaned_df.columns)} columns", level=1)
    debug_print(f"Columns: {list(cleaned_df.columns)}", level=1)

    # export_dataset_to_csv(cleaned_df, "cleaned_municipal_data.csv", output_path)

    export_dataset_to_db(cleaned_df, cnx)

    if cnx is not None:
        cnx.close()

    debug_print("\nData cleaning process completed.", level=1)