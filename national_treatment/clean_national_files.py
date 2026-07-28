"""
Input:
CSV files containing national (Presidentielle) first-round and second-round
results for Paris, for the years 2017 and 2022 - one file per year/round.
Each file has one row per bureau de vote, with a repeated block of columns
per candidate (Nom, Prenom, Voix, then Nom2, Prenom2, Voix2, ...).

Output:
A single cleaned dataset with the same schema as
municipal_treatment.clean_municipal_files (see common.schema.OUTPUT_COLS):
- NOM, PRENOM, BORD_POL, ANNEE, TOUR, NUM_CIRC,
  NB_INSCR, NB_VOTANT, NB_EXPRIM, NB_BL_NUL, NB_VOIX

Each row represents one candidate in one year, one round, and one circonscription.
"""

import re
from pathlib import Path

import pandas as pd

from common.db import connect_to_database, export_dataset_to_db
from common.export import export_dataset_to_csv
from common.logging import debug_print
from common.schema import OUTPUT_COLS, STATS_COLS, merge_blank_null

from .party_map import NAME_FIXES, party_map

# ---------------------------------------------------------------------------

SCRUTIN_TYPE = "Presidentielle"

BASE_DIR    = Path(__file__).resolve().parent
data_path   = BASE_DIR / "data"
output_path = BASE_DIR / "output"

# Only Paris results are kept, to match municipal_treatment's scope.
DEPARTMENT_FILTER = "Paris"

# Matches the repeated per-candidate columns: Nom/Prenom/Voix, then
# Nom2/Prenom2/Voix2, Nom3/Prenom3/Voix3, ...
CANDIDATE_FIELD_RE = re.compile(r"^(Nom|Prenom|Voix)(\d*)$")

RENAME_COLS = {
    "Code de la circonscription": "NUM_CIRC",
    "Inscrits": "NB_INSCR",
    "Votants": "NB_VOTANT",
    "Exprimes": "NB_EXPRIM",
}

# ---------------------------------------------------------------------------

def process_all_data():
    """
    Load, clean and reshape all election files from all years and rounds into
    a single long-form DataFrame (one row per candidate × circonscription).
    """
    frames = []
    for year in (2017, 2022):
        for turn in (1, 2):
            path = data_path / str(year) / str(turn)
            if not path.is_dir():
                continue
            debug_print(f"\nProcessing {year} – tour {turn} ({path})", level=1)
            df = load_and_reshape_path(path, annee=year, tour=turn)
            if not df.empty:
                frames.append(df)

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLS)

    result = pd.concat(frames, ignore_index=True)
    return result[OUTPUT_COLS]

# ---------------------------------------------------------------------------

def load_and_reshape_path(path, annee, tour):
    """
    Read every csv file in *path*, reshape each one, and concatenate.
    """
    frames = []
    for file in sorted(p.name for p in path.iterdir()):
        if file.endswith(".csv"):
            debug_print(f"  Reading {file}", level=2)
            raw_df = pd.read_csv(path / file, sep=";", decimal=",", low_memory=False)
            reshaped = reshape_file(raw_df, annee=annee, tour=tour)
            frames.append(reshaped)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def reshape_file(df, annee, tour):
    """
    Transform a raw national election file into the long-form target structure.

    Steps
    -----
    1. Keep only the target department (Paris)
    2. Merge Blancs + Nuls -> NB_BL_NUL, rename context/stats columns
    3. Detect the repeated per-candidate column groups (Nom, Prenom, Voix; Nom2, ...)
    4. Aggregate rows by NUM_CIRC - sum stats and vote counts, so that the many
       bureau-de-vote rows collapse to one per circonscription
    5. Reshape the candidate slots to long format (one row per candidate × circ)
    6. Map NOM -> BORD_POL and attach ANNEE/TOUR
    """
    df = df.copy()
    df = df[df["Libelle du departement"].str.contains(DEPARTMENT_FILTER, case=False, na=False)]

    df = df.rename(columns=RENAME_COLS)
    df = merge_blank_null(df, blanc_col="Blancs", nul_col="Nuls")

    slots = _detect_candidate_slots(df.columns)

    agg = {col: "sum" for col in STATS_COLS}
    for cols in slots.values():
        agg[cols["voix"]] = "sum"
        agg[cols["nom"]] = "first"
        agg[cols["prenom"]] = "first"
    grouped = df.groupby("NUM_CIRC", as_index=False).agg(agg)

    frames = []
    for cols in slots.values():
        subset = grouped[["NUM_CIRC"] + STATS_COLS + [cols["nom"], cols["prenom"], cols["voix"]]]
        subset = subset.rename(columns={
            cols["nom"]: "NOM", cols["prenom"]: "PRENOM", cols["voix"]: "NB_VOIX",
        })
        frames.append(subset)
    long_df = pd.concat(frames, ignore_index=True)

    long_df["NOM"] = long_df["NOM"].replace(NAME_FIXES)
    long_df["BORD_POL"] = long_df["NOM"].map(party_map)
    long_df["ANNEE"] = annee
    long_df["TOUR"] = tour

    return long_df

# ---------------------------------------------------------------------------

def _detect_candidate_slots(columns):
    """
    Group repeated per-candidate columns (Nom, Prenom, Voix, then Nom2, Prenom2,
    Voix2, ...) by their numeric suffix.

    Returns {suffix: {"nom": col, "prenom": col, "voix": col}}.
    """
    slots = {}
    for col in columns:
        match = CANDIDATE_FIELD_RE.match(col)
        if not match:
            continue
        field, suffix = match.group(1), match.group(2)
        slots.setdefault(suffix, {})[field.lower()] = col
    return {
        suffix: cols for suffix, cols in slots.items()
        if {"nom", "prenom", "voix"} <= cols.keys()
    }

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    debug_print("Starting data cleaning process...", level=1)

    cnx = connect_to_database()

    # Run `python -m common.reset_db` first to start from an empty database -
    # clearing here would also wipe out municipal_treatment's data.

    debug_print("\nProcessing election files...", level=1)
    cleaned_df = process_all_data()

    debug_print(f"\nFinal dataset: {len(cleaned_df)} rows, {len(cleaned_df.columns)} columns", level=1)
    debug_print(f"Columns: {list(cleaned_df.columns)}", level=1)

    # export_dataset_to_csv(cleaned_df, "cleaned_national_data.csv", output_path)

    export_dataset_to_db(cleaned_df, SCRUTIN_TYPE, cnx)

    if cnx is not None:
        cnx.close()

    debug_print("\nData cleaning process completed.", level=1)
