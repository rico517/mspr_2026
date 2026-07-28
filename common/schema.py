"""
Shared long-form schema produced by both election cleaning pipelines
(municipal_treatment and national_treatment) before being handed to
common.db.export_dataset_to_db.

Each row represents one candidate, in one year, one round, one circonscription.
"""

# Columns identifying an election event and district
CONTEXT_COLS = ["ANNEE", "TOUR", "NUM_CIRC"]
# District-level vote statistics
STATS_COLS = ["NB_INSCR", "NB_VOTANT", "NB_EXPRIM", "NB_BL_NUL"]
# Final output columns, in the desired order
OUTPUT_COLS = [
    "NOM", "PRENOM", "BORD_POL", "ANNEE", "TOUR",
    "NUM_CIRC", "NB_INSCR", "NB_VOTANT", "NB_EXPRIM",
    "NB_BL_NUL", "NB_VOIX",
]


def merge_blank_null(df, blanc_col, nul_col, out_col="NB_BL_NUL"):
    """
    Merge a blank-votes column and a null-votes column into a single count,
    when both are present in the raw source file.
    """
    df = df.copy()
    if blanc_col in df.columns and nul_col in df.columns:
        df[out_col] = df[blanc_col] + df[nul_col]
        df = df.drop(columns=[blanc_col, nul_col])
    return df
