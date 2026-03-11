import os
import pandas as pd

data_path = "./data"
data_2014_1_path = f"{data_path}/2014/1"
data_2020_1_path = f"{data_path}/2020/1"

def get_candidate_columns(df):
    """
    Return columns that look like candidate names (heuristic: not standard columns).
    """
    standard_cols = [
        'ID_BVOTE', 'SCRUTIN', 'ANNEE', 'TOUR', 'DATE', 'NUM_CIRC', 'NUM_QUARTIER',
        'NUM_ARROND', 'NUM_BUREAU', 'NB_PROCU', 'NB_INSCR', 'NB_EMARG', 'NB_VOTANT',
        'NB_BL_NUL', 'NB_NUL', 'NB_BLANC', 'NB_EXPRIM'
    ]

    # Clean candidate names in the columns (remove "M. " and "Mme " prefixes)
    cleaned_cols = {}
    for col in df.columns:
        col_clean = col
        if col.startswith("M. "):
            col_clean = col[3:]
        elif col.startswith("Mme "):
            col_clean = col[4:]
        cleaned_cols[col] = col_clean

    df = df.rename(columns=cleaned_cols)

    return [col for col in df.columns if col not in standard_cols]

def extract_candidates_from_folder(folder_path):
    candidates = set()
    for file in os.listdir(folder_path):
        if file.endswith(".xls") or file.endswith(".xlsx"):
            df = pd.read_excel(os.path.join(folder_path, file))
            candidate_cols = get_candidate_columns(df)
            candidates.update(candidate_cols)
    return candidates

def main():
    print("Extracting candidates from all rounds...")
    candidates = set()
    folders = [
        f"{data_path}/2014/1",
        f"{data_path}/2014/2",
        f"{data_path}/2020/1",
        f"{data_path}/2020/2"
    ]
    for folder in folders:
        if os.path.exists(folder):
            candidates.update(extract_candidates_from_folder(folder))
    all_candidates = sorted(candidates)
    print(f"Total unique candidates : {len(all_candidates)}")
    with open("list_candidates.txt", "w", encoding="utf-8") as f:
        for name in all_candidates:
            f.write(str(name) + "\n")
    print("List of candidates exported to list_candidates.txt")

if __name__ == "__main__":
    main()
