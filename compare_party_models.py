from __future__ import annotations

import argparse
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score
from sklearn.model_selection import train_test_split
from sklearn.model_selection import StratifiedKFold, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler

try:
    from xgboost import XGBClassifier
except ImportError as exc:
    raise ImportError(
        "xgboost is required to run this script. Install it with: pip install xgboost"
    ) from exc

from municipal_treatment.db.db_cnx import connect_to_database

RAW_SQL_QUERY = """
SELECT
    s.type AS election_type,
    s.annee AS election_year,
    s.tour AS election_round,
    ci.code AS district_code,
    bp.label AS party_label,
    v.voix AS votes,
    sc.inscrits,
    sc.votants,
    sc.exprimes
FROM votes v
JOIN candidats c ON c.id = v.id_candidat
JOIN bords_politiques bp ON bp.id = c.id_bord_politique
JOIN scrutins_circonscriptions sc ON sc.id = v.id_scrutin_circonscription
JOIN scrutins s ON s.id = sc.id_scrutin
JOIN circonscriptions ci ON ci.id = sc.id_circonscription
"""


def load_raw_data_from_database() -> pd.DataFrame:
    print("Loading election data from database.")
    cnx = connect_to_database()
    try:
        cursor = cnx.cursor()
        # Read all election rows; filtering is handled during feature/target assembly.
        cursor.execute(RAW_SQL_QUERY)
        records = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(records, columns=columns)
    finally:
        cnx.close()

    if df.empty:
        raise ValueError("No records were fetched from the database for the selected districts.")
    return df


def _get_last_two_years(df: pd.DataFrame, election_type: str) -> Tuple[int, int]:
    years = sorted(df.loc[df["election_type"] == election_type, "election_year"].unique())
    if len(years) < 2:
        raise ValueError(f"At least two years are required for election type '{election_type}'.")
    return years[-2], years[-1]


def _aggregate_all_rounds(raw_df: pd.DataFrame) -> pd.DataFrame:
    # Keep each election round as a separate signal to preserve full electoral dynamics.
    grouped = (
        raw_df.groupby(
            [
                "election_type",
                "election_year",
                "election_round",
                "district_code",
                "party_label",
            ],
            as_index=False,
        )
        .agg(
            votes=("votes", "sum"),
            inscrits=("inscrits", "max"),
            votants=("votants", "max"),
            exprimes=("exprimes", "max"),
        )
        .astype({"district_code": int, "election_round": int})
    )

    grouped["vote_share"] = np.where(
        grouped["exprimes"] > 0,
        grouped["votes"] / grouped["exprimes"],
        0.0,
    )
    grouped["turnout"] = np.where(
        grouped["inscrits"] > 0,
        grouped["votants"] / grouped["inscrits"],
        0.0,
    )
    return grouped


def prepare_dataset_from_database(
    raw_df: pd.DataFrame,
    target_election_type: str,
    secondary_election_type: str,
) -> pd.DataFrame:
    aggregated = _aggregate_all_rounds(raw_df)

    target_year_1, target_year_2 = _get_last_two_years(aggregated, target_election_type)
    secondary_year_1, secondary_year_2 = _get_last_two_years(
        aggregated, secondary_election_type
    )

    print(
        "Selected elections for features (all rounds): "
        f"{target_election_type} {target_year_1}, "
        f"{secondary_election_type} {secondary_year_1}, "
        f"{secondary_election_type} {secondary_year_2}"
    )
    print(f"Target election: {target_election_type} {target_year_2}")

    events_for_features: Dict[str, Tuple[str, int]] = {
        # Use only past information relative to the target year to avoid leakage.
        "target_share_year_1": (target_election_type, target_year_1),
        "secondary_share_year_1": (secondary_election_type, secondary_year_1),
        "secondary_share_year_2": (secondary_election_type, secondary_year_2),
    }

    target_event = aggregated[
        (aggregated["election_type"] == target_election_type)
        & (aggregated["election_year"] == target_year_2)
    ].copy()

    if target_event.empty:
        raise ValueError("No target rows found for the latest target election year.")

    target_final_round = target_event.groupby("district_code", as_index=False)[
        "election_round"
    ].max()
    # Winners are determined on the final round of the target election.
    target_for_winner = target_event.merge(
        target_final_round,
        on=["district_code", "election_round"],
        how="inner",
    )

    winner_idx = target_for_winner.groupby("district_code")["vote_share"].idxmax()
    winners = (
        target_for_winner.loc[winner_idx, ["district_code", "party_label"]]
        .rename(columns={"party_label": "winner_party"})
        .reset_index(drop=True)
    )

    base = target_event[["district_code", "party_label"]].drop_duplicates().copy()
    base = base.merge(winners, on="district_code", how="left")
    base["winning_party"] = np.where(
        base["party_label"] == base["winner_party"],
        "Won",
        "Lost",
    )

    for feature_name, (election_type, election_year) in events_for_features.items():
        subset = aggregated[
            (aggregated["election_type"] == election_type)
            & (aggregated["election_year"] == election_year)
        ]

        rounds = sorted(subset["election_round"].unique())
        for election_round in rounds:
            round_subset = subset[subset["election_round"] == election_round]

            # Party-level vote-share features.
            vote_col = f"{feature_name}_tour_{int(election_round)}"
            vote_subset = round_subset[["district_code", "party_label", "vote_share"]].rename(
                columns={"vote_share": vote_col}
            )
            base = base.merge(vote_subset, on=["district_code", "party_label"], how="left")

            # District-level turnout context for the same round.
            turnout_col = f"taux_participation_{election_type}_{election_year}_tour_{int(election_round)}"
            turnout_subset = (
                round_subset[["district_code", "turnout"]]
                .drop_duplicates(subset=["district_code"])
                .rename(columns={"turnout": turnout_col})
            )
            base = base.merge(turnout_subset, on="district_code", how="left")

    base = base.fillna(0.0)
    base = base.drop(columns=["winner_party"])
    return base


def validate_data(df: pd.DataFrame, target_col: str) -> None:
    if df.empty:
        raise ValueError("The input dataset is empty.")
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' was not found in the dataset.")
    if df[target_col].isna().all():
        raise ValueError("The target column only contains missing values.")


def build_xgboost_model(n_classes: int, random_state: int) -> XGBClassifier:
    if n_classes > 2:
        return XGBClassifier(
            objective="multi:softprob",
            num_class=n_classes,
            n_estimators=350,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.9,
            colsample_bytree=0.9,
            eval_metric="mlogloss",
            random_state=random_state,
            n_jobs=-1,
        )

    return XGBClassifier(
        objective="binary:logistic",
        n_estimators=350,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.9,
        colsample_bytree=0.9,
        eval_metric="logloss",
        random_state=random_state,
        n_jobs=-1,
    )


def train_and_compare(
    df: pd.DataFrame,
    target_col: str,
    test_size: float,
    random_state: int,
    cv_folds: int,
) -> None:
    validate_data(df, target_col)

    x = df.drop(columns=[target_col]).copy()
    y_raw = df[target_col].astype(str).fillna("Unknown")

    label_encoder = LabelEncoder()
    y = label_encoder.fit_transform(y_raw)
    class_names = label_encoder.classes_

    if len(class_names) < 2:
        raise ValueError("At least two target classes are required for classification.")

    numeric_cols = x.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = [col for col in x.columns if col not in numeric_cols]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric_cols),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_cols),
        ]
    )

    if test_size <= 0:
        # Full-data mode: evaluate with out-of-fold predictions instead of train predictions.
        class_counts = pd.Series(y).value_counts()
        max_valid_folds = int(class_counts.min())
        if max_valid_folds < 2:
            raise ValueError(
                "Cross-validation requires at least 2 rows in each target class."
            )

        effective_folds = min(cv_folds, max_valid_folds)
        print(
            f"Using stratified {effective_folds}-fold cross-validation on all rows."
        )
        cv = StratifiedKFold(
            n_splits=effective_folds,
            shuffle=True,
            random_state=random_state,
        )
    else:
        x_train, x_eval, y_train, y_eval = train_test_split(
            x,
            y,
            test_size=test_size,
            random_state=random_state,
            stratify=y,
        )

    models = {
        "Logistic Regression": LogisticRegression(max_iter=2000),
        "Random Forest": RandomForestClassifier(
            n_estimators=400,
            random_state=random_state,
            class_weight="balanced",
            n_jobs=-1,
        ),
        "XGBoost": build_xgboost_model(n_classes=len(class_names), random_state=random_state),
    }

    results = []
    confusion_matrices = {}

    for model_name, model in models.items():
        print(f"Training model: {model_name}")
        pipeline = Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                ("model", model),
            ]
        )

        if test_size <= 0:
            # Out-of-fold predictions provide a realistic estimate while using all rows.
            predictions = cross_val_predict(
                pipeline,
                x,
                y,
                cv=cv,
                n_jobs=1,
            )
            y_eval = y
        else:
            pipeline.fit(x_train, y_train)
            predictions = pipeline.predict(x_eval)

        acc = accuracy_score(y_eval, predictions)
        f1 = f1_score(y_eval, predictions, average="weighted")

        results.append(
            {
                "Model": model_name,
                "Accuracy": acc,
                "F1-Score (weighted)": f1,
            }
        )
        confusion_matrices[model_name] = confusion_matrix(
            y_eval,
            predictions,
            labels=np.arange(len(class_names)),
        )

    comparison_df = pd.DataFrame(results).sort_values(
        by=["F1-Score (weighted)", "Accuracy"], ascending=False
    )

    print("\nFinal performance comparison")
    print(comparison_df.to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    print("\nConfusion matrices")
    print(f"Class order: {list(class_names)}")
    for model_name, cm in confusion_matrices.items():
        print(f"\n{model_name}")
        print(cm)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Train and compare XGBoost, Random Forest, and Logistic Regression "
            "for party victory prediction using data stored in MySQL."
        )
    )
    parser.add_argument(
        "--target-col",
        type=str,
        default="winning_party",
        help="Name of the target column in the dataset.",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.0,
        help="Proportion of data to include in the test split. Set 0 to train on all rows.",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--cv-folds",
        type=int,
        default=5,
        help="Number of folds for stratified cross-validation when test_size is 0.",
    )
    parser.add_argument(
        "--target-election-type",
        type=str,
        default="Presidentielle",
        help="Election type used as prediction target.",
    )
    parser.add_argument(
        "--secondary-election-type",
        type=str,
        default="Municipales",
        help="Secondary election type used for additional features.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_df = load_raw_data_from_database()
    data = prepare_dataset_from_database(
        raw_df=raw_df,
        target_election_type=args.target_election_type,
        secondary_election_type=args.secondary_election_type,
    )

    print(f"Modeling dataset shape: {data.shape}")
    train_and_compare(
        df=data,
        target_col=args.target_col,
        test_size=args.test_size,
        random_state=args.random_state,
        cv_folds=args.cv_folds,
    )


if __name__ == "__main__":
    main()
