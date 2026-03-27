import time
from typing import Dict, List, Tuple

import mysql.connector
import numpy as np
import pandas as pd
from mysql.connector import Error
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

try:
    from xgboost import XGBClassifier

    HAS_XGBOOST = True
except Exception:
    HAS_XGBOOST = False


MAX_RETRIES = 5
RETRY_DELAY = 10


def connect_to_database(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    retries: int = MAX_RETRIES,
):
    for attempt in range(retries):
        try:
            print(f"Tentative de connexion a {database} ({attempt + 1}/{retries})...")
            conn = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                connect_timeout=30,
                auth_plugin="mysql_native_password",
            )
            print(f"Connexion a {database} etablie avec succes")
            return conn
        except Error as exc:
            print(f"Erreur de connexion a {database}: {exc}")
            if attempt < retries - 1:
                print(f"Nouvelle tentative dans {RETRY_DELAY} secondes...")
                time.sleep(RETRY_DELAY)
            else:
                raise


def create_onehot_encoder() -> OneHotEncoder:
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def load_votes(conn) -> pd.DataFrame:
    query = """
    SELECT
        bp.label AS bord_politique,
        s.tour,
        s.annee,
        s.type AS scrutin_type,
        c.code AS code_circo,
        v.voix,
        sc.exprimes
    FROM votes v
    JOIN scrutins_circonscriptions sc
        ON sc.id = v.id_scrutin_circonscription
    JOIN candidats cand
        ON cand.id = v.id_candidat
    JOIN bords_politiques bp
        ON bp.id = cand.id_bord_politique
    JOIN scrutins s
        ON s.id = sc.id_scrutin
    JOIN circonscriptions c
        ON c.id = sc.id_circonscription
    """
    df = pd.read_sql(query, conn)
    df["vote_share"] = (df["voix"] / df["exprimes"].replace(0, pd.NA)) * 100.0
    return df


def detect_scrutin_kind(scrutin_type: pd.Series) -> pd.Series:
    st = scrutin_type.fillna("").str.lower()
    return np.where(
        st.str.contains("president"),
        "presidential",
        np.where(st.str.contains("municip"), "municipal", "other"),
    )


def build_training_matrix(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df["scrutin_kind"] = detect_scrutin_kind(df["scrutin_type"])

    # Use first round for comparability across elections.
    df = df[df["tour"] == 1].copy()

    base_cols = [
        "annee",
        "tour",
        "code_circo",
        "bord_politique",
        "scrutin_kind",
        "vote_share",
    ]
    df = df[base_cols].dropna(subset=["vote_share"]).copy()

    pres = df[df["scrutin_kind"] == "presidential"].copy()
    muni = df[df["scrutin_kind"] == "municipal"].copy()
    if pres.empty:
        raise ValueError("Aucune ligne presidentielle disponible.")

    # Binary target: 1 if party is winner in that circonscription/year.
    pres["max_share_in_election"] = pres.groupby(
        ["annee", "tour", "code_circo"]
    )["vote_share"].transform("max")
    pres["is_winner"] = (pres["vote_share"] == pres["max_share_in_election"]).astype(int)

    # Presidential temporal features per party/circo.
    key_cols = ["tour", "code_circo", "bord_politique"]
    pres = pres.sort_values(key_cols + ["annee"]).copy()
    pres["pres_share_t_minus_1"] = pres.groupby(key_cols)["vote_share"].shift(1)
    pres["pres_share_t_minus_2"] = pres.groupby(key_cols)["vote_share"].shift(2)
    pres["pres_share_delta"] = pres["pres_share_t_minus_1"] - pres["pres_share_t_minus_2"]

    # Municipal temporal features merged as latest municipal status before presidential year.
    muni = muni.sort_values(key_cols + ["annee"]).copy()
    muni["muni_share_latest"] = muni["vote_share"]
    muni["muni_share_prev"] = muni.groupby(key_cols)["vote_share"].shift(1)
    muni["muni_share_delta"] = muni["muni_share_latest"] - muni["muni_share_prev"]

    pres = pres.sort_values(key_cols + ["annee"]).copy()
    muni_merge = muni[key_cols + ["annee", "muni_share_latest", "muni_share_delta"]].copy()

    merged = pd.merge_asof(
        pres.sort_values("annee"),
        muni_merge.sort_values("annee"),
        on="annee",
        by=key_cols,
        direction="backward",
        allow_exact_matches=False,
    )

    # Last known election type and year gap add simple temporal context.
    merged["years_since_prev_pres"] = merged["annee"] - merged.groupby(key_cols)["annee"].shift(1)

    return merged, pres


def build_splits(df: pd.DataFrame, min_train_years: int = 1) -> List[Dict]:
    years = sorted(df["annee"].unique())
    if len(years) < 2:
        raise ValueError("Pas assez d'annees presidentielles pour evaluer.")

    splits = []
    for idx in range(min_train_years, len(years)):
        train_years = years[:idx]
        test_year = years[idx]
        train_df = df[df["annee"].isin(train_years)].copy()
        test_df = df[df["annee"] == test_year].copy()
        if train_df.empty or test_df.empty:
            continue
        splits.append(
            {
                "train_years": train_years,
                "test_year": int(test_year),
                "train_df": train_df,
                "test_df": test_df,
            }
        )

    if not splits:
        raise ValueError("Aucun split valide.")
    return splits


def evaluate_ranking(df_test: pd.DataFrame, proba: np.ndarray) -> Dict[str, float]:
    eval_df = df_test[["annee", "tour", "code_circo", "bord_politique", "is_winner"]].copy()
    eval_df["proba_win"] = proba

    group_cols = ["annee", "tour", "code_circo"]
    top1 = eval_df.loc[eval_df.groupby(group_cols)["proba_win"].idxmax()]
    top1_accuracy = float(top1["is_winner"].mean())

    def winner_in_top_k(group: pd.DataFrame, k: int = 3) -> float:
        g = group.sort_values("proba_win", ascending=False)
        return float(g.head(k)["is_winner"].max())

    top3_accuracy = float(eval_df.groupby(group_cols).apply(winner_in_top_k, k=3).mean())

    return {
        "Top1Accuracy": top1_accuracy,
        "Top3Accuracy": top3_accuracy,
    }


def main() -> None:
    db_config = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "elections_db",
    }

    conn = connect_to_database(**db_config)
    try:
        raw = load_votes(conn)
    finally:
        conn.close()

    model_df, pres_df = build_training_matrix(raw)

    numeric_features = [
        "vote_share",
        "pres_share_t_minus_1",
        "pres_share_delta",
        "muni_share_latest",
        "muni_share_delta",
        "years_since_prev_pres",
    ]
    categorical_features = ["code_circo", "bord_politique", "tour"]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(steps=[("imputer", SimpleImputer(strategy="median"))]),
                numeric_features,
            ),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", create_onehot_encoder()),
                    ]
                ),
                categorical_features,
            ),
        ]
    )

    models = {
        "logistic_balanced": LogisticRegression(max_iter=4000, class_weight="balanced"),
        "random_forest_balanced": RandomForestClassifier(
            n_estimators=500,
            max_depth=14,
            min_samples_leaf=2,
            class_weight="balanced_subsample",
            random_state=42,
            n_jobs=-1,
        ),
    }

    if HAS_XGBOOST:
        models["xgboost"] = XGBClassifier(
            n_estimators=500,
            learning_rate=0.03,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="binary:logistic",
            eval_metric="logloss",
            random_state=42,
            n_jobs=-1,
        )

    splits = build_splits(model_df, min_train_years=1)

    print("\n=== CLASSIFICATION ORIENTEE 'CHANCE DE VICTOIRE' ===")
    print("Features basees sur l'evolution des pourcentages de voix exprimees.")
    print(f"Annees presidentielles: {sorted(model_df['annee'].unique())}")
    for split in splits:
        print(
            f"  - Train {split['train_years']} -> Test {[split['test_year']]}"
        )

    results = []
    for model_name, estimator in models.items():
        split_metrics = []
        for split in splits:
            train_df = split["train_df"]
            test_df = split["test_df"]

            X_train = train_df[numeric_features + categorical_features]
            y_train = train_df["is_winner"].astype(int)
            X_test = test_df[numeric_features + categorical_features]

            pipeline = Pipeline(
                steps=[
                    ("preprocessor", preprocessor),
                    ("model", clone(estimator)),
                ]
            )
            pipeline.fit(X_train, y_train)

            proba = pipeline.predict_proba(X_test)[:, 1]
            met = evaluate_ranking(test_df, proba)
            split_metrics.append(met)

        top1_mean = float(np.mean([m["Top1Accuracy"] for m in split_metrics]))
        top3_mean = float(np.mean([m["Top3Accuracy"] for m in split_metrics]))
        top1_std = float(np.std([m["Top1Accuracy"] for m in split_metrics]))

        results.append(
            {
                "model": model_name,
                "Top1Accuracy_mean": top1_mean,
                "Top1Accuracy_std": top1_std,
                "Top3Accuracy_mean": top3_mean,
            }
        )

    results_df = pd.DataFrame(results).sort_values(
        ["Top1Accuracy_mean", "Top3Accuracy_mean", "Top1Accuracy_std"],
        ascending=[False, False, True],
    )
    results_df.insert(0, "rank", range(1, len(results_df) + 1))
    print("\n=== CLASSEMENT MODELES (winner par circonscription) ===")
    print(results_df.to_string(index=False, float_format=lambda v: f"{v:.3f}"))
    results_df.to_csv("party_chance_model_comparison.csv", index=False)
    print("Resultats exportes: party_chance_model_comparison.csv")

    # Forecast next presidential winner chances per circonscription.
    best_model_name = str(results_df.iloc[0]["model"])
    best_estimator = clone(models[best_model_name])
    final_pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", best_estimator),
        ]
    )
    final_pipeline.fit(
        model_df[numeric_features + categorical_features],
        model_df["is_winner"].astype(int),
    )

    latest_year = int(model_df["annee"].max())
    latest_rows = model_df[model_df["annee"] == latest_year].copy()

    # Reuse latest observed party/circo state as proxy input for next election.
    future = latest_rows.copy()
    future["annee"] = latest_year + 5
    future["years_since_prev_pres"] = 5

    proba_next = final_pipeline.predict_proba(future[numeric_features + categorical_features])[:, 1]
    future_out = future[["annee", "tour", "code_circo", "bord_politique"]].copy()
    future_out["chance_win"] = proba_next
    future_out["best_model"] = best_model_name

    # Pick the most likely winner by circonscription.
    winner_idx = future_out.groupby(["annee", "tour", "code_circo"])["chance_win"].idxmax()
    winner_forecast = future_out.loc[winner_idx].sort_values(["tour", "code_circo"])

    future_out.to_csv("next_presidential_party_chance_all_parties.csv", index=False)
    winner_forecast.to_csv("next_presidential_winner_forecast.csv", index=False)
    print("Export detail probabilites: next_presidential_party_chance_all_parties.csv")
    print("Export gagnant predit par circonscription: next_presidential_winner_forecast.csv")

    print("\nApercu gagnants predicts (10 lignes):")
    print(winner_forecast.head(10).to_string(index=False, float_format=lambda v: f"{v:.3f}"))


if __name__ == "__main__":
    main()
