# README - Comparaison des modeles de prediction

## 1. Objectif

Le script [compare_party_models.py](compare_party_models.py) compare 3 modeles de classification pour estimer si un parti gagne dans une circonscription parisienne:

- Logistic Regression
- Random Forest
- XGBoost

La cible predite est `winning_party` avec 2 classes:

- `Won`
- `Lost`

## 2. Source de donnees (BDD)

Les donnees viennent de MySQL (`elections_db`) avec jointure sur:

- `votes`
- `candidats`
- `bords_politiques`
- `scrutins_circonscriptions`
- `scrutins`
- `circonscriptions`

Colonnes chargees:

- `election_type`
- `election_year`
- `election_round`
- `district_code`
- `party_label`
- `votes`
- `inscrits`
- `votants`
- `exprimes`

## 3. Logique de preparation des donnees

## 3.1 Tous les tours sont utilises

Le script n'ignore pas les tours:

- tour 1
- tour 2
- et tout autre tour present dans la base

## 3.2 Agregation par parti et par tour

Agregation sur:

- `election_type`
- `election_year`
- `election_round`
- `district_code`
- `party_label`

Calculs:

- `votes` (somme)
- `inscrits` (max)
- `votants` (max)
- `exprimes` (max)
- `vote_share = votes / exprimes`
- `turnout = votants / inscrits`

## 3.3 Elections de reference

Le script prend automatiquement:

- les 2 dernieres annees disponibles pour `national_type` (defaut: `Presidentielle`)
- les 2 dernieres annees disponibles pour `municipal_type` (defaut: `Municipales`)

Features utilisees:

- nationale annee N-1 (tous tours)
- nationale annee N (tous tours)
- municipale annee N-1 (tous tours)

Cible:

- municipale annee N

## 3.4 Construction de la cible

Le gagnant de chaque circonscription est determine sur le dernier tour de l'election cible (logique electorale), puis chaque ligne parti/circonscription est etiquetee:

- `Won` si c'est le parti gagnant
- `Lost` sinon

Important:

- les features utilisent tous les tours des elections historiques
- la definition du vainqueur reste basee sur le tour final de l'election cible

## 4. Variables/features du modele

Le DataFrame final contient notamment:

- `district_code`
- `party_label`
- des colonnes de parts de voix par election et par tour, par exemple:
  - `nat_share_year_1_tour_1`
  - `nat_share_year_1_tour_2`
  - `mun_share_year_1_tour_1`
- des colonnes de participation par election et par tour, par exemple:
  - `taux_participation_Presidentielle_2017_tour_1`
  - `taux_participation_Municipales_2014_tour_2`

Les valeurs manquantes apres jointure sont remplacees par `0.0`.

## 5. Preprocessing ML

Pipeline scikit-learn:

- variables numeriques -> `StandardScaler`
- variables categorielles (`party_label`) -> `OneHotEncoder(handle_unknown="ignore")`

Ce preprocessing est integre dans chaque pipeline modele.

## 6. Modeles compares

- Logistic Regression (`max_iter=2000`)
- Random Forest (`n_estimators=400`, `class_weight="balanced"`)
- XGBoost (`n_estimators=350`)

## 7. Evaluation

2 modes:

1. `--test-size 0` (defaut)
   - utilisation de toutes les donnees
   - evaluation par validation croisee stratified K-fold (`--cv-folds`, defaut 5)
   - predictions out-of-fold avec `cross_val_predict`

2. `--test-size > 0`
   - split train/test (`train_test_split` stratifie)
   - entrainement sur train et evaluation sur test

## 8. Sorties affichees

- tableau comparatif final:
  - `Accuracy`
  - `F1-Score (weighted)`
- matrices de confusion pour chaque modele
- ordre des classes utilise pour les matrices

## 9. Parametres CLI

- `--target-col` (defaut: `winning_party`)
- `--test-size` (defaut: `0.0`)
- `--random-state` (defaut: `42`)
- `--cv-folds` (defaut: `5`)
- `--national-type` (defaut: `Presidentielle`)
- `--municipal-type` (defaut: `Municipales`)

## 10. Execution

Depuis la racine du projet:

```bash
c:/Users/julie/Desktop/Projets/mspr_2026/.venv/Scripts/python.exe compare_party_models.py
```

Exemple en split train/test:

```bash
c:/Users/julie/Desktop/Projets/mspr_2026/.venv/Scripts/python.exe compare_party_models.py --test-size 0.2
```

## 11. Interpretation des resultats

- Un score tres eleve en evaluation sur train peut etre trompeur.
- En mode par defaut (`test_size=0`), l'evaluation est faite en out-of-fold CV, donc plus fiable.
- Avec un petit volume de donnees, les scores peuvent varier selon les folds.

## 12. Limites et pistes d'amelioration

Limites:

- volume de donnees encore limite
- cible binaire (`Won` / `Lost`) au lieu d'une prediction multiclasses directe du gagnant
- pas de recherche d'hyperparametres

Ameliorations possibles:

- repeated stratified CV
- rapport precision/recall par classe
- calibration des probabilites
- validation temporelle stricte (train sur elections anciennes, test sur la plus recente)
