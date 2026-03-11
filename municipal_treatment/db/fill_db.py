"""
Insert the cleaned election data into the MySQL database.

Expected DataFrame schema (from clean_municipal_files.process_all_data):
    NOM, PRENOM, BORD_POL, ANNEE, TOUR, NUM_CIRC,
    NB_INSCR, NB_VOTANT, NB_EXPRIM, NB_BL_NUL, NB_VOIX

Insertion order (respects FK dependencies):
    1. bords_politiques
    2. candidats
    3. scrutins
    4. circonscriptions
    5. scrutins_circonscriptions
    6. votes
"""

from utils.debug import debug_print

SCRUTIN_TYPE = "Municipales"

# ---------------------------------------------------------------------------

def export_dataset_to_db(df, cnx):
    """
    Insert the full long-form DataFrame into the database.
    All six tables are populated in dependency order.
    Build caches to minimize redundant SELECT queries.
    """
    if cnx is None:
        debug_print("No database connection - skipping DB insertion.", level=1)
        return

    debug_print("\nInserting data into the database...", level=1)
    cursor = cnx.cursor()

    # region bords_politiques                                      
    debug_print("  Filling bords_politiques...", level=2)
    for bord in df["BORD_POL"].dropna().unique():
        cursor.execute("SELECT id FROM bords_politiques WHERE label = %s", (bord,))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT IGNORE INTO bords_politiques (label) VALUES (%s)", (bord,)
            )
    cnx.commit()

    # Build cache: label -> id
    cursor.execute("SELECT id, label FROM bords_politiques")
    bord_id: dict = {label: bid for bid, label in cursor.fetchall()}
    # endregion

    # region candidats
    debug_print("  Filling candidats...", level=2)
    unique_candidates = (
        df[["NOM", "PRENOM", "BORD_POL"]].drop_duplicates().dropna(subset=["BORD_POL"])
    )
    for _, row in unique_candidates.iterrows():
        id_bord = bord_id.get(row["BORD_POL"])
        if id_bord is None:
            continue
        cursor.execute(
            "SELECT id FROM candidats WHERE nom = %s AND prenom = %s AND id_bord_politique = %s",
            (row["NOM"], row["PRENOM"], id_bord),
        )
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO candidats (nom, prenom, id_bord_politique) VALUES (%s, %s, %s)",
                (row["NOM"], row["PRENOM"], id_bord),
            )
    cnx.commit()

    # Build cache: (nom, prenom) -> id
    cursor.execute("SELECT id, nom, prenom FROM candidats")
    candidat_id: dict = {(nom, prenom): cid for cid, nom, prenom in cursor.fetchall()}
    # endregion

    # region scrutins
    debug_print("  Filling scrutins...", level=2)
    for _, row in df[["ANNEE", "TOUR"]].drop_duplicates().iterrows():
        cursor.execute(
            "SELECT id FROM scrutins WHERE type = %s AND annee = %s AND tour = %s",
            (SCRUTIN_TYPE, int(row["ANNEE"]), int(row["TOUR"])),
        )
        if not cursor.fetchone():
            cursor.execute(
                "INSERT IGNORE INTO scrutins (type, annee, tour) VALUES (%s, %s, %s)",
                (SCRUTIN_TYPE, int(row["ANNEE"]), int(row["TOUR"])),
            )
    cnx.commit()

    # Build cache: (annee, tour) -> id
    cursor.execute("SELECT id, annee, tour FROM scrutins WHERE type = %s", (SCRUTIN_TYPE,))
    scrutin_id: dict = {(annee, tour): sid for sid, annee, tour in cursor.fetchall()}
    # endregion

    # region circonscriptions
    debug_print("  Filling circonscriptions...", level=2)
    for circ in df["NUM_CIRC"].unique():
        cursor.execute("SELECT id FROM circonscriptions WHERE code = %s", (int(circ),))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT IGNORE INTO circonscriptions (code) VALUES (%s)", (int(circ),)
            )
    cnx.commit()

    # Build cache: code -> id
    cursor.execute("SELECT id, code FROM circonscriptions")
    circ_id: dict = {code: cid for cid, code in cursor.fetchall()}
    # endregion

    # region scrutins_circonscriptions
    debug_print("  Filling scrutins_circonscriptions...", level=2)
    circ_stats = df.drop_duplicates(subset=["ANNEE", "TOUR", "NUM_CIRC"])

    for _, row in circ_stats.iterrows():
        id_s = scrutin_id.get((int(row["ANNEE"]), int(row["TOUR"])))
        id_c = circ_id.get(int(row["NUM_CIRC"]))
        if id_s is None or id_c is None:
            continue
        cursor.execute(
            """
            INSERT IGNORE INTO scrutins_circonscriptions
                (id_scrutin, id_circonscription,
                 inscrits, abstentions, votants, exprimes, blancs_nuls)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                id_s,
                id_c,
                int(row["NB_INSCR"]),
                int(row["NB_INSCR"]) - int(row["NB_VOTANT"]),  # abstentions
                int(row["NB_VOTANT"]),
                int(row["NB_EXPRIM"]),
                int(row["NB_BL_NUL"]),
            ),
        )
    cnx.commit()

    # Build cache: (id_scrutin, id_circ) -> id_scrutins_circ
    cursor.execute("SELECT id, id_scrutin, id_circonscription FROM scrutins_circonscriptions")
    sc_id: dict = {(id_s, id_c): scid for scid, id_s, id_c in cursor.fetchall()}
    # endregion

    # region votes
    debug_print("  Filling votes...", level=2)
    for _, row in df.iterrows():
        id_cand = candidat_id.get((row["NOM"], row["PRENOM"]))
        id_s    = scrutin_id.get((int(row["ANNEE"]), int(row["TOUR"])))
        id_c    = circ_id.get(int(row["NUM_CIRC"]))

        if id_cand is None or id_s is None or id_c is None:
            continue

        id_scc = sc_id.get((id_s, id_c))
        if id_scc is None:
            continue

        cursor.execute(
            "INSERT IGNORE INTO votes (id_candidat, id_scrutin_circonscription, voix) VALUES (%s, %s, %s)",
            (id_cand, id_scc, int(row["NB_VOIX"])),
        )
    cnx.commit()

    debug_print("All data inserted successfully.", level=1)
    # endregion

# ---------------------------------------------------------------------------

def clear_database(cnx):
    """
    Truncate all tables in reverse FK order.
    """
    if cnx is None:
        debug_print("No database connection - skipping clear.", level=1)
        return

    debug_print("Clearing database tables...", level=1)
    cursor = cnx.cursor()

    for table in ["votes", "scrutins_circonscriptions", "candidats",
                  "scrutins", "circonscriptions", "bords_politiques"]:
        cursor.execute(f"DELETE FROM {table}")

    cnx.commit()
    debug_print("Database tables cleared.", level=1)
