"""
Single shared entry point for every database action used across the project
(connection, insertion, clearing). Both municipal_treatment and national_treatment
produce the same long-form DataFrame (see common.schema.OUTPUT_COLS) and hand it to
export_dataset_to_db, only the scrutin type ("Municipales" / "Presidentielle") differs.

Insertion order (respects FK dependencies):
    1. bords_politiques
    2. candidats
    3. scrutins
    4. circonscriptions
    5. scrutins_circonscriptions
    6. votes
"""

import os
import time

import mysql.connector
from mysql.connector import Error

from .logging import debug_print

MAX_RETRIES = 5
RETRY_DELAY = 10

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "root")
DB_NAME = os.environ.get("DB_NAME", "elections_db")


def connect_to_database():
    """
    Establish a connection to the database, retrying MAX_RETRIES times on failure.
    """
    for attempt in range(MAX_RETRIES):
        try:
            return mysql.connector.connect(
                host=DB_HOST, port=DB_PORT,
                user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
                connect_timeout=30, auth_plugin="mysql_native_password",
            )
        except Error as e:
            print(f"Error connecting to {DB_NAME}: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"New attempt in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Failed to connect to {DB_NAME} after {MAX_RETRIES} attempts")
                raise

# ---------------------------------------------------------------------------
# Generic helpers, shared by every table sync below.
# ---------------------------------------------------------------------------


def sync_dimension(cursor, table, key_cols, rows, extra_cols=None):
    """
    Ensure each row exists in `table` (matched on key_cols, inserted via INSERT
    IGNORE with key_cols + extra_cols when missing), then return a cache dict
    built from a single fresh SELECT: key -> id. The key is a scalar when there
    is a single key column, otherwise a tuple.
    """
    all_cols = list(key_cols) + list(extra_cols or [])
    where_clause = " AND ".join(f"{col} = %s" for col in key_cols)
    select_sql = f"SELECT id FROM {table} WHERE {where_clause}"
    insert_sql = (
        f"INSERT IGNORE INTO {table} ({', '.join(all_cols)}) "
        f"VALUES ({', '.join(['%s'] * len(all_cols))})"
    )

    for row in rows:
        cursor.execute(select_sql, row[: len(key_cols)])
        if not cursor.fetchone():
            cursor.execute(insert_sql, row)

    cursor.execute(f"SELECT id, {', '.join(key_cols)} FROM {table}")
    cache = {}
    for record in cursor.fetchall():
        row_id, *key_parts = record
        cache[key_parts[0] if len(key_parts) == 1 else tuple(key_parts)] = row_id
    return cache


def bulk_insert_ignore(cursor, table, columns, rows):
    insert_sql = (
        f"INSERT IGNORE INTO {table} ({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(columns))})"
    )
    for row in rows:
        cursor.execute(insert_sql, row)

# ---------------------------------------------------------------------------


def export_dataset_to_db(df, scrutin_type, cnx=None):
    """
    Insert a long-form election DataFrame into the database.

    Expected columns (see common.schema.OUTPUT_COLS):
        NOM, PRENOM, BORD_POL, ANNEE, TOUR, NUM_CIRC,
        NB_INSCR, NB_VOTANT, NB_EXPRIM, NB_BL_NUL, NB_VOIX
    """
    should_close_cnx_at_end = False
    if cnx is None:
        cnx = connect_to_database()
        should_close_cnx_at_end = True

    debug_print(f"\nInserting {scrutin_type} data into the database...", level=1)
    cursor = cnx.cursor()

    debug_print("  Syncing bords_politiques...", level=2)
    bord_id = sync_dimension(
        cursor, "bords_politiques", ["label"],
        [(label,) for label in df["BORD_POL"].dropna().unique()],
    )
    cnx.commit()

    debug_print("  Syncing candidats...", level=2)
    unique_candidates = (
        df[["NOM", "PRENOM", "BORD_POL"]].drop_duplicates().dropna(subset=["BORD_POL"])
    )
    candidate_rows = [
        (row.NOM, row.PRENOM, bord_id[row.BORD_POL])
        for row in unique_candidates.itertuples()
        if row.BORD_POL in bord_id
    ]
    candidat_id = sync_dimension(
        cursor, "candidats", ["nom", "prenom"], candidate_rows,
        extra_cols=["id_bord_politique"],
    )
    cnx.commit()

    debug_print("  Syncing scrutins...", level=2)
    scrutin_rows = [
        (scrutin_type, int(annee), int(tour))
        for annee, tour in df[["ANNEE", "TOUR"]].drop_duplicates().itertuples(index=False)
    ]
    scrutin_id = sync_dimension(cursor, "scrutins", ["type", "annee", "tour"], scrutin_rows)
    cnx.commit()

    debug_print("  Syncing circonscriptions...", level=2)
    circ_id = sync_dimension(
        cursor, "circonscriptions", ["code"],
        [(int(code),) for code in df["NUM_CIRC"].unique()],
    )
    cnx.commit()

    debug_print("  Syncing scrutins_circonscriptions...", level=2)
    circ_stats = df.drop_duplicates(subset=["ANNEE", "TOUR", "NUM_CIRC"])
    sc_rows = []
    for row in circ_stats.itertuples():
        id_s = scrutin_id.get((scrutin_type, int(row.ANNEE), int(row.TOUR)))
        id_c = circ_id.get(int(row.NUM_CIRC))
        if id_s is None or id_c is None:
            continue
        sc_rows.append((
            id_s, id_c,
            int(row.NB_INSCR),
            int(row.NB_INSCR) - int(row.NB_VOTANT),  # abstentions
            int(row.NB_VOTANT),
            int(row.NB_EXPRIM),
            int(row.NB_BL_NUL),
        ))
    sc_id = sync_dimension(
        cursor, "scrutins_circonscriptions", ["id_scrutin", "id_circonscription"],
        sc_rows, extra_cols=["inscrits", "abstentions", "votants", "exprimes", "blancs_nuls"],
    )
    cnx.commit()

    debug_print("  Inserting votes...", level=2)
    vote_rows = []
    for row in df.itertuples():
        id_cand = candidat_id.get((row.NOM, row.PRENOM))
        id_s = scrutin_id.get((scrutin_type, int(row.ANNEE), int(row.TOUR)))
        id_c = circ_id.get(int(row.NUM_CIRC))
        if id_cand is None or id_s is None or id_c is None:
            continue
        id_scc = sc_id.get((id_s, id_c))
        if id_scc is None:
            continue
        vote_rows.append((id_cand, id_scc, int(row.NB_VOIX)))
    bulk_insert_ignore(cursor, "votes", ["id_candidat", "id_scrutin_circonscription", "voix"], vote_rows)
    cnx.commit()

    if should_close_cnx_at_end:
        cnx.close()
    debug_print(f"{scrutin_type} data inserted successfully.", level=1)


def clear_database(cnx=None):
    """
    Truncate all tables in reverse FK order.
    """
    should_close_cnx_at_end = False
    if cnx is None:
        cnx = connect_to_database()
        should_close_cnx_at_end = True

    debug_print("Clearing database tables...", level=1)
    cursor = cnx.cursor()

    for table in ["votes", "scrutins_circonscriptions", "candidats",
                  "scrutins", "circonscriptions", "bords_politiques"]:
        cursor.execute(f"DELETE FROM {table}")

    cnx.commit()

    if should_close_cnx_at_end:
        cnx.close()

    debug_print("Database tables cleared.", level=1)
