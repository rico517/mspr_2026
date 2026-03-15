"""
This script is simply used to clear all the data inside the database
"""

from utils.debug import debug_print
from db.db_cnx import connect_to_database

def clear_database(cnx = None):
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

if __name__ == "__main__":
    clear_database()