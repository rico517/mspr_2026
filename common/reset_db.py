"""
Standalone entry point: wipe every election table.

Run once before a full re-clean (see clean_all_data.sh) so that municipal and
national data both start from an empty database - clearing from within each
pipeline individually would wipe out whatever the other one just inserted.
"""

from .db import clear_database

if __name__ == "__main__":
    clear_database()
