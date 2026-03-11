# Compare list_candidates.txt and candidates_map.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from candidates_map import candidates_map

list_candidates_path = "list_candidates.txt"

# Load the list of candidates from the text file
with open(list_candidates_path, encoding="utf-8") as f:
    candidates_list = [line.strip() for line in f if line.strip()]

# Check for names in the list that are not in the map
missing = [name for name in candidates_list if name not in candidates_map]

# Check for names in the map that are not in the list
extra = [name for name in candidates_map if name not in candidates_list]

if not missing:
    print("All candidates in the list are in the map.")
else:
    print("More candidates in the list than in the map:")
    for name in missing:
        print(name)

if not extra:
    print("All candidates in the map are in the list.")
else:
    print("More candidates in the map than in the list:")
    for name in extra:
        print(name)