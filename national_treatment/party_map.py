"""
Map of national (Presidentielle) candidates' surname to their political side.
Covers every candidate present in the 2017 and 2022 Paris source files.
"""

# The raw source files contain a couple of surname typos.
NAME_FIXES = {
    "MeLENCHON": "MELENCHON",
    "PeCRESSE": "PECRESSE",
}

party_map = {
    "ARTHAUD": "EXG",
    "POUTOU": "EXG",
    "MELENCHON": "RG",
    "HIDALGO": "G",
    "JADOT": "G",
    "HAMON": "G",
    "ROUSSEL": "COM",
    "MACRON": "C",
    "LASSALLE": "CD",
    "PECRESSE": "D",
    "FILLON": "D",
    "LE PEN": "EXD",
    "ZEMMOUR": "EXD",
    "DUPONT-AIGNAN": "SOU",
    "ASSELINEAU": "SOU",
    "CHEMINADE": "SOU",
}
