import pandas as pd
import re
import unicodedata

LEGAL_SUFFIXES = {
    "llc", "inc", "ltd", "corp", "co", "company",
    "gmbh", "sarl", "bv", "plc", "pte", "limited"
}
def normalize_registrar_name(name: str) -> str:
    if not name:
        return ""

    # 1. Normalize unicode (accents, etc.)
    name = unicodedata.normalize("NFKD", name)

    # 2. Lowercase
    name = name.lower()

    # 3. Remove punctuation
    name = re.sub(r"[^\w\s]", " ", name)

    # 4. Tokenize
    tokens = name.split()

    # 5. Remove legal suffixes
    tokens = [t for t in tokens if t not in LEGAL_SUFFIXES]

    # 6. Optional: remove common noise words
    tokens = [t for t in tokens if t not in {"domains", "domain"}]

    # 7. Sort tokens (important!)
    tokens.sort()

    return " ".join(tokens)

def _load_registrars():
    df = pd.read_csv("asdec/enrichers/rdap/assets/registrars.csv")
    
    df = df[["IANA Number", "Registrar Name", "Country/Territory"]]
    df = df.rename(columns={
        "Registrar Name": "name",
        "Country/Territory": "country"
    })
    
    
    df["IANA Number"] = df["IANA Number"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()

    df = df.drop_duplicates(subset="IANA Number").set_index("IANA Number")

    # Primary index (IANA ID)
    by_id = df.to_dict(orient="index")

    # Secondary index (normalized name)
    by_name = {}

    for iana_id, data in by_id.items():
        norm = normalize_registrar_name(data["name"])

        # Handle collisions by storing a list
        if norm not in by_name:
            by_name[norm] = []
        
        by_name[norm].append({
            "iana_id": iana_id,
            **data
        })

    return by_id, by_name
    
    # return df.to_dict(orient="index")

# Loaded ONCE when module is imported
REGISTRARS_BY_ID, REGISTRARS_BY_NAME = _load_registrars()