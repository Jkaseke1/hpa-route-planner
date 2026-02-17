import re

COMMON_REPLACEMENTS = {
    r"\bst\b": "street",
    r"\brd\b": "road",
    r"\bave\b": "avenue",
    r"\bblvd\b": "drive",
    }

def normalize_address(address: str) -> str:
    if not isinstance(address, str):
        return ""

    address = address.lower().strip()

    # remove double spaces
    address = re.sub(r"\s+", " ", address)

    # expand common abbreviations
    for pattern, replacement in COMMON_REPLACEMENTS.items():
        address = re.sub(pattern, replacement, address)

    return address

from rapidfuzz import process, fuzz

KNOWN_STREETS = [
    "routledge avenue",
    "samora machel avenue",
    "jason moyo avenue",
    "borrowdale road",
    "chiremba road",
    "josiah tongogara avenue",
    ]

def correct_street_name(address: str) -> str:
    if not address:
        return address

    match, score, _ = process.extractOne(
        address,
        KNOWN_STREETS,
        scorer=fuzz.partial_ratio
    )

    if score >= 85:
        return match

    return address


    def build_geocodable_address(row):
        parts = []

        if row.get("physical address"):
            addr = normalize_address(row["physical address"])
            addr = correct_street_name(addr)
            parts.append(addr)

        if row.get("city"):
            parts.append(row["city"])

        parts.append("Zimbabwe")

        return ", ".join(parts)

