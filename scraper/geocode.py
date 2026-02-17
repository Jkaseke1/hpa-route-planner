import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import os
import re
import json

# ==================================================
# PATHS
# ==================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, "data", "facilities_raw.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "facilities_geocoded1.csv")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "data", "geocode_checkpoint.json")

print(f"📥 Reading from: {INPUT_FILE}")
print(f"💾 Saving to: {OUTPUT_FILE}")

# ==================================================
# GEOCODER
# ==================================================
geolocator = Nominatim(
    user_agent="hpa_geographic_mapping (contact: admin@hpa.local)",
    timeout=10
)

# ==================================================
# CHECKPOINTING
# ==================================================
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"last_index": -1}

def save_checkpoint(index):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_index": index}, f)

# ==================================================
# ADDRESS NORMALIZATION
# ==================================================
def normalize_address(address):
    if pd.isna(address):
        return ""

    address = str(address).lower()

    address = re.sub(r"(\d)([a-z])", r"\1 \2", address)
    address = re.sub(r"([a-z])(\d)", r"\1 \2", address)
    address = re.sub(r"([a-z])([A-Z])", r"\1 \2", address)

    replacements = {
        "ave": "avenue",
        "rd": "road",
        "st": "street",
        "cnr": "corner",
        "&": "and",
        "n.mandela": "nelson mandela",
        "n mandela": "nelson mandela",
        "r mugabe": "robert mugabe",
        "josiah tongogara": "josiah tongogara avenue"
    }

    for k, v in replacements.items():
        address = address.replace(k, v)

    address = re.sub(r"[^\w\s]", " ", address)
    return re.sub(r"\s+", " ", address).strip().title()

# ==================================================
# LOAD DATA
# ==================================================
df = pd.read_csv(INPUT_FILE)

if "Latitude" not in df.columns:
    df["Latitude"] = None
if "Longitude" not in df.columns:
    df["Longitude"] = None

# ==================================================
# RESUME FROM CHECKPOINT
# ==================================================
checkpoint = load_checkpoint()
start_index = checkpoint["last_index"] + 1

print(f"▶ Resuming from row {start_index} of {len(df)}")

# ==================================================
# GEOCODING LOOP
# ==================================================
for index, row in df.iloc[start_index:].iterrows():

    if pd.notna(row["Latitude"]) and pd.notna(row["Longitude"]):
        save_checkpoint(index)
        continue

    facility = row.get("Facility Name", "Unknown Facility")
    city = row.get("City", "")
    raw_address = row.get("Physical Address", "")

    clean_address = normalize_address(raw_address)

    queries = [
        f"{clean_address}, {city}, Zimbabwe",
        f"{clean_address}, {city}",
        f"{city}, Zimbabwe"
    ]

    success = False

    for attempt, query in enumerate(queries, start=1):
        if not clean_address and attempt < 3:
            continue

        try:
            location = geolocator.geocode(query)
            if location:
                df.at[index, "Latitude"] = location.latitude
                df.at[index, "Longitude"] = location.longitude
                print(f"✔ Geocoded → {facility} | {query}")
                success = True
                break

        except (GeocoderTimedOut, GeocoderServiceError):
            wait = attempt * 5
            print(f"⏳ Retry in {wait}s → {query}")
            time.sleep(wait)

    if not success:
        print(f"⚠ Skipped → {facility}")

    # SAVING PROGRESS AFTER EACH ROW
    save_checkpoint(index)
    df.to_csv(OUTPUT_FILE, index=False)

    # Respect OpenStreetMap usage policy
    time.sleep(2)

# ==================================================
# CLEANUP
# ==================================================
df.to_csv(OUTPUT_FILE, index=False)

if os.path.exists(CHECKPOINT_FILE):
    os.remove(CHECKPOINT_FILE)

print("✅ GEOCODING COMPLETE")