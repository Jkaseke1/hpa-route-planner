import pandas as pd
from geopy.geocoders import Nominatim
import time
import os

# ---------------- CONFIG ----------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, "data", "facilities_raw.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "facilities_geocoded.csv")

geolocator = Nominatim(user_agent="hpa_facilities_mapper_v2", timeout=10)

# Zimbabwe city centroids (fallback)
CITY_CENTROIDS = {
    "Harare": (-17.8252, 31.0335),
    "Bulawayo": (-20.1325, 28.6265),
    "Gweru": (-19.4500, 29.8167),
    "Mutare": (-18.9758, 32.6504),
    "Masvingo": (-20.0744, 30.8328),
    "Chitungwiza": (-18.0127, 31.0756),
    "Kwekwe": (-18.9281, 29.8149),
    "Kadoma": (-18.3333, 29.9167),
    "Bindura": (-17.3019, 31.3306),
    "Marondera": (-18.1853, 31.5519)
}

# ---------------- LOAD DATA ----------------
df = pd.read_csv(INPUT_FILE)

for col in ["Latitude", "Longitude", "Geocode_Status"]:
    if col not in df.columns:
        df[col] = None

geo_cache = {}

# ---------------- GEOCODING ----------------
for index, row in df.iterrows():

    # Skip already geocoded rows
    if pd.notna(row["Latitude"]) and pd.notna(row["Longitude"]):
        continue

    queries = [
        f"{row['Physical Address']}, {row['City']}, Zimbabwe",
        f"{row['City']}, Zimbabwe",
        f"{row['Facility Name']}, {row['City']}, Zimbabwe"
    ]

    location = None
    used_query = None

    for query in queries:
        if not query.strip():
            continue

        if query in geo_cache:
            location = geo_cache[query]
        else:
            try:
                location = geolocator.geocode(query)
                geo_cache[query] = location
                time.sleep(1)
            except Exception:
                location = None

        if location:
            used_query = query
            break

    # ---- SAVE RESULTS ----
    if location:
        df.at[index, "Latitude"] = location.latitude
        df.at[index, "Longitude"] = location.longitude
        df.at[index, "Geocode_Status"] = "Exact / Approximate"
        print(f"✔ Geocoded: {row['Facility Name']}")

    # ---- CITY CENTROID FALLBACK ----
    else:
        city = str(row["City"]).strip()
        if city in CITY_CENTROIDS:
            lat, lon = CITY_CENTROIDS[city]
            df.at[index, "Latitude"] = lat
            df.at[index, "Longitude"] = lon
            df.at[index, "Geocode_Status"] = "City-Level"
            print(f"⚠ City-level used: {row['Facility Name']}")
        else:
            df.at[index, "Geocode_Status"] = "Failed"
            print(f"❌ Failed: {row['Facility Name']}")

# ---------------- SAVE OUTPUT ----------------
df.to_csv(OUTPUT_FILE, index=False)
print("✅ GEOCODING COMPLETE — Output saved to facilities_geocoded.csv")