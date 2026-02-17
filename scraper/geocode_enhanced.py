import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import os
import re
import json
from difflib import SequenceMatcher
from functools import lru_cache

# ==================================================
# PATHS
# ==================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, "data", "facilities_raw.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "facilities_geocoded.csv")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "data", "geocode_checkpoint.json")
CACHE_FILE = os.path.join(BASE_DIR, "data", "geocode_cache.json")

print(f"📥 Reading from: {INPUT_FILE}")
print(f"💾 Saving to: {OUTPUT_FILE}")

# ==================================================
# GEOCODER
# ==================================================
geolocator = Nominatim(
    user_agent="hpa_geographic_mapping (contact: admin@hpa.local)",
    timeout=15
)

# ==================================================
# CACHE MANAGEMENT
# ==================================================
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

geocode_cache = load_cache()

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
# ZIMBABWE LOCATION DATABASE FOR FUZZY MATCHING
# ==================================================
ZIMBABWE_CITIES = [
    "Harare", "Bulawayo", "Chitungwiza", "Mutare", "Gweru", "Kwekwe", "Kadoma",
    "Masvingo", "Chinhoyi", "Marondera", "Norton", "Chegutu", "Bindura", "Beitbridge",
    "Redcliff", "Victoria Falls", "Hwange", "Chiredzi", "Kariba", "Karoi", "Chipinge",
    "Zvishavane", "Rusape", "Shurugwi", "Gokwe", "Plumtree", "Gwanda", "Lupane",
    "Murewa", "Ruwa", "Epworth", "Borrowdale", "Avondale", "Hatfield", "Highlands",
    "Mbare", "Highfield", "Glen Norah", "Budiriro", "Warren Park", "Mufakose",
    "Dzivarasekwa", "Kambuzuma", "Kuwadzana", "Marlborough", "Mount Pleasant",
    "Greendale", "Eastlea", "Arcadia", "Belvedere", "Milton Park", "Alexandra Park",
    "Newlands", "Hillside", "Burnside", "Matsheumhlope", "Nkulumane", "Pumula",
    "Nketa", "Cowdray Park", "Luveve", "Entumbane", "Magwegwe", "Lobengula",
    "Emakhandeni", "Tshabalala", "Njube", "Mpopoma", "Makokoba", "Barbourfields",
    "Filabusi", "Glendale", "Concession", "Shamva", "Mazowe", "Domboshava",
    "Hatcliffe", "Borrowdale Brooke", "Glen Lorne", "Chisipite", "Greystone Park"
]

COMMON_STREETS = [
    "Nelson Mandela", "Robert Mugabe", "Samora Machel", "Julius Nyerere",
    "Herbert Chitepo", "Jason Moyo", "Leopold Takawira", "Josiah Tongogara",
    "Simon Muzenda", "Joshua Nkomo", "Fife Avenue", "Enterprise Road",
    "Borrowdale Road", "Harare Drive", "Seke Road", "Chiremba Road",
    "Lomagundi Road", "Bulawayo Road", "Mutare Road", "Masvingo Road"
]

# ==================================================
# FUZZY MATCHING FUNCTIONS
# ==================================================
def similarity_ratio(a, b):
    """Calculate similarity between two strings"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def find_best_city_match(city_input):
    """Find the best matching city from known Zimbabwe cities"""
    if not city_input:
        return None
    
    city_input = str(city_input).strip()
    
    # Exact match first
    for city in ZIMBABWE_CITIES:
        if city.lower() == city_input.lower():
            return city
    
    # Fuzzy match
    best_match = None
    best_score = 0.0
    
    for city in ZIMBABWE_CITIES:
        score = similarity_ratio(city_input, city)
        if score > best_score and score >= 0.7:  # 70% threshold
            best_score = score
            best_match = city
    
    if best_match:
        print(f"  🔄 Fuzzy city match: '{city_input}' → '{best_match}' ({best_score:.0%})")
    
    return best_match or city_input

def find_best_street_match(address):
    """Find and correct common street name misspellings"""
    if not address:
        return address
    
    address_lower = address.lower()
    
    # Common misspellings and corrections
    corrections = {
        "n.mandela": "Nelson Mandela",
        "n mandela": "Nelson Mandela",
        "n. mandela": "Nelson Mandela",
        "nmandela": "Nelson Mandela",
        "r mugabe": "Robert Mugabe",
        "r.mugabe": "Robert Mugabe",
        "rmugabe": "Robert Mugabe",
        "s machel": "Samora Machel",
        "j tongogara": "Josiah Tongogara",
        "h chitepo": "Herbert Chitepo",
        "j moyo": "Jason Moyo",
        "l takawira": "Leopold Takawira",
        "fife ave": "Fife Avenue",
        "borrowdale rd": "Borrowdale Road",
        "enterprise rd": "Enterprise Road",
        "chiremba rd": "Chiremba Road"
    }
    
    result = address
    for wrong, correct in corrections.items():
        if wrong in address_lower:
            result = re.sub(re.escape(wrong), correct, result, flags=re.IGNORECASE)
    
    return result

# ==================================================
# ADDRESS NORMALIZATION (ENHANCED)
# ==================================================
def normalize_address(address):
    if pd.isna(address):
        return ""

    address = str(address)
    
    # Fix common street name misspellings first
    address = find_best_street_match(address)
    
    # Add spaces between numbers and letters
    address = re.sub(r"(\d)([a-zA-Z])", r"\1 \2", address)
    address = re.sub(r"([a-zA-Z])(\d)", r"\1 \2", address)
    
    # Standard abbreviation replacements
    replacements = {
        r"\bave\b": "Avenue",
        r"\brd\b": "Road",
        r"\bst\b": "Street",
        r"\bcnr\b": "Corner",
        r"\b&\b": "and",
        r"\bblvd\b": "Boulevard",
        r"\bdr\b": "Drive",
        r"\bcres\b": "Crescent",
        r"\bct\b": "Court",
        r"\bext\b": "Extension"
    }

    for pattern, replacement in replacements.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)

    # Clean up punctuation and extra spaces
    address = re.sub(r"[^\w\s,]", " ", address)
    address = re.sub(r"\s+", " ", address).strip()
    
    return address.title()

# ==================================================
# GEOCODING WITH MULTIPLE STRATEGIES
# ==================================================
def geocode_with_fallback(facility_name, address, city, max_retries=3):
    """
    Try multiple geocoding strategies with fuzzy matching
    Returns (latitude, longitude, matched_query) or (None, None, None)
    """
    global geocode_cache
    
    # Normalize inputs
    clean_address = normalize_address(address)
    matched_city = find_best_city_match(city)
    
    # Build query variations (most specific to least specific)
    queries = []
    
    # Strategy 1: Full address with city and country
    if clean_address:
        queries.append(f"{clean_address}, {matched_city}, Zimbabwe")
    
    # Strategy 2: Address without building numbers
    if clean_address:
        simplified = re.sub(r"^\d+\s*", "", clean_address)
        simplified = re.sub(r"\d+\s*", "", simplified)
        if simplified and simplified != clean_address:
            queries.append(f"{simplified}, {matched_city}, Zimbabwe")
    
    # Strategy 3: Just street name and city
    if clean_address:
        # Extract potential street name
        street_match = re.search(r"([A-Za-z\s]+(?:Avenue|Road|Street|Drive|Way|Crescent))", clean_address, re.IGNORECASE)
        if street_match:
            queries.append(f"{street_match.group(1)}, {matched_city}, Zimbabwe")
    
    # Strategy 4: Facility name with city (for well-known places)
    if facility_name:
        queries.append(f"{facility_name}, {matched_city}, Zimbabwe")
    
    # Strategy 5: Just city and country (fallback to city center)
    queries.append(f"{matched_city}, Zimbabwe")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_queries = []
    for q in queries:
        if q.lower() not in seen:
            seen.add(q.lower())
            unique_queries.append(q)
    
    # Try each query
    for query in unique_queries:
        # Check cache first
        cache_key = query.lower().strip()
        if cache_key in geocode_cache:
            cached = geocode_cache[cache_key]
            if cached.get("lat") and cached.get("lon"):
                print(f"  📦 Cache hit: {query}")
                return cached["lat"], cached["lon"], query
        
        # Try geocoding
        for attempt in range(max_retries):
            try:
                location = geolocator.geocode(query, exactly_one=True)
                
                if location:
                    # Validate the result is in Zimbabwe (rough bounds check)
                    if -22.5 <= location.latitude <= -15.0 and 25.0 <= location.longitude <= 34.0:
                        # Cache the result
                        geocode_cache[cache_key] = {
                            "lat": location.latitude,
                            "lon": location.longitude,
                            "display": location.address
                        }
                        save_cache(geocode_cache)
                        return location.latitude, location.longitude, query
                    else:
                        print(f"  ⚠️ Result outside Zimbabwe bounds: {location.address}")
                
                break  # No result, try next query
                
            except GeocoderTimedOut:
                wait = (attempt + 1) * 3
                print(f"  ⏳ Timeout, retry in {wait}s...")
                time.sleep(wait)
                
            except GeocoderServiceError as e:
                print(f"  ❌ Service error: {e}")
                time.sleep(5)
                break
        
        # Rate limiting
        time.sleep(1)
    
    return None, None, None

# ==================================================
# LOAD DATA
# ==================================================
df = pd.read_csv(INPUT_FILE)

if "Latitude" not in df.columns:
    df["Latitude"] = None
if "Longitude" not in df.columns:
    df["Longitude"] = None
if "Geocode_Query" not in df.columns:
    df["Geocode_Query"] = None

# ==================================================
# RESUME FROM CHECKPOINT
# ==================================================
checkpoint = load_checkpoint()
start_index = checkpoint["last_index"] + 1

print(f"▶ Resuming from row {start_index} of {len(df)}")
print(f"📦 Cache contains {len(geocode_cache)} entries")

# ==================================================
# GEOCODING LOOP
# ==================================================
success_count = 0
fail_count = 0

for index, row in df.iloc[start_index:].iterrows():
    # Skip if already geocoded
    if pd.notna(row["Latitude"]) and pd.notna(row["Longitude"]):
        save_checkpoint(index)
        continue

    facility = row.get("Facility Name", "Unknown Facility")
    city = row.get("City", "")
    raw_address = row.get("Physical Address", "")

    print(f"\n[{index + 1}/{len(df)}] {facility}")
    
    lat, lon, matched_query = geocode_with_fallback(facility, raw_address, city)
    
    if lat and lon:
        df.at[index, "Latitude"] = lat
        df.at[index, "Longitude"] = lon
        df.at[index, "Geocode_Query"] = matched_query
        print(f"  ✔ Geocoded: ({lat:.6f}, {lon:.6f})")
        success_count += 1
    else:
        print(f"  ⚠ Could not geocode")
        fail_count += 1

    # Save progress after each row
    save_checkpoint(index)
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Rate limiting for Nominatim
    time.sleep(1.5)

# ==================================================
# FINAL SAVE AND CLEANUP
# ==================================================
df.to_csv(OUTPUT_FILE, index=False)

if os.path.exists(CHECKPOINT_FILE):
    os.remove(CHECKPOINT_FILE)

print(f"\n{'='*50}")
print(f"✅ GEOCODING COMPLETE")
print(f"   Success: {success_count}")
print(f"   Failed:  {fail_count}")
print(f"   Total:   {len(df)}")
print(f"   Cache:   {len(geocode_cache)} entries")
print(f"{'='*50}")
