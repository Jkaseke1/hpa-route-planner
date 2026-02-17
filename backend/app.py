from flask import Flask, jsonify, render_template, request
import pandas as pd
import os
import subprocess
from datetime import datetime
import hashlib
import json
import logging
import smtplib
from email.mime.text import MIMEText
from backend.utils.facility_type import detect_facility_type

logging.basicConfig(level=logging.INFO)

# =========================
# APP SETUP
# =========================
app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static"
)
app.config["JSON_SORT_KEYS"] = False

# =========================
# PATHS
# =========================
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data")
)

RAW_FILE = os.path.join(BASE_DIR, "facilities_raw.csv")
CLEANED_FILE = os.path.join(BASE_DIR, "facilities_cleaned.csv")
GEOCODED_FILE = os.path.join(BASE_DIR, "facilities_geocoded.csv")

HASH_FILE = os.path.join(BASE_DIR, "last_hash.txt")
LOG_FILE = os.path.join(BASE_DIR, "change_log.json")

# =========================
# HELPERS
# =========================
def load_best_data():
    """Load the best available CSV"""
    for path in [GEOCODED_FILE, CLEANED_FILE, RAW_FILE]:
        if os.path.exists(path):
            df = pd.read_csv(path)
            if df.empty:
                continue

            df.columns = df.columns.str.strip().str.lower()

            # Normalize facility name
            if "facility_name" in df.columns and "facility name" not in df.columns:
                df.rename(columns={"facility_name": "facility name"}, inplace=True)

            # Normalize lat/lng
            for col in ["latitude", "longitude"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Add facility type if missing
            if "facility_type" not in df.columns:
                df["facility_type"] = df["facility name"].apply(detect_facility_type)

            return df

    logging.warning("No data files found")
    return pd.DataFrame()


def safe_col(df, col):
    return col in df.columns


def file_hash(path):
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def detect_changes():
    """Detect if geocoded CSV changed, log and send email alerts if new facility type"""
    if not os.path.exists(GEOCODED_FILE):
        return False

    new_hash = file_hash(GEOCODED_FILE)
    old_hash = None

    if os.path.exists(HASH_FILE):
        with open(HASH_FILE) as f:
            old_hash = f.read().strip()

    if new_hash == old_hash:
        return False

    # Update hash
    with open(HASH_FILE, "w") as f:
        f.write(new_hash)

    # Load data
    df = load_best_data()
    if df.empty:
        return False

    # Detect new facility types
    known_types = set()
    if os.path.exists(LOG_FILE):
        history = json.load(open(LOG_FILE))
        for h in history:
            known_types.update(h.get("facility_types", []))
    else:
        history = []

    current_types = set(df["facility_type"].dropna().unique())
    new_types = current_types - known_types

    # Log change
    history.append({
        "timestamp": datetime.now().isoformat(),
        "message": "HPA facilities data changed",
        "facility_types": list(current_types)
    })
    json.dump(history, open(LOG_FILE, "w"), indent=2)

    # Send email if new facility type appears
    if new_types:
        send_email_alert(new_types, change_type="new_facility_types")

    return True


def send_email_alert(new_types, change_type="new_facility_types"):
    """Send email alert for changes"""
    # SMTP Configuration - Update with actual credentials
    SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
    SMTP_USER = os.environ.get("SMTP_USER", "")
    SMTP_PASS = os.environ.get("SMTP_PASS", "")
    TO_EMAIL = ["nmkanganwi@pulse-pharmaceuticals.co.zw"]

    if change_type == "new_facility_types":
        subject = "🚨 HPA Facilities: New Facility Types Detected"
        body = f"""Hello,

The HPA Geographic Mapping system has detected new facility types:

{', '.join(new_types)}

Please review the dashboard for more details.

Best regards,
HPA Facilities Intelligence System"""
    elif change_type == "data_refresh":
        subject = "🔄 HPA Facilities: Data Refreshed"
        body = f"""Hello,

The HPA Geographic Mapping data has been refreshed.

Changes detected:
{new_types}

Please review the dashboard for more details.

Best regards,
HPA Facilities Intelligence System"""
    else:
        subject = "📊 HPA Facilities: Update Notification"
        body = f"""Hello,

An update has occurred in the HPA Geographic Mapping system.

Details:
{new_types}

Best regards,
HPA Facilities Intelligence System"""

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(TO_EMAIL)

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, TO_EMAIL, msg.as_string())
        server.quit()
        logging.info(f"Email alert sent: {new_types}")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")


# =========================
# ROUTES
# =========================
@app.route("/")
def dashboard():
    return render_template("index.html")


@app.route("/map")
def map_view():
    return render_template("map.html")


# =========================
# API – DASHBOARD SUMMARY
# =========================
@app.route("/api/summary")
def summary():
    df = load_best_data()
    if df.empty:
        return jsonify({
            "total": 0,
            "geocoded": 0,
            "missing": 0,
            "geocode_rate": 0,
            "cities": 0,
            "top_cities": [],
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    total = len(df)
    geocoded = df["latitude"].notna().sum() if safe_col(df, "latitude") else 0
    missing = total - geocoded
    geocode_rate = round((geocoded / total) * 100, 1) if total else 0
    cities = df["city"].dropna().nunique() if safe_col(df, "city") else 0

    top_cities = []
    if safe_col(df, "city"):
        top_cities = (
            df.groupby("city")
            .size()
            .sort_values(ascending=False)
            .head(10)
            .reset_index(name="count")
            .to_dict(orient="records")
        )

    return jsonify({
        "total": int(total),
        "geocoded": int(geocoded),
        "missing": int(missing),
        "geocode_rate": geocode_rate,
        "cities": int(cities),
        "top_cities": top_cities,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })


# =========================
# API – MAP FACILITIES
# =========================
@app.route("/api/map/facilities")
def map_facilities():
    df = load_best_data()
    if df.empty or not (safe_col(df, "latitude") and safe_col(df, "longitude")):
        return jsonify([])

    df = df[df["latitude"].notna() & df["longitude"].notna()]
    df["facility_type"] = df["facility_type"].fillna("unknown")

    df = df.where(pd.notnull(df), None)
    data = df[["facility name", "city", "latitude", "longitude", "facility_type"]].rename(
        columns={
            "facility name": "Facility Name",
            "city": "City",
            "latitude": "Latitude",
            "longitude": "Longitude",
            "facility_type": "Facility Type"
        }
    ).to_dict(orient="records")

    return jsonify(data)


# =========================
# API – REFRESH PIPELINE
# =========================
@app.route("/api/refresh", methods=["POST"])
def refresh_pipeline():
    try:
        subprocess.run(["python", "scraper.py"], check=True)
        subprocess.run(["python", "geocode.py"], check=True)

        # Optionally, rebuild final CSV
        df = load_best_data()
        df.to_csv(GEOCODED_FILE, index=False)

        detect_changes()

        send_email_alert("Data refreshed", change_type="data_refresh")

        return jsonify({
            "status": "success",
            "message": "Dashboard refreshed successfully",
            "timestamp": datetime.now().strftime("%H:%M:%S")
        })
    except Exception as e:
        logging.exception("Refresh failed")
        return jsonify({
            "status": "failed",
            "error": str(e)
        }), 500


# =========================
# API – FACILITY TYPE SUMMARY
# =========================
@app.route("/api/facility-types")
def facility_types():
    """Get facilities grouped by type with counts"""
    df = load_best_data()
    if df.empty:
        return jsonify({"types": [], "counts": {}})
    
    type_counts = df["facility_type"].value_counts().to_dict()
    
    return jsonify({
        "types": list(type_counts.keys()),
        "counts": type_counts
    })


# =========================
# API – AUTO REFRESH ON LOGIN
# =========================
@app.route("/api/auto-refresh", methods=["POST"])
def auto_refresh():
    """Trigger data refresh when user logs in"""
    try:
        changed = detect_changes()
        df = load_best_data()
        
        return jsonify({
            "status": "success",
            "data_changed": changed,
            "total_facilities": len(df),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    except Exception as e:
        logging.exception("Auto-refresh failed")
        return jsonify({"status": "error", "message": str(e)}), 500


# =========================
# API – ROUTE PLANNER
# =========================
@app.route("/api/route/facilities-on-route", methods=["POST"])
def facilities_on_route():
    """
    Find facilities along a route between origin and destination.
    Uses a corridor approach - finds facilities within a certain distance of the route.
    """
    data = request.get_json()
    
    origin_lat = data.get("origin_lat")
    origin_lon = data.get("origin_lon")
    dest_lat = data.get("dest_lat")
    dest_lon = data.get("dest_lon")
    corridor_km = data.get("corridor_km", 10)  # Default 10km corridor
    facility_types_filter = data.get("facility_types", [])  # Optional filter
    
    if not all([origin_lat, origin_lon, dest_lat, dest_lon]):
        return jsonify({"error": "Missing coordinates"}), 400
    
    df = load_best_data()
    if df.empty:
        return jsonify({"facilities": [], "total": 0})
    
    # Filter to geocoded facilities only
    df = df[df["latitude"].notna() & df["longitude"].notna()]
    
    # Apply facility type filter if provided
    if facility_types_filter:
        df = df[df["facility_type"].isin(facility_types_filter)]
    
    # Calculate distance from route line for each facility
    from math import radians, sin, cos, sqrt, atan2
    
    def haversine(lat1, lon1, lat2, lon2):
        """Calculate distance in km between two points"""
        R = 6371  # Earth's radius in km
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))
        return R * c
    
    def point_to_line_distance(px, py, x1, y1, x2, y2):
        """Calculate perpendicular distance from point to line segment"""
        # Vector from line start to point
        dx = x2 - x1
        dy = y2 - y1
        
        if dx == 0 and dy == 0:
            return haversine(py, px, y1, x1)
        
        # Parameter t for closest point on line
        t = max(0, min(1, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
        
        # Closest point on line
        closest_x = x1 + t * dx
        closest_y = y1 + t * dy
        
        return haversine(py, px, closest_y, closest_x)
    
    facilities_on_route = []
    
    for _, row in df.iterrows():
        facility_lat = row["latitude"]
        facility_lon = row["longitude"]
        
        # Calculate distance from route
        distance = point_to_line_distance(
            facility_lon, facility_lat,
            origin_lon, origin_lat,
            dest_lon, dest_lat
        )
        
        # Also calculate distance from origin (for sorting)
        dist_from_origin = haversine(origin_lat, origin_lon, facility_lat, facility_lon)
        
        if distance <= corridor_km:
            facilities_on_route.append({
                "facility_name": row.get("facility name", "Unknown"),
                "city": row.get("city", ""),
                "facility_type": row.get("facility_type", "Unknown"),
                "latitude": facility_lat,
                "longitude": facility_lon,
                "distance_from_route_km": round(distance, 2),
                "distance_from_origin_km": round(dist_from_origin, 2)
            })
    
    # Sort by distance from origin
    facilities_on_route.sort(key=lambda x: x["distance_from_origin_km"])
    
    return jsonify({
        "facilities": facilities_on_route,
        "total": len(facilities_on_route),
        "corridor_km": corridor_km,
        "origin": {"lat": origin_lat, "lon": origin_lon},
        "destination": {"lat": dest_lat, "lon": dest_lon}
    })


@app.route("/api/route/geocode-address", methods=["POST"])
def geocode_address():
    """Geocode an address for route planning"""
    from geopy.geocoders import Nominatim
    
    data = request.get_json()
    address = data.get("address", "")
    
    if not address:
        return jsonify({"error": "Address required"}), 400
    
    # Add Zimbabwe context if not present
    if "zimbabwe" not in address.lower():
        address = f"{address}, Zimbabwe"
    
    try:
        geolocator = Nominatim(user_agent="hpa_route_planner")
        location = geolocator.geocode(address, timeout=10)
        
        if location:
            return jsonify({
                "success": True,
                "latitude": location.latitude,
                "longitude": location.longitude,
                "display_name": location.address
            })
        else:
            return jsonify({"success": False, "error": "Address not found"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/route-planner")
def route_planner():
    """Route planner page for sales reps"""
    return render_template("route_planner.html")


# =========================
# RUN
# =========================
if __name__ == "__main__":
    # Auto-refresh on startup
    detect_changes()
    app.run(host='0.0.0.0', debug=True, use_reloader=False)