import pandas as pd
import os
from datetime import datetime

# Paths
DATA_DIR = "data"
GEOCODED_FILE = os.path.join(DATA_DIR, "facilities_geocoded.csv")
FINAL_FILE = os.path.join(DATA_DIR, "facilities_final.csv")

# Load geocoded data
df = pd.read_csv(GEOCODED_FILE)

# Optional: Add extra helpful columns
df['Last_Updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df['Source'] = 'HPA'

# Clean text columns
df['Facility Name'] = df['Facility Name'].str.strip()
df['Physical Address'] = df['Physical Address'].str.strip()
df['City'] = df['City'].str.strip()

# Ensure latitude/longitude are numeric
df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

# Save final dashboard-ready CSV
df.to_csv(FINAL_FILE, index=False)
print(f"✅ Final dataset saved: {FINAL_FILE}")
print(f"Total facilities: {len(df)}")
print(f"Geocoded: {df['Geocoded_Status'].value_counts().to_dict()}")