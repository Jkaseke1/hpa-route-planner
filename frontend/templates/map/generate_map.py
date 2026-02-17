import folium
import pandas as pd

df = pd.read_csv("../data/facilities_geocoded.csv")

# Center of Zimbabwe
m = folium.Map(location=[-19.0, 29.0], zoom_start=6)

for _, r in df.iterrows():
    if pd.notna(r["Latitude"]) and pd.notna(r["Longitude"]):
        folium.Marker(
            [r["Latitude"], r["Longitude"]],
            popup=f"""
            <b>{r['Facility Name']}</b><br>
            {r['Physical Address']}<br>
            {r['City']}
            """
        ).add_to(m)

m.save("map.html")
print("✅ Map generated: map/map.html")
