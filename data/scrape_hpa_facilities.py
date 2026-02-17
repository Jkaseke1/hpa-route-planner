import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE_URL = "https://hpa.co.zw/registered-facilities/"
HEADERS = {"Use-Agent": "Mozilla/5.0"}

all_rows = []
page = 1

while True:
    url = f"{BASE_URL}?page={page}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code !=200:
        break

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        break

    rows=table.find("tbody").find_all("tr")

    if not rows:
        break

    for rows in rows:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        all_rows.append(cols)

        page += 1

        columns = [
            "Province",
            "District",
            "Facility Name",
            "Facility Type",
            "Registration Number",
            "Status"
            ]

        df = pd.DataFrame(all_rows, columns=columns)
        df.to_csv("hpa_facilities_live.csv", index=False)

        print(f"Saved {len(df)} facilities")