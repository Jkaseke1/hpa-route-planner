import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT = os.path.join(BASE_DIR, "data", "facilities_raw.csv")
OUTPUT = os.path.join(BASE_DIR, "data", "facilities_cleaned.csv")

df = pd.read_csv(INPUT)

# Standardize column names
df.columns = df.columns.str.lower().str.strip()

# Clean text fields
for col in ["facility name", "physical address", "city"]:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.title()
            .replace("Nan", None)
        )

# Remove duplicates
df = df.drop_duplicates(subset=["facility name", "physical address", "city"])

# Replace empty strings with NaN
df = df.replace("", pd.NA)

df.to_csv(OUTPUT, index=False)
print(f"✅ Cleaned data saved to {OUTPUT}")

