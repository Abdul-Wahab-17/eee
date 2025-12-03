#!/usr/bin/env python3
import requests
import pandas as pd
import time
from pathlib import Path

class USDAQuickStats:
    def __init__(self, api_key=None):
        self.base = "https://quickstats.nass.usda.gov/api/get_param_values"
        self.api = "https://quickstats.nass.usda.gov/api/api_GET/"
        self.key = api_key or "760A18DD-29A5-3562-814A-79B45D1A70EB"

    def query(self, params):
        """Perform a QuickStats API GET request."""
        params["key"] = self.key
        try:
            r = requests.get(self.api, params=params, timeout=40)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            print("Error:", e, params)
            return []

# -------------------------------------------------------------
# MAJOR CROPS (Option B — Unified Classes)
# -------------------------------------------------------------
MAJOR_CROPS = [
    "CORN",
    "SOYBEANS",
    "WHEAT",
    "COTTON",
    "RICE",
    "BARLEY",
    "SORGHUM",
    "PEANUTS",
    "SUGARCANE",
    "SUGARBEETS",
    "OATS",
    "SUNFLOWER"
]

# -------------------------------------------------------------
# CONFIG FOR METRICS (Will fetch both)
# -------------------------------------------------------------
METRIC_DEFINITIONS = {
    "YIELD": {
        "statisticcat_desc": "YIELD",
        "unit_desc": "BU / ACRE"
    },
    "AREA_PLANTED": {
        "statisticcat_desc": "AREA PLANTED"
    }
}

# -------------------------------------------------------------
# MASTER DOWNLOAD FUNCTION
# -------------------------------------------------------------
def download_major_crop_dataset(start_year=1980, end_year=2025, output="us_major_crops_yield_and_acres.csv"):

    usda = USDAQuickStats()
    all_records = []

    print("\n==========================================================")
    print("   DOWNLOADING MAJOR CROP DATA (YIELD + ACRES PLANTED)")
    print("==========================================================\n")

    for crop in MAJOR_CROPS:
        for metric, settings in METRIC_DEFINITIONS.items():

            base_params = {
                "source_desc": "SURVEY",
                "sector_desc": "CROPS",
                "group_desc": "FIELD CROPS",
                "commodity_desc": crop,
                "agg_level_desc": "COUNTY",
                "year__GE": start_year,
                "year__LE": end_year
            }

            base_params.update(settings)

            print(f"\n>>> Fetching {crop} — {metric} ...")
            data = usda.query(base_params)

            print(f"    ✓ Returned: {len(data):,} rows")

            all_records.extend(data)
            time.sleep(1.5)

    if not all_records:
        print("\n✗ ERROR: No data returned!")
        return None

    print("\nMerging all crop + metric records...")
    df = pd.DataFrame(all_records)

    # Normalize numeric fields
    if "Value" in df.columns:
        df["Value"] = df["Value"].str.replace(",", "").astype(float, errors="ignore")

    # Save final dataset
    df.to_csv(output, index=False)
    print(f"\n==========================================================")
    print(f"SUCCESS! Saved giant dataset → {output}")
    print(f"Total rows: {len(df):,}")
    print(f"Years: {df['year'].min()} - {df['year'].max()}")
    print(f"Unique Crops: {df['commodity_desc'].nunique()}")
    print(f"Unique Counties: {df['county_name'].nunique()}")
    print("==========================================================\n")

    return output


# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
if __name__ == "__main__":
    download_major_crop_dataset(start_year=1980, end_year=2025)
