#!/usr/bin/env python3
import pandas as pd
import time
import requests
from pathlib import Path

class USDAQuickStats:
    def __init__(self, api_key=None):
        self.base = "https://quickstats.nass.usda.gov/api/get_param_values"
        self.api = "https://quickstats.nass.usda.gov/api/api_GET/"
        self.key = api_key or "DEMO_KEY"   # replace with your real USDA API key if possible

    def query(self, params):
        params["key"] = self.key
        try:
            r = requests.get(self.api, params=params, timeout=40)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            print("Error:", e)
            return []

MAJOR_CROPS = [
    "CORN","SOYBEANS","WHEAT","COTTON","RICE","BARLEY","SORGHUM","PEANUTS","SUGARCANE","SUGARBEETS","OATS","SUNFLOWER"
]

METRICS = {
    "YIELD": {"statisticcat_desc":"YIELD","unit_desc":"BU / ACRE"},
    "AREA_PLANTED": {"statisticcat_desc":"AREA PLANTED"}
}

def download_major_crop_dataset(output="data/raw/us_major_crops_yield_and_acres.csv", start_year=1980, end_year=2025):
    usda = USDAQuickStats()
    all_records = []

    Path("data/raw").mkdir(exist_ok=True)

    for crop in MAJOR_CROPS:
        for metric, settings in METRICS.items():
            params = {
                "source_desc":"SURVEY",
                "sector_desc":"CROPS",
                "group_desc":"FIELD CROPS",
                "commodity_desc": crop,
                "agg_level_desc":"COUNTY",
                "year__GE": start_year,
                "year__LE": end_year
            }
            params.update(settings)
            print(f"Fetching {crop} — {metric}")
            data = usda.query(params)
            all_records.extend(data)
            time.sleep(1.5)

    if not all_records:
        print("✗ ERROR: No data returned!")
        return

    df = pd.DataFrame(all_records)
    if "Value" in df.columns:
        df["Value"] = df["Value"].str.replace(",", "").astype(float, errors="ignore")

    df.to_csv(output, index=False)
    print(f"✓ Saved dataset → {output} ({len(df):,} rows)")

if __name__ == "__main__":
    download_major_crop_dataset()
