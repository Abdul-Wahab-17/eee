import requests
import pandas as pd
import time

class USDAQuickStats:
    def __init__(self, api_key=None):
        self.api = "https://quickstats.nass.usda.gov/api/api_GET/"
        self.key = api_key or "760A18DD-29A5-3562-814A-79B45D1A70EB"

    def query(self, params):
        params["key"] = self.key
        try:
            r = requests.get(self.api, params=params, timeout=40)
            r.raise_for_status()
            return r.json().get("data", [])
        except Exception as e:
            print("Error:", e)
            return []


# -------------------------------------------------------------
# Major Crops (Unified Classes — Option B)
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


def download_area_harvested_data(start_year=1980, end_year=2025,
                                 output="us_major_crops_area_harvested.csv"):

    usda = USDAQuickStats()
    all_records = []

    print("\n=======================================================")
    print("  DOWNLOADING AREA HARVESTED FOR ALL MAJOR CROPS")
    print("=======================================================\n")

    for crop in MAJOR_CROPS:

        params = {
            "source_desc": "SURVEY",
            "sector_desc": "CROPS",
            "group_desc": "FIELD CROPS",
            "commodity_desc": crop,
            "statisticcat_desc": "AREA HARVESTED",
            "agg_level_desc": "COUNTY",
            "year__GE": start_year,
            "year__LE": end_year
        }

        print(f"\n>>> Fetching {crop} - AREA HARVESTED...")
        data = usda.query(params)

        print(f"    ✓ Retrieved: {len(data):,} records")

        all_records.extend(data)
        time.sleep(1.0)   # small delay to avoid throttling

    if not all_records:
        print("\n✗ ERROR: No data returned")
        return None

    print("\nMerging all crops...")

    df = pd.DataFrame(all_records)

    # Clean numeric values
    if "Value" in df.columns:
        df["Value"] = df["Value"].str.replace(",", "").astype(float, errors="ignore")

    # Save final giant dataset
    df.to_csv(output, index=False)

    print("\n=======================================================")
    print(f"SUCCESS! Saved: {output}")
    print(f"Total rows: {len(df):,}")
    print(f"Years: {df['year'].min()} - {df['year'].max()}")
    print(f"Crops: {df['commodity_desc'].nunique()}")
    print(f"Counties: {df['county_name'].nunique()}")
    print("=======================================================\n")

    return output


if __name__ == "__main__":
    download_area_harvested_data(start_year=1980, end_year=2025)
