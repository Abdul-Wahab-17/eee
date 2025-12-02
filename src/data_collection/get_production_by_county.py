#!/usr/bin/env python3
"""
USDA QuickStats – Major Crops: Yield (BU/ACRE) + Acres Planted
County-level | All Years | Single merged dataset

This version replaces the original "corn only" production script.
It follows the same logic as the large unified script previously generated.
"""

import requests
import pandas as pd
import time
import json
from pathlib import Path
from typing import Optional, List

# ================================================================
# CONFIG
# ================================================================

BASE_URL = "https://quickstats.nass.usda.gov"
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CROPS = [
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

START_YEAR = 1900
END_YEAR = 2026

DELAY = 1.0
RETRIES = 3

SHORT_DESC_PATTERNS = {
    "yield": [
        "{crop}, GRAIN - YIELD, MEASURED IN BU / ACRE",
        "{crop} - YIELD, MEASURED IN BU / ACRE",
        "{crop}, YIELD, MEASURED IN BU / ACRE",
        "{crop} - YIELD",
        "YIELD"
    ],
    "acres_planted": [
        "{crop} - ACRES PLANTED",
        "{crop}, ACRES PLANTED",
        "ACRES PLANTED",
    ]
}

# ================================================================
# SCRAPER CLASS
# ================================================================

class USDAQuickStats:
    def __init__(self):
        self.base_url = BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Linux; DataCollector)',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': self.base_url,
            'Referer': f'{self.base_url}/'
        })

    def get_uuid(self, params: dict) -> Optional[str]:
        """
        Sends encoded query with breadcrumbs → returns UUID or None.
        """
        url = f"{self.base_url}/uuid/encode"

        breadcrumb_order = []
        for key in [
            'source_desc', 'sector_desc', 'group_desc',
            'commodity_desc', 'statisticcat_desc', 'short_desc',
            'agg_level_desc', 'year'
        ]:
            if key in params:
                breadcrumb_order.append(key)

        # Build form-data request
        form_data = []
        for k, v in params.items():
            form_data.append((k, v))
        for bc in breadcrumb_order:
            form_data.append(("breadcrumb", bc))

        for attempt in range(1, RETRIES+1):
            try:
                r = self.session.post(url, data=form_data, timeout=30)
                r.raise_for_status()
                uuid = r.text.strip().strip('"')
                if uuid:
                    return uuid
            except Exception as e:
                print(f"UUID error (attempt {attempt}): {e}")
                time.sleep(1)

        return None

    def download_csv(self, uuid: str, out_path: Path) -> bool:
        """
        Download CSV using UUID.
        """
        url = f"{self.base_url}/data/spreadsheet/{uuid}.csv"

        for attempt in range(1, RETRIES + 1):
            try:
                r = self.session.get(url, timeout=60)
                r.raise_for_status()
                with open(out_path, 'wb') as f:
                    f.write(r.content)
                return True
            except Exception as e:
                print(f"Download error (attempt {attempt}): {e}")
                time.sleep(1)
        return False


qs = USDAQuickStats()

# ================================================================
# FETCHING LOGIC
# ================================================================

def fetch_one(crop: str, metric: str, year: int) -> Optional[Path]:
    """
    Attempts all known short_desc patterns for selected metric.
    """

    metric_tag = "yield" if metric == "yield" else "acres_planted"
    out_name = f"{crop}_{metric_tag}_{year}.csv".replace(" ", "_")
    out_file = RAW_DIR / out_name

    # Skip if already downloaded
    if out_file.exists():
        print(f"  ✓ Already exists: {out_file.name}")
        return out_file

    for pattern in SHORT_DESC_PATTERNS[metric]:
        short_desc = pattern.format(crop=crop)

        params = {
            'source_desc': 'SURVEY',
            'sector_desc': 'CROPS',
            'group_desc': 'FIELD CROPS',
            'commodity_desc': crop,
            'statisticcat_desc': 'YIELD' if metric == "yield" else "AREA PLANTED",
            'short_desc': short_desc,
            'agg_level_desc': 'COUNTY',
            'year': str(year)
        }

        print(f"    Trying short_desc = {short_desc}")
        uuid = qs.get_uuid(params)

        time.sleep(DELAY)

        if not uuid:
            print("    ✗ No UUID for pattern.")
            continue

        print(f"    UUID → {uuid}")

        success = qs.download_csv(uuid, out_file)
        if success:
            print(f"    ✓ Saved: {out_file.name}")
            return out_file

    print(f"✗ Failed crop={crop}, metric={metric}, year={year}")
    return None


# ================================================================
# MASTER LOOP
# ================================================================

def collect_all():
    files = []
    years = range(START_YEAR, END_YEAR + 1)

    total = len(CROPS) * len(years) * 2
    step = 0

    for crop in CROPS:
        print("\n===================================================")
        print(f"Processing crop: {crop}")
        print("===================================================")

        for year in years:
            for metric in ["yield", "acres_planted"]:
                step += 1
                print(f"\n[{step}/{total}] {crop} {year} {metric}")

                f = fetch_one(crop, metric, year)
                if f:
                    files.append(f)

    return files


# ================================================================
# MERGE
# ================================================================

def merge_all(file_list: List[Path]):
    if not file_list:
        print("No files to merge.")
        return None

    dfs = []
    for f in file_list:
        try:
            df = pd.read_csv(f, dtype=str)
            df["source_file"] = f.name
            dfs.append(df)
        except Exception as e:
            print(f"Could not load {f.name}: {e}")

    merged = pd.concat(dfs, ignore_index=True)
    merged_file = PROCESSED_DIR / "us_major_crops_yield_and_acres.csv"
    merged.to_csv(merged_file, index=False)

    print("\n===================================================")
    print("MERGE COMPLETE")
    print("===================================================")
    print(f"Saved: {merged_file}")
    print(f"Rows: {len(merged):,}")
    print("===================================================")

    return merged_file


# ================================================================
# MAIN
# ================================================================

def main():
    print("===================================================")
    print("USDA Major Crops — Yield + Acres Planted (County Level)")
    print("===================================================")

    files = collect_all()
    merge_all(files)

if __name__ == "__main__":
    main()
