#!/usr/bin/env python3
"""
get_yield_by_county.py

Collect major-crop county-level yield (bu/acre) and acres planted from USDA NASS QuickStats,
for a unified "ALL classes" approach (Option B).

Output: single merged CSV:
  data/processed/us_major_crops_county_all_years_major_crops.csv

Author: adapted for Roger
"""

import requests
import pandas as pd
import time
import json
import os
from pathlib import Path
from typing import Optional, List

# ----------------------------
# Configuration
# ----------------------------
DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://quickstats.nass.usda.gov"
UUID_ENCODE_ENDPOINT = f"{BASE_URL}/uuid/encode"
CSV_SPREADSHEET_URL = f"{BASE_URL}/data/spreadsheet"

# Major crops (Option B: unified "ALL classes" where relevant)
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

# Default years to fetch (adjust as needed)
START_YEAR = 1866
END_YEAR = 2026

# HTTP session
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (compatible; DataCollector/1.0)',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': BASE_URL,
    'Referer': f'{BASE_URL}/'
})

# Polite delay between requests
DELAY_BETWEEN_REQUESTS = 1.0
# Retries
MAX_TRIES = 3
RETRY_DELAY = 2.0

# Short description templates to try per metric.
# These are common QuickStats short_desc patterns; we attempt them in order.
SHORT_DESC_TEMPLATES = {
    "yield": [
        "{crop}, GRAIN - YIELD, MEASURED IN BU / ACRE",
        "{crop} - YIELD, MEASURED IN BU / ACRE",
        "{crop}, YIELD, MEASURED IN BU / ACRE",
        "{crop}, YIELD, MEASURED IN BU/ACRE",
        "{crop} - YIELD",
        "YIELD"
    ],
    "acres_planted": [
        "ACRES PLANTED",
        "{crop} - ACRES PLANTED",
        "{crop}, ACRES PLANTED",
        "{crop} - ACRES PLANTED, MEASURED IN ACRES",
        "{crop}, ACRES PLANTED, MEASURED IN ACRES",
    ]
}

# Base params that don't vary per-crop/metric
BASE_PARAMS_COMMON = {
    'source_desc': 'SURVEY',
    'sector_desc': 'CROPS',
    'group_desc': 'FIELD CROPS',
    'agg_level_desc': 'COUNTY'
}

# Output merged filename
MERGED_OUTPUT = PROCESSED_DIR / "us_major_crops_county_all_years_major_crops.csv"


# ----------------------------
# Utility functions
# ----------------------------
def try_post_uuid(form_data: List[tuple]) -> Optional[str]:
    """Post to UUID encode endpoint using form-encoded (breadcrumb) data and return uuid string."""
    for attempt in range(1, MAX_TRIES + 1):
        try:
            resp = session.post(UUID_ENCODE_ENDPOINT, data=form_data, timeout=30)
            resp.raise_for_status()
            # server returns uuid as a raw string or quoted string
            uuid = resp.text.strip().strip('"')
            if uuid:
                return uuid
        except Exception as e:
            print(f"  → UUID request failed (attempt {attempt}/{MAX_TRIES}): {e}")
            time.sleep(RETRY_DELAY)
    return None


def download_csv_for_uuid(uuid: str, out_path: Path) -> bool:
    csv_url = f"{CSV_SPREADSHEET_URL}/{uuid}.csv"
    for attempt in range(1, MAX_TRIES + 1):
        try:
            r = session.get(csv_url, timeout=60)
            r.raise_for_status()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(r.content)
            return True
        except Exception as e:
            print(f"  → Download failed (attempt {attempt}/{MAX_TRIES}): {e}")
            time.sleep(RETRY_DELAY)
    return False


def build_breadcrumb_form(params: dict, breadcrumb_order: List[str]) -> List[tuple]:
    """
    Build the form-encoded data with multiple 'breadcrumb' entries so QuickStats encodes properly.
    This mirrors how the original script built the breadcrumb list before.
    """
    form = []
    for k, v in params.items():
        form.append((k, v))
    for bc in breadcrumb_order:
        form.append(("breadcrumb", bc))
    return form


def guess_breadcrumb_order(params_keys: List[str]) -> List[str]:
    """A reasonable breadcrumb order used by the original script (keeps it stable)."""
    order = []
    for key in ['source_desc', 'sector_desc', 'group_desc', 'commodity_desc', 'short_desc', 'agg_level_desc', 'year']:
        if key in params_keys:
            order.append(key)
    return order


def sanitize_crop_name(crop: str) -> str:
    """Used for filenames and readable crop column values."""
    return crop.strip().upper().replace(" ", "_").replace(",", "")


# ----------------------------
# Main download workflow
# ----------------------------
def fetch_for_crop_metric(crop: str, metric_key: str, year: int) -> Optional[Path]:
    """
    Attempt to query for a given crop + metric (e.g., yield or acres_planted) and year.
    Tries multiple short_desc templates until one returns a valid UUID & CSV.
    Returns path to downloaded CSV or None on failure.
    """
    crop_display = crop  # commodity_desc passed to API
    crop_tag = sanitize_crop_name(crop)
    metric_name = "yield" if metric_key == "yield" else "acres_planted"

    tried_variants = []
    for template in SHORT_DESC_TEMPLATES[metric_key]:
        short_desc = template.format(crop=crop_display)
        params = BASE_PARAMS_COMMON.copy()
        params.update({
            'commodity_desc': crop_display,
            'short_desc': short_desc,
            'year': str(year),
        })

        tried_variants.append(short_desc)
        breadcrumb = guess_breadcrumb_order(list(params.keys()))
        form = build_breadcrumb_form(params, breadcrumb)

        print(f"  Trying short_desc: '{short_desc}'")
        uuid = try_post_uuid(form)
        time.sleep(DELAY_BETWEEN_REQUESTS)

        if not uuid:
            print("   ✗ No UUID returned for this short_desc.")
            continue

        # Build output path
        out_filename = f"{crop_tag}__{metric_name}__{year}__{uuid}.csv"
        out_path = RAW_DIR / out_filename

        if out_path.exists():
            print(f"   ✓ File already exists: {out_path.name} (skipping download)")
            return out_path

        print(f"   ✓ UUID obtained: {uuid} → downloading CSV...")
        success = download_csv_for_uuid(uuid, out_path)
        time.sleep(DELAY_BETWEEN_REQUESTS)
        if success:
            print(f"   ✓ Saved: {out_path}")
            return out_path
        else:
            print("   ✗ Download unsuccessful for this UUID — trying next pattern.")
            continue

    print(f"  ✗ All short_desc patterns failed for {crop} / {metric_key} (year {year}). Tried: {tried_variants}")
    return None


def gather_all(crops: List[str], start_year: int, end_year: int) -> List[Path]:
    all_files = []
    years = list(range(start_year, end_year + 1))
    total = len(crops) * len(years) * 2  # two metrics per crop per year
    counter = 0
    print(f"Starting fetch for {len(crops)} crops × {len(years)} years × 2 metrics = ~{total} files (best-effort).")
    for crop in crops:
        for year in years:
            for metric in ["yield", "acres_planted"]:
                counter += 1
                print(f"\n[{counter}/{total}] {crop} | {year} | {metric}")
                try:
                    csv_path = fetch_for_crop_metric(crop, metric, year)
                    if csv_path:
                        all_files.append(csv_path)
                except KeyboardInterrupt:
                    print("\nInterrupted by user — exiting.")
                    return all_files
                except Exception as e:
                    print(f"  Unexpected error: {e}")
                    continue
    return all_files


# ----------------------------
# Merge function
# ----------------------------
def merge_raw_files(file_paths: List[Path], output_file: Path) -> Optional[Path]:
    """
    Merge downloaded CSVs into a single giant dataset.
    Attempt to detect whether a file contains yield or acres planted based on short_desc / column values.
    Output columns:
      - State, County, Year, Commodity (Crop), Yield_BuPerAcre, Acres_Planted, Value, CV, Unit, ...
    """
    if not file_paths:
        print("No files to merge.")
        return None

    print("\nMerging CSV files...")
    dfs = []
    for p in file_paths:
        try:
            df = pd.read_csv(p, dtype=str)
        except Exception as e:
            print(f"  ✗ Failed to read {p.name}: {e}")
            continue

        # Normalize columns (many QuickStats files use 'Value' and 'short_desc', 'Year')
        # Keep original columns but enforce lowercase column names for detection convenience
        cols = {c: c for c in df.columns}
        df.columns = [c.strip() for c in df.columns]

        # Add a helper column 'DetectedMetric'
        detected_metric = None
        # If 'short_desc' exists, try to detect
        if 'short_desc' in df.columns:
            sample_desc = str(df['short_desc'].iloc[0]).upper()
            if 'YIELD' in sample_desc:
                detected_metric = 'yield'
            elif 'ACRES' in sample_desc and 'PLANTED' in sample_desc:
                detected_metric = 'acres_planted'

        # If not detect, try to infer from filename
        if not detected_metric:
            fname = p.name.upper()
            if "__YIELD__" in fname or "YIELD" in fname:
                detected_metric = 'yield'
            elif "ACRES" in fname or "ACRES_PLANTED" in fname or "PLANTED" in fname:
                detected_metric = 'acres_planted'

        # Standardize minimal cols if available
        # QuickStats typical columns: 'Year', 'State', 'State ANSI', 'County', 'County ANSI', 'Value', 'CV (%)', 'short_desc'
        for expected in ['Year', 'State', 'State ANSI', 'County', 'County ANSI', 'Value', 'CV (%)', 'short_desc', 'commodity_desc', 'Unit']:
            if expected not in df.columns:
                df[expected] = None

        # Keep detected metric
        df['DetectedMetric'] = detected_metric
        dfs.append(df)

    if not dfs:
        print("No readable CSVs to merge.")
        return None

    big = pd.concat(dfs, ignore_index=True, sort=False)

    # Normalize Year column
    if 'Year' in big.columns:
        big['Year'] = pd.to_numeric(big['Year'], errors='coerce').astype('Int64')

    # Use commodity_desc if present, else try to parse from 'short_desc' or filename
    def detect_commodity(row):
        if pd.notna(row.get('commodity_desc')):
            return row['commodity_desc']
        sd = row.get('short_desc', '')
        if isinstance(sd, str) and ',' in sd:
            # often "CORN, GRAIN - YIELD, MEASURED IN ..."
            return sd.split(',')[0]
        # fallback: try to get from filename embedded in index
        return None

    big['Crop'] = big.apply(detect_commodity, axis=1)
    # Fill blank crops with parsed filename parts if still missing
    big.loc[big['Crop'].isnull(), 'Crop'] = big.loc[big['Crop'].isnull(), 'short_desc'].apply(
        lambda s: (s.split(',')[0] if isinstance(s, str) and ',' in s else s)
    )

    # Convert Value to numeric where possible
    big['ValueNumeric'] = pd.to_numeric(big['Value'].str.replace(',', '', regex=False), errors='coerce')

    # Create columns for the two target metrics
    big['Yield_BuPerAcre'] = None
    big['Acres_Planted'] = None

    # Populate them based on DetectedMetric or short_desc detection
    mask_yield = big['DetectedMetric'] == 'yield'
    mask_acres = big['DetectedMetric'] == 'acres_planted'

    # Also expand detection by checking short_desc contains keywords
    mask_yield = mask_yield | big['short_desc'].astype(str).str.upper().str.contains('YIELD', na=False)
    mask_acres = mask_acres | big['short_desc'].astype(str).str.upper().str.contains('ACRES', na=False) & big['short_desc'].astype(str).str.upper().str.contains('PLANTED', na=False)

    big.loc[mask_yield, 'Yield_BuPerAcre'] = big.loc[mask_yield, 'ValueNumeric']
    big.loc[mask_acres, 'Acres_Planted'] = big.loc[mask_acres, 'ValueNumeric']

    # Keep base columns we want and pivot so yield & acres appear on same row
    key_cols = ['State', 'State ANSI', 'County', 'County ANSI', 'Year', 'Crop']

    # Some rows might have missing Crop values: drop them
    big = big[big['Crop'].notnull()].copy()
    big['Crop'] = big['Crop'].astype(str).str.strip()

    # Aggregate by taking first non-null value for numeric columns
    agg_funcs = {
        'Yield_BuPerAcre': lambda x: pd.to_numeric(x, errors='coerce').dropna().iloc[0] if x.dropna().shape[0] > 0 else pd.NA,
        'Acres_Planted': lambda x: pd.to_numeric(x, errors='coerce').dropna().iloc[0] if x.dropna().shape[0] > 0 else pd.NA,
    }

    grouped = big.groupby(key_cols).agg(agg_funcs).reset_index()

    # Add some counts and metadata if desired
    grouped['SourceRowsCount'] = big.groupby(key_cols).size().values

    # Save
    grouped.to_csv(output_file, index=False)
    print(f"\n✓ Merged dataset saved: {output_file}")
    print(f"  Rows: {len(grouped):,}")
    return output_file


# ----------------------------
# Entrypoint
# ----------------------------
def main():
    print("=" * 70)
    print("USDA NASS QuickStats - Major Crops Yield + Acres Planted (County level)")
    print("=" * 70)
    print(f"Crops: {', '.join(CROPS)}")
    print(f"Years: {START_YEAR} - {END_YEAR}")
    print("=" * 70)

    # Gather files (this can be long — you can adjust year range above)
    file_list = gather_all(CROPS, START_YEAR, END_YEAR)

    if not file_list:
        print("No CSVs were downloaded. Exiting.")
        return

    # Merge them into a single giant dataset
    merged_path = merge_raw_files(file_list, MERGED_OUTPUT)
    if merged_path:
        print(f"\nAll done. Merged dataset at: {merged_path}")
    else:
        print("\nMerge failed or no data aggregated.")

if __name__ == "__main__":
    main()
