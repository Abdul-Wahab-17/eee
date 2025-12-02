#!/usr/bin/env python3
"""
STEP 1: Extract county geographic centroids
============================================

This upgraded version supports ALL MAJOR CROPS (not only corn).
It uses the merged USDA dataset and extracts unique counties using FIPS codes.

Input:  us_major_crops_yield_and_acres.csv
Output: county_centroids.csv

Advantages:
- 100% reliable FIPS-based matching (no name mismatches)
- Uses US Census TIGER county boundaries
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path

print("="*70)
print("STEP 1: EXTRACTING COUNTY CENTROIDS")
print("="*70)

# =====================================================================
# CONFIG
# =====================================================================
INPUT_FILE = "us_major_crops_yield_and_acres.csv"
OUTPUT_FILE = "county_centroids.csv"

CENSUS_URL = "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip"

# =====================================================================
# STEP 1 — LOAD DATASET
# =====================================================================
print("\n[1/4] Loading merged major-crop dataset...")

try:
    df = pd.read_csv(INPUT_FILE, dtype=str)
    print(f"  ✓ Loaded {len(df):,} records")
except FileNotFoundError:
    print(f"  ✗ ERROR: {INPUT_FILE} not found!")
    exit(1)

# Extract unique counties using FIPS (most reliable)
required_cols = ["State ANSI", "County ANSI", "State", "County"]

for col in required_cols:
    if col not in df.columns:
        print(f"  ✗ ERROR: Missing required column: {col}")
        exit(1)

df["State ANSI"] = df["State ANSI"].astype(str).str.zfill(2)
df["County ANSI"] = df["County ANSI"].astype(str).str.zfill(3)

unique_counties = df[["State", "County", "State ANSI", "County ANSI"]].drop_duplicates()
print(f"  ✓ Found {len(unique_counties):,} unique counties")

# =====================================================================
# STEP 2 — LOAD US COUNTY SHAPEFILE
# =====================================================================
print("\n[2/4] Downloading US county boundaries (Census TIGER 2020)...")

try:
    counties_gdf = gpd.read_file(CENSUS_URL)
    print(f"  ✓ Loaded {len(counties_gdf):,} counties from TIGER/Line")
except Exception as e:
    print(f"  ✗ ERROR loading shapefile: {e}")
    exit(1)

# Ensure FIPS codes are zero-padded
counties_gdf["STATEFP"] = counties_gdf["STATEFP"].astype(str).str.zfill(2)
counties_gdf["COUNTYFP"] = counties_gdf["COUNTYFP"].astype(str).str.zfill(3)

# =====================================================================
# STEP 3 — CALCULATE CENTROIDS
# =====================================================================
print("\n[3/4] Computing county centroids...")

counties_gdf["centroid"] = counties_gdf.geometry.centroid
counties_gdf["Latitude"] = counties_gdf["centroid"].y
counties_gdf["Longitude"] = counties_gdf["centroid"].x

centroids = counties_gdf[["STATEFP", "COUNTYFP", "Latitude", "Longitude"]].copy()
centroids.rename(columns={
    "STATEFP": "State ANSI",
    "COUNTYFP": "County ANSI"
}, inplace=True)

# =====================================================================
# STEP 4 — MERGE FIPS → CENTROIDS
# =====================================================================
print("\n[4/4] Matching counties using FIPS codes (100% reliable)...")

merged = unique_counties.merge(
    centroids,
    on=["State ANSI", "County ANSI"],
    how="left"
)

missing = merged["Latitude"].isna().sum()

if missing > 0:
    print(f"  ⚠ WARNING: {missing} counties missing centroid (likely territories)")
    print("    → Dropping missing entries")
    merged = merged.dropna(subset=["Latitude", "Longitude"])

# Save output
merged.to_csv(OUTPUT_FILE, index=False)

print(f"\n  ✓ Saved: {OUTPUT_FILE}")
print(f"  ✓ Total counties with valid coordinates: {len(merged):,}")

# =====================================================================
# SUMMARY
# =====================================================================
print("\n" + "="*70)
print("VALIDATION SUMMARY")
print("="*70)
print(f"Input counties:  {len(unique_counties):,}")
print(f"Output counties: {len(merged):,}")
print(f"Coverage: {len(merged) / len(unique_counties) * 100:.1f}%")

print("\nCoordinate ranges:")
print(f"  Latitude:  {merged['Latitude'].astype(float).min():.2f} to {merged['Latitude'].astype(float).max():.2f}")
print(f"  Longitude: {merged['Longitude'].astype(float).min():.2f} to {merged['Longitude'].astype(float).max():.2f}")

print("\nSample:")
print(merged.head(5).to_string(index=False))

print("\n" + "="*70)
print("✓ STEP 1 COMPLETE — COUNTY CENTROIDS READY")
print("="*70)
print("\nNext: Run weather download script.\n")
