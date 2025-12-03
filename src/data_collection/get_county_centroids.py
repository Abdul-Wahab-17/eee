#!/usr/bin/env python3
"""
Download US County Geographic Centroids
========================================

Downloads county boundaries from US Census TIGER/Line and extracts centroids.
This script is INDEPENDENT and doesn't require any crop data.

Output: data/raw/county_centroids.csv
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path

print("="*70)
print("DOWNLOADING COUNTY CENTROIDS")
print("="*70)

# Create output directory
DATA_DIR = Path("data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_DIR / "county_centroids.csv"
CENSUS_URL = "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip"

# Download and load US county shapefile
print("\n[1/2] Downloading US county boundaries (Census TIGER 2020)...")
print(f"  URL: {CENSUS_URL}")

try:
    counties_gdf = gpd.read_file(CENSUS_URL)
    print(f"  ✓ Loaded {len(counties_gdf):,} counties from TIGER/Line")
except Exception as e:
    print(f"  ✗ ERROR loading shapefile: {e}")
    exit(1)

# Ensure FIPS codes are zero-padded
counties_gdf["STATEFP"] = counties_gdf["STATEFP"].astype(str).str.zfill(2)
counties_gdf["COUNTYFP"] = counties_gdf["COUNTYFP"].astype(str).str.zfill(3)

# Calculate centroids
print("\n[2/2] Computing county centroids...")

counties_gdf["centroid"] = counties_gdf.geometry.centroid
counties_gdf["Latitude"] = counties_gdf["centroid"].y
counties_gdf["Longitude"] = counties_gdf["centroid"].x

# Create output dataframe
centroids = counties_gdf[[
    "STATEFP", "COUNTYFP", "NAME", "STUSPS", 
    "Latitude", "Longitude"
]].copy()

centroids.rename(columns={
    "STATEFP": "State_FIPS",
    "COUNTYFP": "County_FIPS",
    "NAME": "County",
    "STUSPS": "State"
}, inplace=True)

# Save output
centroids.to_csv(OUTPUT_FILE, index=False)

print(f"\n  ✓ Saved: {OUTPUT_FILE}")
print(f"  ✓ Total counties: {len(centroids):,}")

# Summary
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"Total US counties: {len(centroids):,}")
print(f"\nCoordinate ranges:")
print(f"  Latitude:  {centroids['Latitude'].min():.2f} to {centroids['Latitude'].max():.2f}")
print(f"  Longitude: {centroids['Longitude'].min():.2f} to {centroids['Longitude'].max():.2f}")

print("\nSample:")
print(centroids.head(5).to_string(index=False))

print("\n" + "="*70)
print("✓ COUNTY CENTROIDS DOWNLOAD COMPLETE")
print("="*70)
print()

def main():
    """Main function for compatibility with download script."""
    pass

if __name__ == "__main__":
    main()
