"""
Merge Downloaded Crop Data Files
=================================

This script consolidates all the individual crop/year/metric CSV files
downloaded by get_yield_by_county.py and other data collection scripts
into the merged files that preprocessing expects.

Input: data/raw/CROP__metric__year__UUID.csv files
Output: Merged CSV files for each crop and metric
"""

import pandas as pd
from pathlib import Path
import glob

print("="*70)
print("MERGING DOWNLOADED CROP DATA FILES")
print("="*70)

DATA_DIR = Path("data/raw")

# Define crops and metrics
CROPS = [
    "CORN", "SOYBEANS", "WHEAT", "COTTON", "RICE", "BARLEY",
    "SORGHUM", "PEANUTS", "SUGARCANE", "SUGARBEETS", "OATS", "SUNFLOWER"
]

METRICS = {
    "yield": "us_{crop}_yield_county_all_years.csv",
    "acres_planted": "us_{crop}_area_planted_county_all_years.csv",
    "acres_harvested": "us_{crop}_area_harvested_county_all_years.csv",
    "production": "us_{crop}_production_county_all_years.csv"
}

def merge_crop_metric(crop: str, metric: str) -> bool:
    """Merge all files for a specific crop and metric."""
    
    # Find all matching files
    pattern = f"{crop}__{metric}__*.csv"
    files = list(DATA_DIR.glob(pattern))
    
    if not files:
        print(f"  ⚠ No files found for {crop} {metric}")
        return False
    
    print(f"  Found {len(files)} files for {crop} {metric}")
    
    # Read and merge all files
    dfs = []
    for file in files:
        try:
            df = pd.read_csv(file, dtype=str)
            # Only keep files with actual data (not just headers)
            if len(df) > 0:
                dfs.append(df)
        except Exception as e:
            print(f"    ✗ Error reading {file.name}: {e}")
            continue
    
    if not dfs:
        print(f"  ⚠ No valid data for {crop} {metric}")
        return False
    
    # Concatenate all dataframes
    merged = pd.concat(dfs, ignore_index=True)
    
    # Remove duplicates
    merged = merged.drop_duplicates()
    
    # Save merged file
    output_file = METRICS[metric].format(crop=crop.lower())
    output_path = DATA_DIR / output_file
    merged.to_csv(output_path, index=False)
    
    print(f"  ✓ Saved: {output_file} ({len(merged):,} rows)")
    return True

def main():
    """Merge all downloaded crop data files."""
    
    print("\n[1/4] Merging yield data...")
    print("-" * 70)
    for crop in CROPS:
        merge_crop_metric(crop, "yield")
    
    print("\n[2/4] Merging area planted data...")
    print("-" * 70)
    for crop in CROPS:
        merge_crop_metric(crop, "acres_planted")
    
    print("\n[3/4] Merging area harvested data...")
    print("-" * 70)
    for crop in CROPS:
        merge_crop_metric(crop, "acres_harvested")
    
    print("\n[4/4] Merging production data...")
    print("-" * 70)
    for crop in CROPS:
        merge_crop_metric(crop, "production")
    
    print("\n" + "="*70)
    print("MERGE COMPLETE")
    print("="*70)
    print("\nMerged files saved to: data/raw/")
    print("\nNext: Run 02_prepare_data.py")

if __name__ == "__main__":
    main()
