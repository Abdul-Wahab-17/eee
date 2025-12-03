"""
Quick fix script to update all data collection file paths to use data/raw directory.
Run this once to fix all path issues.
"""

import os
from pathlib import Path

# Files to fix and their path replacements
fixes = {
    'src/data_collection/get_weather_data.py': [
        ("INPUT_FILE = 'county_centroids.csv'", "INPUT_FILE = 'data/raw/county_centroids.csv'"),
        ("OUTPUT_FILE = 'weather_data_weekly.csv'", "OUTPUT_FILE = 'data/raw/weather_data_weekly.csv'"),
        ("CHECKPOINT_FILE = 'weekly_weather_checkpoint.csv'", "CHECKPOINT_FILE = 'data/raw/weekly_weather_checkpoint.csv'"),
    ],
    'src/data_collection/get_soil_data.py': [
        ("yield_file = self.output_dir / \"us_major_crops_county_all_years_major_crops.csv\"", 
         "yield_file = Path(\"data/raw\") / \"county_centroids.csv\""),
    ]
}

print("Fixing file paths in data collection scripts...")
print("="*70)

for filepath, replacements in fixes.items():
    print(f"\nFixing: {filepath}")
    
    if not os.path.exists(filepath):
        print(f"  ✗ File not found, skipping")
        continue
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    modified = False
    for old, new in replacements:
        if old in content:
            content = content.replace(old, new)
            modified = True
            print(f"  ✓ Replaced: {old[:50]}...")
    
    if modified:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✓ File updated")
    else:
        print(f"  ℹ No changes needed")

print("\n" + "="*70)
print("✓ All paths fixed!")
print("\nYou can now run: run_full_setup.bat")
