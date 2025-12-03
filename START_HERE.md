# Quick Start - Fixed Version

## What I Fixed

The `run_full_setup.bat` script now:
1. ✅ Downloads yield data for all 12 crops
2. ✅ Downloads area planted data
3. ✅ Downloads area harvested data
4. ✅ Downloads production data
5. ✅ Downloads county centroids
6. ✅ Downloads weather data (skips if already done)
7. ✅ Downloads soil data
8. ✅ Processes all data
9. ✅ Trains models
10. ✅ Evaluates models

## Before Running

Install all dependencies:
```bash
python -m pip install -r requirements.txt
python -m pip install seaborn matplotlib python-dotenv
```

## Run the Script

Just double-click:
```
run_full_setup.bat
```

Or from command line:
```bash
run_full_setup.bat
```

## What to Expect

**Total time: 12-16 hours**

The script will show progress for each step:
- Step 1: Yield data (4-6 hours)
- Step 2: Area planted (2-3 hours)
- Step 3: Area harvested (2-3 hours)
- Step 4: Production (2-3 hours)
- Step 5: County centroids (5 min)
- Step 6: Weather data (skips if done)
- Step 7: Soil data (30-45 min)
- Processing & training (1-2 hours)

## What Changed

The old script was calling `scripts/01_download_all_data.py` which had a bug.

The new script calls each download script directly:
- `src/data_collection/get_yield_by_county.py`
- `src/data_collection/get_area_planted_by_county.py`
- `src/data_collection/get_area_harvested_by_county.py`
- `src/data_collection/get_production_by_county.py`
- `src/data_collection/get_county_centroids.py`
- `src/data_collection/get_weather_data.py`
- `src/data_collection/get_soil_data.py`

This ensures ALL data gets downloaded!

## After Completion

Launch your app:
```bash
streamlit run app/streamlit_app.py
```

🎉 Done!
