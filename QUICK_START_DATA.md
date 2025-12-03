# Quick Start: Data Collection for All Crops

## ✅ GOOD NEWS: No API Keys Required!

The data collection scripts use **web scraping** and **public APIs** that don't require authentication. You can start downloading data immediately!

## Step-by-Step Instructions

### 1. Run the Setup Script (Already Done!)
```bash
python setup_data_collection.py
```

This creates all necessary directories:
- `data/raw/` - For downloaded raw data
- `data/processed/` - For processed datasets
- `results/figures/` - For visualizations
- `results/tables/` - For result tables
- `models/` - For trained models

### 2. Download All Data
```bash
python scripts/01_download_all_data.py
```

This will download data for **all 12 crops**:
- CORN
- SOYBEANS
- WHEAT
- COTTON
- RICE
- BARLEY
- SORGHUM
- PEANUTS
- SUGARCANE
- SUGARBEETS
- OATS
- SUNFLOWER

### What Gets Downloaded:

#### From USDA NASS QuickStats (Web Scraping - No Key Needed)
- ✅ Yield data (BU/ACRE) for each crop
- ✅ Area planted (ACRES)
- ✅ Area harvested (ACRES)
- ✅ Production (BU)
- 📅 Years: 1866-2026 (actual data availability varies by crop)
- 🗺️ Coverage: All US counties with data

#### From NASA POWER (Public API - No Key Needed)
- ✅ Temperature (min, max, mean)
- ✅ Precipitation
- ✅ Relative humidity
- 📅 Years: 1981-2023
- 🗺️ Coverage: All county centroids

#### From USDA NRCS (Public API - No Key Needed)
- ✅ Soil properties (AWC, clay%, pH, organic matter)
- 🗺️ Coverage: ~2,500 counties

#### From US Census (Public Data - No Key Needed)
- ✅ County geographic centroids (lat/lon)
- 🗺️ Coverage: All 3,143 US counties

## Expected Download Time

| Data Source | Time | Size |
|------------|------|------|
| USDA NASS (all 12 crops) | 4-6 hours | ~1.5 GB |
| NASA POWER Weather | 2-3 hours | ~800 MB |
| USDA NRCS Soil | 30-45 min | ~50 MB |
| US Census Centroids | 5 min | ~10 MB |
| **TOTAL** | **6-10 hours** | **~2.5 GB** |

💡 **Tip**: Run overnight or during a time you won't need your computer.

## After Download Complete

### 3. Prepare the Data
```bash
python scripts/02_prepare_data.py
```

This merges and processes all downloaded data.

### 4. Train Models
```bash
python scripts/03_train_models.py
```

Trains prediction models for your crops.

### 5. Evaluate Models
```bash
python scripts/04_evaluate_models.py
```

Generates performance metrics and visualizations.

### 6. Launch Dashboard
```bash
streamlit run app/streamlit_app.py
```

Interactive web dashboard for exploring predictions.

## Troubleshooting

### If Download Fails:
- **Check internet connection**
- **The script will resume** - it skips already downloaded files
- **Run again**: `python scripts/01_download_all_data.py`

### If You Get Rate Limited:
- The scripts include delays to be polite to servers
- If you still get errors, increase `DELAY_BETWEEN_REQUESTS` in the scripts

### Missing Dependencies:
```bash
pip install -r requirements.txt
```

## Data Sources (No Registration Needed!)

1. **USDA NASS QuickStats**: https://quickstats.nass.usda.gov/
2. **NASA POWER**: https://power.larc.nasa.gov/
3. **USDA NRCS Soil Data**: https://sdmdataaccess.sc.egov.usda.gov/
4. **US Census TIGER/Line**: https://www.census.gov/

All are **public data** with **no API keys required**! 🎉

## Need Help?

Check the detailed documentation:
- `README.md` - Project overview
- `SETUP.md` - Installation guide
- `docs/data_sources.md` - Data source details
- `API_KEYS_GUIDE.md` - API information (spoiler: none needed!)
