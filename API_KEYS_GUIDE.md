# Multi-Crop Data Collection Setup Guide

## Required API Keys

### 1. USDA NASS QuickStats API Key ⭐ **REQUIRED**
- **Website**: https://quickstats.nass.usda.gov/api
- **How to get**:
  1. Visit https://quickstats.nass.usda.gov/api
  2. Click "Request API Key"
  3. Fill out the form with your email
  4. Check your email for the API key
- **Cost**: FREE
- **Used for**: Crop yield, area planted, area harvested, and production data for all 12 crops

### 2. NASA POWER API
- **Website**: https://power.larc.nasa.gov/
- **API Key**: NOT REQUIRED (Public API, no key needed)
- **Used for**: Weather data (temperature, precipitation, humidity)

### 3. USDA NRCS Soil Data Access
- **Website**: https://sdmdataaccess.sc.egov.usda.gov/
- **API Key**: NOT REQUIRED (Public API, no key needed)
- **Used for**: Soil properties (AWC, clay content, pH, organic matter)

### 4. US Census TIGER/Line
- **Website**: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
- **API Key**: NOT REQUIRED (Public data download)
- **Used for**: County geographic centroids

## Summary

**YOU ONLY NEED 1 API KEY**: USDA NASS QuickStats

The other data sources are publicly accessible without authentication.

## Setup Instructions

1. **Get your USDA NASS API key** from https://quickstats.nass.usda.gov/api

2. **Create a `.env` file** in your project root:
   ```
   USDA_NASS_API_KEY=your_api_key_here
   ```

3. **Run the setup script** (see `setup_data_collection.py`)

4. **Execute data download**:
   ```bash
   python scripts/01_download_all_data.py
   ```

## Data That Will Be Downloaded

For each of the 12 crops (CORN, SOYBEANS, WHEAT, COTTON, RICE, BARLEY, SORGHUM, PEANUTS, SUGARCANE, SUGARBEETS, OATS, SUNFLOWER):

1. **Yield data** (BU/ACRE) - County level, 1981-2023
2. **Area planted** (ACRES) - County level, 1981-2023
3. **Area harvested** (ACRES) - County level, 1981-2023
4. **Production** (BU) - County level, 1981-2023

Plus:
5. **Weather data** - Daily temperature, precipitation, humidity for all counties
6. **Soil data** - AWC, clay%, pH, organic matter for all counties
7. **County centroids** - Lat/lon coordinates for spatial queries

## Estimated Download Time

- **USDA NASS data** (all crops): 4-6 hours
- **Weather data**: 2-3 hours
- **Soil data**: 30-45 minutes
- **County centroids**: 5 minutes

**Total**: ~6-10 hours (can run overnight)

## Storage Requirements

- **Raw data**: ~2-3 GB
- **Processed data**: ~500 MB
- **Total**: ~3-4 GB free space needed
