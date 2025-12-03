@echo off
REM Complete Data Collection and Model Training Pipeline
REM This script downloads ALL data and trains models

echo ============================================================
echo MULTI-CROP YIELD PREDICTION - COMPLETE PIPELINE
echo ============================================================
echo.
echo This will download data for all 12 crops and train models.
echo.
echo IMPORTANT: Install dependencies first if you haven't:
echo    python -m pip install -r requirements.txt
echo    python -m pip install seaborn matplotlib python-dotenv
echo.
pause

echo.
echo ============================================================
echo STEP 1: DOWNLOADING CROP YIELD DATA (4-6 hours)
echo ============================================================
echo.
python src/data_collection/get_yield_by_county.py

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Yield data download failed
    pause
    exit /b 1
)

echo.
echo ============================================================
echo STEP 2: DOWNLOADING AREA PLANTED DATA (2-3 hours)
echo ============================================================
echo.
python src/data_collection/get_area_planted_by_county.py

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Area planted download failed
    pause
    exit /b 1
)

echo.
echo ============================================================
echo STEP 3: DOWNLOADING AREA HARVESTED DATA (2-3 hours)
echo ============================================================
echo.
python src/data_collection/get_area_harvested_by_county.py

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Area harvested download failed
    pause
    exit /b 1
)

echo.
echo ============================================================
echo STEP 4: DOWNLOADING PRODUCTION DATA (2-3 hours)
echo ============================================================
echo.
python src/data_collection/get_production_by_county.py

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Production data download failed
    pause
    exit /b 1
)

echo.
echo ============================================================
echo STEP 5: DOWNLOADING COUNTY CENTROIDS (5 minutes)
echo ============================================================
echo.
python src/data_collection/get_county_centroids.py

echo.
echo ============================================================
echo STEP 6: DOWNLOADING WEATHER DATA (2-3 hours or skip if done)
echo ============================================================
echo.
python src/data_collection/get_weather_data.py

echo.
echo ============================================================
echo STEP 7: DOWNLOADING SOIL DATA (30-45 minutes)
echo ============================================================
echo.
python src/data_collection/get_soil_data.py

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Soil data download failed
    pause
    exit /b 1
)

echo.
echo ============================================================
echo DATA DOWNLOAD COMPLETE! Now processing...
echo ============================================================
echo.

echo NOTE: The data collection scripts automatically merge files.
echo If you see merged output files, you're good to go!
echo.
pause

echo.
echo ============================================================
echo PROCESSING AND TRAINING...
echo ============================================================
echo.
python scripts/02_prepare_data.py

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Data preparation failed
    pause
    exit /b 1
)

echo.
echo ============================================================
echo TRAINING MODELS...
echo ============================================================
echo.
python scripts/03_train_models.py

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Model training failed
    pause
    exit /b 1
)

echo.
echo ============================================================
echo EVALUATING MODELS...
echo ============================================================
echo.
python scripts/04_evaluate_models.py

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Model evaluation failed
    pause
    exit /b 1
)

echo.
echo ============================================================
echo SUCCESS! EVERYTHING COMPLETE!
echo ============================================================
echo.
echo Your models are trained and ready!
echo.
echo To launch the dashboard:
echo     streamlit run app/streamlit_app.py
echo.
echo Total time taken: 12-16 hours
echo.
pause
