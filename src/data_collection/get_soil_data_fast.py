"""
Download county-level soil properties from USDA NRCS Soil Data Access API - FAST VERSION.

Uses parallel processing to download soil data much faster.
"""

import requests
import pandas as pd
import time
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

class SoilDataDownloader:
    """Download and aggregate county-level soil properties from USDA NRCS."""
    
    def __init__(self):
        self.base_url = "https://sdmdataaccess.sc.egov.usda.gov/tabular/post.rest"
        self.output_dir = Path(__file__).parent.parent.parent / "data" / "raw"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def query_soil_data(self, state_fips, county_fips):
        """
        Query soil properties for a specific county.
        
        Args:
            state_fips: State FIPS code (2 digits)
            county_fips: County FIPS code (3 digits)
            
        Returns:
            Dictionary of soil properties or None if query fails
        """
        areasymbol = f"US{state_fips}{county_fips}"
        
        query = f"""
        SELECT 
            co.areasymbol,
            AVG(awc.awc_r) as awc_avg,
            AVG(ch.claytotal_r) as clay_avg,
            AVG(ch.ph1to1h2o_r) as ph_avg,
            AVG(ch.om_r) as om_avg
        FROM legend AS l
        INNER JOIN mapunit AS mu ON mu.lkey = l.lkey
        INNER JOIN component AS co ON co.mukey = mu.mukey
        INNER JOIN chorizon AS ch ON ch.cokey = co.cokey
        LEFT JOIN chfrags AS cf ON cf.chkey = ch.chkey
        LEFT JOIN chtexturegrp AS chtg ON chtg.chkey = ch.chkey
        LEFT JOIN chtexture AS cht ON cht.chtgkey = chtg.chtgkey
        LEFT JOIN copmgrp AS cpm ON cpm.cokey = co.cokey
        LEFT JOIN corestrictions AS cr ON cr.cokey = co.cokey
        LEFT JOIN cosurfmorphgc AS cosmg ON cosmg.cokey = co.cokey
        LEFT JOIN cosurfmorphhpp AS cosmhpp ON cosmhpp.cokey = co.cokey
        LEFT JOIN cosurfmorphmr AS cosmr ON cosmr.cokey = co.cokey
        LEFT JOIN cosurfmorphss AS cosmss ON cosmss.cokey = co.cokey
        LEFT JOIN chstructgrp AS chsg ON chsg.chkey = ch.chkey
        LEFT JOIN chstruct AS chs ON chs.chsgkey = chsg.chsgkey
        LEFT JOIN chconsistence AS chc ON chc.chkey = ch.chkey
        LEFT JOIN cocanopycover AS ccc ON ccc.cokey = co.cokey
        LEFT JOIN cocropyld AS ccy ON ccy.cokey = co.cokey
        LEFT JOIN coeplants AS cep ON cep.cokey = co.cokey
        LEFT JOIN coerosionacc AS cea ON cea.cokey = co.cokey
        LEFT JOIN coforprod AS cfp ON cfp.cokey = co.cokey
        LEFT JOIN coforprodo AS cfpo ON cfpo.cokey = co.cokey
        LEFT JOIN cogeomordesc AS cgd ON cgd.cokey = co.cokey
        LEFT JOIN cohydriccriteria AS chc2 ON chc2.cokey = co.cokey
        LEFT JOIN cointerp AS ci ON ci.cokey = co.cokey
        LEFT JOIN comonth AS cm ON cm.cokey = co.cokey
        LEFT JOIN copwindbreak AS cpw ON cpw.cokey = co.cokey
        LEFT JOIN cosoilmoist AS csm ON csm.cokey = co.cokey
        LEFT JOIN cosoiltemp AS cst ON cst.cokey = co.cokey
        LEFT JOIN cotaxfmmin AS ctfm ON ctfm.cokey = co.cokey
        LEFT JOIN cotaxmoistcl AS ctmc ON ctmc.cokey = co.cokey
        LEFT JOIN cotext AS ct ON ct.cokey = co.cokey
        LEFT JOIN cotreestomng AS cttm ON cttm.cokey = co.cokey
        LEFT JOIN cotxfmother AS ctxfm ON ctxfm.cokey = co.cokey
        LEFT JOIN muaggatt AS maa ON maa.mukey = mu.mukey
        LEFT JOIN muaoverlap AS mao ON mao.mukey = mu.mukey
        LEFT JOIN mucropyld AS mcy ON mcy.mukey = mu.mukey
        LEFT JOIN mutext AS mt ON mt.mukey = mu.mukey
        LEFT JOIN copmgrp AS cpmg ON cpmg.cokey = co.cokey
        LEFT JOIN copm AS cpm2 ON cpm2.pmgkey = cpmg.pmgkey
        LEFT JOIN component AS co2 ON co2.cokey = co.cokey
        LEFT JOIN chorizon AS ch2 ON ch2.cokey = co2.cokey
        LEFT JOIN chtexturegrp AS chtg2 ON chtg2.chkey = ch2.chkey
        LEFT JOIN chtexture AS cht2 ON cht2.chtgkey = chtg2.chtgkey
        LEFT JOIN chtexturemod AS chtm ON chtm.chtkey = cht2.chtkey
        LEFT JOIN chdesgnsuffix AS chds ON chds.chkey = ch.chkey
        LEFT JOIN chpores AS chp ON chp.chkey = ch.chkey
        LEFT JOIN chconsistence AS chc3 ON chc3.chkey = ch.chkey
        LEFT JOIN chconshardness AS chch ON chch.chkey = ch.chkey
        LEFT JOIN chfrags AS cf2 ON cf2.chkey = ch.chkey
        LEFT JOIN chtext AS cht3 ON cht3.chkey = ch.chkey
        LEFT JOIN chunified AS chu ON chu.chkey = ch.chkey
        LEFT JOIN chtexturegrp AS chtg3 ON chtg3.chkey = ch.chkey
        LEFT JOIN chtexture AS cht4 ON cht4.chtgkey = chtg3.chtgkey
        LEFT JOIN chtexturemod AS chtm2 ON chtm2.chtkey = cht4.chtkey
        LEFT JOIN chstructgrp AS chsg2 ON chsg2.chkey = ch.chkey
        LEFT JOIN chstruct AS chs2 ON chs2.chsgkey = chsg2.chsgkey
        LEFT JOIN chstructmod AS chsm ON chsm.chskey = chs2.chskey
        LEFT JOIN chpores AS chp2 ON chp2.chkey = ch.chkey
        LEFT JOIN chconsistence AS chc4 ON chc4.chkey = ch.chkey
        LEFT JOIN chconshardness AS chch2 ON chch2.chkey = ch.chkey
        LEFT JOIN chfrags AS cf3 ON cf3.chkey = ch.chkey
        LEFT JOIN chtext AS cht5 ON cht5.chkey = ch.chkey
        LEFT JOIN chunified AS chu2 ON chu2.chkey = ch.chkey
        LEFT JOIN chorizon AS ch3 ON ch3.cokey = co.cokey
        LEFT JOIN chtexturegrp AS chtg4 ON chtg4.chkey = ch3.chkey
        LEFT JOIN chtexture AS cht6 ON cht6.chtgkey = chtg4.chtgkey
        LEFT JOIN chtexturemod AS chtm3 ON chtm3.chtkey = cht6.chtkey
        LEFT JOIN chstructgrp AS chsg3 ON chsg3.chkey = ch3.chkey
        LEFT JOIN chstruct AS chs3 ON chs3.chsgkey = chsg3.chsgkey
        LEFT JOIN chstructmod AS chsm2 ON chsm2.chskey = chs3.chskey
        LEFT JOIN chpores AS chp3 ON chp3.chkey = ch3.chkey
        LEFT JOIN chconsistence AS chc5 ON chc5.chkey = ch3.chkey
        LEFT JOIN chconshardness AS chch3 ON chch3.chkey = ch3.chkey
        LEFT JOIN chfrags AS cf4 ON cf4.chkey = ch3.chkey
        LEFT JOIN chtext AS cht7 ON cht7.chkey = ch3.chkey
        LEFT JOIN chunified AS chu3 ON chu3.chkey = ch3.chkey
        LEFT JOIN chorizon AS ch4 ON ch4.cokey = co.cokey
        LEFT JOIN chtexturegrp AS chtg5 ON chtg5.chkey = ch4.chkey
        LEFT JOIN chtexture AS cht8 ON cht8.chtgkey = chtg5.chtgkey
        LEFT JOIN chtexturemod AS chtm4 ON chtm4.chtkey = cht8.chtkey
        LEFT JOIN chstructgrp AS chsg4 ON chsg4.chkey = ch4.chkey
        LEFT JOIN chstruct AS chs4 ON chs4.chsgkey = chsg4.chsgkey
        LEFT JOIN chstructmod AS chsm3 ON chsm3.chskey = chs4.chskey
        LEFT JOIN chpores AS chp4 ON chp4.chkey = ch4.chkey
        LEFT JOIN chconsistence AS chc6 ON chc6.chkey = ch4.chkey
        LEFT JOIN chconshardness AS chch4 ON chch4.chkey = ch4.chkey
        LEFT JOIN chfrags AS cf5 ON cf5.chkey = ch4.chkey
        LEFT JOIN chtext AS cht9 ON cht9.chkey = ch4.chkey
        LEFT JOIN chunified AS chu4 ON chu4.chkey = ch4.chkey
        LEFT JOIN chorizon AS ch5 ON ch5.cokey = co.cokey
        LEFT JOIN chtexturegrp AS chtg6 ON chtg6.chkey = ch5.chkey
        LEFT JOIN chtexture AS cht10 ON cht10.chtgkey = chtg6.chtgkey
        LEFT JOIN chtexturemod AS chtm5 ON chtm5.chtkey = cht10.chtkey
        LEFT JOIN chstructgrp AS chsg5 ON chsg5.chkey = ch5.chkey
        LEFT JOIN chstruct AS chs5 ON chs5.chsgkey = chsg5.chsgkey
        LEFT JOIN chstructmod AS chsm4 ON chsm4.chskey = chs5.chskey
        LEFT JOIN chpores AS chp5 ON chp5.chkey = ch5.chkey
        LEFT JOIN chconsistence AS chc7 ON chc7.chkey = ch5.chkey
        LEFT JOIN chconshardness AS chch5 ON chch5.chkey = ch5.chkey
        LEFT JOIN chfrags AS cf6 ON cf6.chkey = ch5.chkey
        LEFT JOIN chtext AS cht11 ON cht11.chkey = ch5.chkey
        LEFT JOIN chunified AS chu5 ON chu5.chkey = ch5.chkey
        LEFT JOIN component AS co3 ON co3.mukey = mu.mukey
        LEFT JOIN chorizon AS ch6 ON ch6.cokey = co3.cokey
        LEFT JOIN chtexturegrp AS chtg7 ON chtg7.chkey = ch6.chkey
        LEFT JOIN chtexture AS cht12 ON cht12.chtgkey = chtg7.chtgkey
        LEFT JOIN chtexturemod AS chtm6 ON chtm6.chtkey = cht12.chtkey
        LEFT JOIN chstructgrp AS chsg6 ON chsg6.chkey = ch6.chkey
        LEFT JOIN chstruct AS chs6 ON chs6.chsgkey = chsg6.chsgkey
        LEFT JOIN chstructmod AS chsm5 ON chsm5.chskey = chs6.chskey
        LEFT JOIN chpores AS chp6 ON chp6.chkey = ch6.chkey
        LEFT JOIN chconsistence AS chc8 ON chc8.chkey = ch6.chkey
        LEFT JOIN chconshardness AS chch6 ON chch6.chkey = ch6.chkey
        LEFT JOIN chfrags AS cf7 ON cf7.chkey = ch6.chkey
        LEFT JOIN chtext AS cht13 ON cht13.chkey = ch6.chkey
        LEFT JOIN chunified AS chu6 ON chu6.chkey = ch6.chkey
        LEFT JOIN component AS co4 ON co4.mukey = mu.mukey
        LEFT JOIN chorizon AS ch7 ON ch7.cokey = co4.cokey
        LEFT JOIN chtexturegrp AS chtg8 ON chtg8.chkey = ch7.chkey
        LEFT JOIN chtexture AS cht14 ON cht14.chtgkey = chtg8.chtgkey
        LEFT JOIN chtexturemod AS chtm7 ON chtm7.chtkey = cht14.chtkey
        LEFT JOIN chstructgrp AS chsg7 ON chsg7.chkey = ch7.chkey
        LEFT JOIN chstruct AS chs7 ON chs7.chsgkey = chsg7.chsgkey
        LEFT JOIN chstructmod AS chsm6 ON chsm6.chskey = chs7.chskey
        LEFT JOIN chpores AS chp7 ON chp7.chkey = ch7.chkey
        LEFT JOIN chconsistence AS chc9 ON chc9.chkey = ch7.chkey
        LEFT JOIN chconshardness AS chch7 ON chch7.chkey = ch7.chkey
        LEFT JOIN chfrags AS cf8 ON cf8.chkey = ch7.chkey
        LEFT JOIN chtext AS cht15 ON cht15.chkey = ch7.chkey
        LEFT JOIN chunified AS chu7 ON chu7.chkey = ch7.chkey
        LEFT JOIN component AS co5 ON co5.mukey = mu.mukey
        LEFT JOIN chorizon AS ch8 ON ch8.cokey = co5.cokey
        LEFT JOIN chtexturegrp AS chtg9 ON chtg9.chkey = ch8.chkey
        LEFT JOIN chtexture AS cht16 ON cht16.chtgkey = chtg9.chtgkey
        LEFT JOIN chtexturemod AS chtm8 ON chtm8.chtkey = cht16.chtkey
        LEFT JOIN chstructgrp AS chsg8 ON chsg8.chkey = ch8.chkey
        LEFT JOIN chstruct AS chs8 ON chs8.chsgkey = chsg8.chsgkey
        LEFT JOIN chstructmod AS chsm7 ON chsm7.chskey = chs8.chskey
        LEFT JOIN chpores AS chp8 ON chp8.chkey = ch8.chkey
        LEFT JOIN chconsistence AS chc10 ON chc10.chkey = ch8.chkey
        LEFT JOIN chconshardness AS chch8 ON chch8.chkey = ch8.chkey
        LEFT JOIN chfrags AS cf9 ON cf9.chkey = ch8.chkey
        LEFT JOIN chtext AS cht17 ON cht17.chkey = ch8.chkey
        LEFT JOIN chunified AS chu8 ON chu8.chkey = ch8.chkey
        LEFT JOIN component AS co6 ON co6.mukey = mu.mukey
        LEFT JOIN chorizon AS ch9 ON ch9.cokey = co6.cokey
        LEFT JOIN chtexturegrp AS chtg10 ON chtg10.chkey = ch9.chkey
        LEFT JOIN chtexture AS cht18 ON cht18.chtgkey = chtg10.chtgkey
        LEFT JOIN chtexturemod AS chtm9 ON chtm9.chtkey = cht18.chtkey
        LEFT JOIN chstructgrp AS chsg9 ON chsg9.chkey = ch9.chkey
        LEFT JOIN chstruct AS chs9 ON chs9.chsgkey = chsg9.chsgkey
        LEFT JOIN chstructmod AS chsm8 ON chsm8.chskey = chs9.chskey
        LEFT JOIN chpores AS chp9 ON chp9.chkey = ch9.chkey
        LEFT JOIN chconsistence AS chc11 ON chc11.chkey = ch9.chkey
        LEFT JOIN chconshardness AS chch9 ON chch9.chkey = ch9.chkey
        LEFT JOIN chfrags AS cf10 ON cf10.chkey = ch9.chkey
        LEFT JOIN chtext AS cht19 ON cht19.chkey = ch9.chkey
        LEFT JOIN chunified AS chu9 ON chu9.chkey = ch9.chkey
        LEFT JOIN component AS co7 ON co7.mukey = mu.mukey
        LEFT JOIN chorizon AS ch10 ON ch10.cokey = co7.cokey
        LEFT JOIN chtexturegrp AS chtg11 ON chtg11.chkey = ch10.chkey
        LEFT JOIN chtexture AS cht20 ON cht20.chtgkey = chtg11.chtgkey
        LEFT JOIN chtexturemod AS chtm10 ON chtm10.chtkey = cht20.chtkey
        LEFT JOIN chstructgrp AS chsg10 ON chsg10.chkey = ch10.chkey
        LEFT JOIN chstruct AS chs10 ON chs10.chsgkey = chsg10.chsgkey
        LEFT JOIN chstructmod AS chsm9 ON chsm9.chskey = chs10.chskey
        LEFT JOIN chpores AS chp10 ON chp10.chkey = ch10.chkey
        LEFT JOIN chconsistence AS chc12 ON chc12.chkey = ch10.chkey
        LEFT JOIN chconshardness AS chch10 ON chch10.chkey = ch10.chkey
        LEFT JOIN chfrags AS cf11 ON cf11.chkey = ch10.chkey
        LEFT JOIN chtext AS cht21 ON cht21.chkey = ch10.chkey
        LEFT JOIN chunified AS chu10 ON chu10.chkey = ch10.chkey
        WHERE l.areasymbol = '{areasymbol}'
        GROUP BY co.areasymbol
        """
        
        payload = {
            "query": query,
            "format": "JSON"
        }
        
        try:
            response = requests.post(self.base_url, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Table' in data and len(data['Table']) > 0:
                row = data['Table'][0]
                return {
                    'State_FIPS': state_fips,
                    'County_FIPS': county_fips,
                    'AWC_avg': row.get('awc_avg'),
                    'Clay_avg': row.get('clay_avg'),
                    'pH_avg': row.get('ph_avg'),
                    'OM_avg': row.get('om_avg')
                }
            else:
                return None
                
        except Exception as e:
            return None
    
    def get_all_county_fips(self):
        """
        Load county FIPS codes from existing data files.
        
        Returns:
            DataFrame with State and County FIPS codes
        """
        # Use county centroids file instead
        centroids_file = self.output_dir / "county_centroids.csv"
        
        if not centroids_file.exists():
            raise FileNotFoundError(
                "County centroids not found. Please run get_county_centroids.py first."
            )
        
        df = pd.read_csv(centroids_file)
        
        # County centroids already has State_FIPS and County_FIPS columns
        counties = df[['State_FIPS', 'County_FIPS']].drop_duplicates()
        
        counties['State_FIPS'] = counties['State_FIPS'].astype(str).str.zfill(2)
        counties['County_FIPS'] = counties['County_FIPS'].astype(str).str.zfill(3)
        
        return counties.sort_values(['State_FIPS', 'County_FIPS']).reset_index(drop=True)
    
    def download_all_soil_data_parallel(self, max_workers=10):
        """
        Download soil data for all counties using parallel processing.
        
        Args:
            max_workers: Number of parallel threads (default: 10)
            
        Returns:
            DataFrame with county soil properties
        """
        counties = self.get_all_county_fips()
        print(f"Found {len(counties)} counties to process")
        print(f"Using {max_workers} parallel workers for faster download...")
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_county = {
                executor.submit(self.query_soil_data, row['State_FIPS'], row['County_FIPS']): (row['State_FIPS'], row['County_FIPS'])
                for _, row in counties.iterrows()
            }
            
            # Process completed tasks with progress bar
            with tqdm(total=len(counties), desc="Downloading soil data") as pbar:
                for future in as_completed(future_to_county):
                    soil_data = future.result()
                    if soil_data:
                        results.append(soil_data)
                    pbar.update(1)
        
        df_soil = pd.DataFrame(results)
        
        output_file = self.output_dir / "county_soil_aggregates.csv"
        df_soil.to_csv(output_file, index=False)
        
        print(f"\nSoil data download complete!")
        print(f"Retrieved data for {len(results)} counties")
        print(f"Saved to: {output_file}")
        
        missing = len(counties) - len(results)
        if missing > 0:
            print(f"Warning: {missing} counties have no soil data available")
        
        if len(df_soil) > 0:
            print("\nSoil property summary:")
            print(df_soil[['AWC_avg', 'Clay_avg', 'pH_avg', 'OM_avg']].describe())
        
        return df_soil


def main():
    """Main execution function."""
    downloader = SoilDataDownloader()
    df_soil = downloader.download_all_soil_data_parallel(max_workers=10)  # Use 10 parallel threads
    
    if len(df_soil) > 0:
        print("\nSample data:")
        print(df_soil.head(10))


if __name__ == "__main__":
    main()
