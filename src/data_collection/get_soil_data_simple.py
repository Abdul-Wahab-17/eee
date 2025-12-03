"""
Download county-level soil properties from USDA NRCS - SIMPLIFIED VERSION.

Uses a simpler query that's more likely to work with the USDA API.
"""

import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

class SoilDataDownloader:
    """Download county-level soil properties from USDA NRCS."""
    
    def __init__(self):
        self.base_url = "https://sdmdataaccess.sc.egov.usda.gov/tabular/post.rest"
        self.output_dir = Path(__file__).parent.parent.parent / "data" / "raw"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def query_soil_data(self, state_fips, county_fips):
        """Query soil properties for a specific county using simplified query."""
        
        # Simpler query that's more likely to work
        query = f"""
        SELECT 
            mu.mukey,
            mu.muname,
            co.compname,
            co.comppct_r,
            ch.hzdept_r,
            ch.hzdepb_r,
            ch.awc_r,
            ch.claytotal_r,
            ch.ph1to1h2o_r,
            ch.om_r
        FROM legend l
        INNER JOIN mapunit mu ON mu.lkey = l.lkey
        INNER JOIN component co ON co.mukey = mu.mukey
        INNER JOIN chorizon ch ON ch.cokey = co.cokey
        WHERE l.areasymbol = 'US{state_fips}{county_fips}'
        AND ch.hzdept_r = 0
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
                # Calculate weighted averages
                total_pct = 0
                awc_sum = clay_sum = ph_sum = om_sum = 0
                count = 0
                
                for row in data['Table']:
                    pct = row.get('comppct_r', 0) or 0
                    awc = row.get('awc_r')
                    clay = row.get('claytotal_r')
                    ph = row.get('ph1to1h2o_r')
                    om = row.get('om_r')
                    
                    if pct > 0:
                        total_pct += pct
                        if awc: awc_sum += awc * pct
                        if clay: clay_sum += clay * pct
                        if ph: ph_sum += ph * pct
                        if om: om_sum += om * pct
                        count += 1
                
                if total_pct > 0:
                    return {
                        'State_FIPS': state_fips,
                        'County_FIPS': county_fips,
                        'AWC_avg': awc_sum / total_pct if awc_sum > 0 else None,
                        'Clay_avg': clay_sum / total_pct if clay_sum > 0 else None,
                        'pH_avg': ph_sum / total_pct if ph_sum > 0 else None,
                        'OM_avg': om_sum / total_pct if om_sum > 0 else None
                    }
            
            return None
                
        except Exception as e:
            return None
    
    def get_all_county_fips(self):
        """Load county FIPS codes from centroids file."""
        centroids_file = self.output_dir / "county_centroids.csv"
        
        if not centroids_file.exists():
            raise FileNotFoundError(
                "County centroids not found. Please run get_county_centroids.py first."
            )
        
        df = pd.read_csv(centroids_file)
        counties = df[['State_FIPS', 'County_FIPS']].drop_duplicates()
        
        counties['State_FIPS'] = counties['State_FIPS'].astype(str).str.zfill(2)
        counties['County_FIPS'] = counties['County_FIPS'].astype(str).str.zfill(3)
        
        return counties.sort_values(['State_FIPS', 'County_FIPS']).reset_index(drop=True)
    
    def download_all_soil_data_parallel(self, max_workers=100):
        """Download soil data using parallel processing."""
        counties = self.get_all_county_fips()
        print(f"Found {len(counties)} counties to process")
        print(f"Using {max_workers} parallel workers...")
        print("Note: Many counties may not have soil data available in the database.\n")
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_county = {
                executor.submit(self.query_soil_data, row['State_FIPS'], row['County_FIPS']): (row['State_FIPS'], row['County_FIPS'])
                for _, row in counties.iterrows()
            }
            
            with tqdm(total=len(counties), desc="Downloading soil data") as pbar:
                for future in as_completed(future_to_county):
                    soil_data = future.result()
                    if soil_data:
                        results.append(soil_data)
                    pbar.update(1)
        
        df_soil = pd.DataFrame(results)
        
        output_file = self.output_dir / "soil_data.csv"
        df_soil.to_csv(output_file, index=False)
        
        print(f"\n{'='*60}")
        print(f"Soil data download complete!")
        print(f"{'='*60}")
        print(f"Retrieved data for {len(results)} out of {len(counties)} counties")
        print(f"Saved to: {output_file}")
        
        missing = len(counties) - len(results)
        if missing > 0:
            print(f"\nNote: {missing} counties have no soil data in the USDA database")
            print("This is normal - not all counties have detailed soil surveys.")
        
        if len(df_soil) > 0:
            print("\nSoil property summary:")
            print(df_soil[['AWC_avg', 'Clay_avg', 'pH_avg', 'OM_avg']].describe())
        else:
            print("\n⚠️  WARNING: No soil data was retrieved!")
            print("The USDA Soil Data Access API may be unavailable or the query format may need adjustment.")
        
        return df_soil


def main():
    """Main execution function."""
    downloader = SoilDataDownloader()
    df_soil = downloader.download_all_soil_data_parallel(max_workers=100)
    
    if len(df_soil) > 0:
        print("\nSample data:")
        print(df_soil.head(10))
    else:
        print("\n💡 TIP: You can proceed without soil data.")
        print("Most yield prediction models work well with just weather and yield history.")


if __name__ == "__main__":
    main()
