import pandas as pd
import httpx
from pathlib import Path
import time


# -----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = BASE_DIR / "data" / "curated" / "historical_apps.csv"
OUTPUT_PATH = BASE_DIR / "data" / "curated" / "survivorship_results.csv"

def check_app_status(url):
    """Sending HTTP request to check existance of app URL """
    if pd.isna(url) or not isinstance(url, str) or not url.startswith("http"):
        return "Unknown"
    try:
        #  use httpx to send request
        response = httpx.get(url, timeout=10.0, follow_redirects=True)
        return "Operational" if response.status_code == 200 else "Defunct"
    except Exception:
        return "Defunct"

def run_survivorship_analysis():
    print("loading raw data")
    try:
        
        df = pd.read_csv(INPUT_PATH)
    except Exception as e:
        print(f"error when read csv file: {e}")
        return

    # find column contain URL 
    url_col = None
    for col in df.columns:
        if df[col].astype(str).str.contains('http').any():
            url_col = col
            break
            
    if not url_col:
        print("can not find URL in any column")
        return

    print(f"Found URL: '{url_col}'")

    #create status
    df["Status"] = df[url_col].apply(check_app_status)
    
    # save file
    df.to_csv(OUTPUT_PATH, index=False)
    
    operational = len(df[df["Status"] == "Operational"])
    defunct = len(df[df["Status"] == "Defunct"])
    
    print("Complete analytics")
    print(f"Result: {operational} Operational | {defunct} Defunct")

if __name__ == "__main__":
    run_survivorship_analysis()