# fetch_fred_series.py
# Downloads six FRED series as individual CSVs to your specified folder.

import os
import time
import json
import requests
import pandas as pd

FRED_API_KEY = "3707355de3032aa9b43716f690e0cf29"
OUTPUT_DIR = r"C:\Users\Ignacio\projects\ucla\fall25\econometrics\project_one\data\csv"

# series_id : nice filename (and human-readable name)
SERIES = {
    "DGS10":     "dgs10_10y_treasury_yield.csv",              # Outcome
    "FEDFUNDS":  "fedfunds_effective_rate.csv",
    "UNRATE":    "unrate_unemployment_rate.csv",
    "BAA":       "baa_moodys_baa_yield.csv",
    "CPIAUCSL":  "cpiaucsl_cpi_index.csv",
    "BBKMGDP":   "bbkmgdp_monthly_real_gdp.csv",
}

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

def fetch_fred_series(series_id: str, api_key: str) -> pd.DataFrame:
    """
    Fetches a FRED series (all observations) as a DataFrame with columns: date, value.
    Values that are '.' are converted to NaN (missing).
    """
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        # You can add optional filters here, e.g., observation_start, observation_end, frequency, etc.
    }
    r = requests.get(FRED_BASE, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    obs = data.get("observations", [])
    df = pd.DataFrame(obs)[["date", "value"]]
    # Convert value to numeric; '.' becomes NaN
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    # Cast date to pandas datetime
    df["date"] = pd.to_datetime(df["date"])
    # Sort just in case
    df = df.sort_values("date").reset_index(drop=True)
    return df

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for sid, fname in SERIES.items():
        try:
            print(f"Fetching {sid} ...")
            df = fetch_fred_series(sid, FRED_API_KEY)
            out_path = os.path.join(OUTPUT_DIR, fname)
            df.to_csv(out_path, index=False)
            print(f"Saved: {out_path} ({len(df)} rows)")
            # Be polite to the API
            time.sleep(0.5)
        except requests.HTTPError as e:
            print(f"[HTTP ERROR] {sid}: {e}")
        except requests.RequestException as e:
            print(f"[REQUEST ERROR] {sid}: {e}")
        except Exception as e:
            print(f"[UNEXPECTED ERROR] {sid}: {e}")

if __name__ == "__main__":
    main()
