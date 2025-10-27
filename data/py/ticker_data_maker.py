import yfinance as yf
import pandas as pd
import numpy as np
import time
from pathlib import Path

# -----------------------------
# User paths / inputs
# -----------------------------
tickers_csv = r"C:\Users\Ignacio\projects\ucla\fall25\econometrics\leverage_ratios\data\csv\company_names.csv"
out_dir     = r"C:\Users\Ignacio\projects\ucla\fall25\econometrics\leverage_ratios\data\csv"

# Use the same quarter you used for Apple
period = pd.Timestamp("2024-09-30")

# (Optional) polite pause between tickers to avoid hammering Yahoo
SLEEP_BETWEEN = 0.3

# -----------------------------
# Helper: pick first available key name
# -----------------------------
def first_key(s: pd.Series, candidates):
    for k in candidates:
        if k in s.index:
            return k
    return None

# -----------------------------
# Load ticker list (Column A header 'Symbol')
# -----------------------------
tickers_df = pd.read_csv(tickers_csv)
tickers = (
    tickers_df.iloc[:, 0]  # first column
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
)

print(f"Loaded {len(tickers)} tickers from CSV.")

# -----------------------------
# Main loop over tickers
# -----------------------------
all_rows = []   # to accumulate a combined dataset

for i, sym in enumerate(tickers, start=1):
    try:
        print(f"[{i}/{len(tickers)}] Processing {sym} ...")

        # --- Define ticker ---
        ticker = yf.Ticker(sym)

        # --- Pull all quarterly statements ---
        income_q   = ticker.quarterly_financials.T
        balance_q  = ticker.quarterly_balance_sheet.T
        cashflow_q = ticker.quarterly_cashflow.T

        # Ensure the requested period exists
        if period not in income_q.index or period not in balance_q.index:
            print(f"  -> Skipping {sym}: {period.date()} not available in statements.")
            time.sleep(SLEEP_BETWEEN)
            continue

        # --- Keep only the best (most complete) quarter ---
        income_yahoo   = income_q.loc[period]
        balance_yahoo  = balance_q.loc[period]
        cashflow_yahoo = cashflow_q.loc[period] if period in cashflow_q.index else pd.Series(dtype=float)

        # --- OPTIONAL Find where each variable is located ---
        # (unchanged/commented in your original)

        # --- Outcome variable (Y): Leverage Ratio = Total Debt / Total Assets ---
        # tolerate key-name differences
        total_debt_key   = first_key(balance_yahoo, ["Total Debt"])
        total_assets_key = first_key(balance_yahoo, ["Total Assets"])

        if total_debt_key is None or total_assets_key is None:
            print(f"  -> Skipping {sym}: missing Total Debt/Total Assets.")
            time.sleep(SLEEP_BETWEEN)
            continue

        total_debt   = balance_yahoo[total_debt_key]
        total_assets = balance_yahoo[total_assets_key]
        leverage = np.nan if total_assets in [0, np.nan] else total_debt / total_assets

        # --- Explanatory Variable 1: Return on Assets (ROA) ---
        net_income_key = first_key(income_yahoo, ["Net Income"])
        if net_income_key is None:
            print(f"  -> Skipping {sym}: missing Net Income.")
            time.sleep(SLEEP_BETWEEN)
            continue

        net_income = income_yahoo[net_income_key]
        ROA = np.nan if total_assets in [0, np.nan] else net_income / total_assets

        # --- Explanatory Variable 2: Asset Tangibility ---
        ppe_key = first_key(balance_yahoo, ["Net PPE", "Property Plant Equipment Net",
                                            "Property, Plant & Equipment Net"])
        if ppe_key is None:
            ppe_net = np.nan
        else:
            ppe_net = balance_yahoo[ppe_key]
        tangibility = np.nan if total_assets in [0, np.nan] else ppe_net / total_assets

        # --- Explanatory Variable 3: Firm Size (Log of Total Assets) ---
        log_total_assets = np.log(total_assets) if pd.notna(total_assets) and total_assets > 0 else np.nan

        # --- Explanatory Variable 4: Market-to-Book (M/B) ---
        # M/B = (Price × Shares Outstanding) / Book Equity
        book_equity_key = first_key(balance_yahoo, ["Common Stock Equity", "Total Stockholder Equity"])
        book_equity = balance_yahoo[book_equity_key] if book_equity_key else np.nan

        shares_out = balance_yahoo.get("Ordinary Shares Number", np.nan)
        # fallback to info if balance sheet doesn’t have shares
        info = ticker.info
        if (pd.isna(shares_out) or shares_out == 0) and isinstance(info, dict):
            shares_out = info.get("sharesOutstanding", np.nan)

        # pick quarter end in the market (unchanged logic)
        price_data = ticker.history(start=period, end=period + pd.Timedelta(days=1))["Close"].dropna()
        if not price_data.empty:
            price_data.index = price_data.index.tz_localize(None)  # remove timezone
            price_at_period = price_data.iloc[-1]
        else:
            price_at_period = np.nan

        market_cap = price_at_period * shares_out if pd.notna(price_at_period) and pd.notna(shares_out) else np.nan
        market_to_book = market_cap / book_equity if pd.notna(market_cap) and pd.notna(book_equity) and book_equity != 0 else np.nan
        log_market_to_book = np.log(market_to_book) if pd.notna(market_to_book) and market_to_book > 0 else np.nan

        # --- Explanatory Variable 5: Effective Tax Rate (ETR) ---
        effective_tax_rate = income_yahoo.get("Tax Rate For Calcs", np.nan)

        # --- Explanatory Variable 6: Stock Price Volatility (Realized Volatility) ---
        # Keep your original hard-coded quarter window to match period
        price_quarter = ticker.history(start="2024-07-01", end="2024-09-30")["Close"].dropna()
        returns = price_quarter.pct_change().dropna()
        realized_volatility = returns.std() if not returns.empty else np.nan

        # --- Explanatory Variable 7: Stock Turnover (Trading Activity) ---
        volume_quarter = ticker.history(start="2024-07-01", end="2024-09-30")["Volume"].dropna()
        avg_daily_volume = volume_quarter.mean() if not volume_quarter.empty else np.nan
        turnover = (avg_daily_volume / shares_out) if pd.notna(avg_daily_volume) and pd.notna(shares_out) and shares_out != 0 else np.nan

        # --- Explanatory Variable 8: Industry (Categorical Control) ---
        sector   = info.get("sector")   if isinstance(info, dict) else None
        industry = info.get("industry") if isinstance(info, dict) else None

        # --- Combine all variables into a DataFrame (unchanged schema) ---
        data = {
            "Ticker": [sym],
            "Period": [period],
            "Leverage": [leverage],
            "ROA": [ROA],
            "Tangibility": [tangibility],
            "Log_Total_Assets": [log_total_assets],
            "Market_to_Book": [market_to_book],
            "Log_Market_to_Book": [log_market_to_book],
            "Effective_Tax_Rate": [effective_tax_rate],
            "Realized_Volatility": [realized_volatility],
            "Turnover": [turnover],
            "Sector": [sector],
            "Industry": [industry]
        }
        df_one = pd.DataFrame(data)

        # --- Save to per-ticker CSV (your original pattern, just dynamic file name) ---
        output_path = Path(out_dir) / f"{sym}_variables.csv"
        df_one.to_csv(output_path, index=False)

        # accumulate for the combined dataset
        all_rows.append(df_one)

        print(f"  -> Saved: {output_path}")
        time.sleep(SLEEP_BETWEEN)

    except Exception as e:
        print(f"  !! Error on {sym}: {e}")
        time.sleep(SLEEP_BETWEEN)
        continue

# -----------------------------
# Save combined dataset
# -----------------------------
if all_rows:
    df_all = pd.concat(all_rows, ignore_index=True)
    combined_path = Path(out_dir) / "all_firm_variables.csv"
    df_all.to_csv(combined_path, index=False)
    print(f"\nCombined dataset saved to: {combined_path}")
else:
    print("\nNo rows saved. Check logs above.")
