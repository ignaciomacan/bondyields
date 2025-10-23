import yfinance as yf
import pandas as pd
import numpy as np
# --- Define ticker ---
ticker = yf.Ticker("AAPL")

# --- Pull all quarterly statements ---
income_q   = ticker.quarterly_financials.T
balance_q  = ticker.quarterly_balance_sheet.T
cashflow_q = ticker.quarterly_cashflow.T

# --- Keep only the best (most complete) quarter ---
period = pd.Timestamp("2024-09-30")

income_yahoo   = income_q.loc[period]
balance_yahoo  = balance_q.loc[period]
cashflow_yahoo = cashflow_q.loc[period]

# --- OPTIONAL Find where each variable is located ---
#search_terms = ["Total Debt", "Total Assets"]
#sources = {"income": income_yahoo, "balance": balance_yahoo, "cashflow": cashflow_yahoo}

#found = {}
#for term in search_terms:
#    for name, df in sources.items():
#        if term in df.index:
#            found[term] = (name, df[term])
#            break

#found

# --- Outcome variable (Y): Leverage Ratio = Total Debt / Total Assets ---
total_debt   = balance_yahoo["Total Debt"]
total_assets = balance_yahoo["Total Assets"]

leverage = total_debt / total_assets


# --- Explanatory Variable 1: Return on Assets (ROA) ---
# ROA = Net Income / Total Assets
# Net Income comes from the income statement, Total Assets from the balance sheet

net_income   = income_yahoo["Net Income"]
total_assets = balance_yahoo["Total Assets"]

ROA = net_income / total_assets

# --- Explanatory Variable 2: Asset Tangibility ---
# Tangibility = Net Property, Plant, and Equipment (PPE) / Total Assets
# NOTE: We're using "Net PPE" as reported in Apple's balance sheet.
#       This label may vary across firms (e.g., "Property Plant Equipment Net"),
#       which can be handled later when expanding the dataset.

ppe_net      = balance_yahoo["Net PPE"]
total_assets = balance_yahoo["Total Assets"]

tangibility  = ppe_net / total_assets

# --- Explanatory Variable 3: Firm Size (Log of Total Assets) ---

total_assets = balance_yahoo["Total Assets"]
log_total_assets = np.log(total_assets)

# --- Explanatory Variable 4: Market-to-Book (M/B) ---
# M/B = (Price × Shares Outstanding) / Book Equity

book_equity = balance_yahoo["Common Stock Equity"]
shares_out  = balance_yahoo["Ordinary Shares Number"]

#pick quarter end in the market
price_data = ticker.history(start=period, end=period + pd.Timedelta(days=1))["Close"].dropna()
#remove timezone
price_data.index = price_data.index.tz_localize(None)
price_at_period = price_data.iloc[-1]

market_cap = price_at_period * shares_out
market_to_book = market_cap / book_equity

# Take the log to reduce skew and make the variable more normally distributed for regression
log_market_to_book = np.log(market_to_book)

# --- Explanatory Variable 5: Effective Tax Rate (ETR) ---
# ETR = 'Tax Rate For Calcs' (as reported by Yahoo Finance)

effective_tax_rate = income_yahoo["Tax Rate For Calcs"]

# --- Explanatory Variable 6: Stock Price Volatility (Realized Volatility) ---
# Realized Volatility = standard deviation of daily returns within the quarter

# Pull daily close prices over the quarter
price_quarter = ticker.history(start="2024-07-01", end="2024-09-30")["Close"].dropna()

# Compute daily returns
returns = price_quarter.pct_change().dropna()

# Compute realized volatility (standard deviation of returns)
realized_volatility = returns.std()

# --- Explanatory Variable 7: Stock Turnover (Trading Activity) ---
# Turnover = Average Daily Volume / Shares Outstanding

# Pull daily trading volume over the same quarter
volume_quarter = ticker.history(start="2024-07-01", end="2024-09-30")["Volume"].dropna()

# Compute average daily trading volume
avg_daily_volume = volume_quarter.mean()

# Get shares outstanding from balance sheet
shares_out = balance_yahoo["Ordinary Shares Number"]

# Compute turnover ratio
turnover = avg_daily_volume / shares_out

# --- Explanatory Variable 8: Industry (Categorical Control) ---
info = ticker.info

industry = info.get("industry")
sector   = info.get("sector")

# --- Combine all variables into a DataFrame ---

data = {
    "Ticker": ["AAPL"],
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

df_apple = pd.DataFrame(data)

# --- Save to CSV ---
output_path = r"C:\Users\Ignacio\projects\ucla\fall25\econometrics\leverage_ratios\data\csv\apple_variables.csv"
df_apple.to_csv(output_path, index=False)

print(df_apple)