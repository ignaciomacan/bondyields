from fredapi import Fred
import pandas as pd

fred = Fred(api_key='3707355de3032aa9b43716f690e0cf29 ')

codes = {
    'yield_10y': 'DGS10',        # daily (business)
    'cpi': 'CPIAUCSL',           # monthly (MS)
    'real_pce': 'PCEC96',        # monthly (MS)
    'fedfunds': 'FEDFUNDS',      # monthly (MS)  <-- if you want daily use EFFR
    'unrate': 'UNRATE',          # monthly (MS)
    'credit_spread': 'BAA10YM'   # monthly (MS)
}

def to_month_end(series):
    """Convert any FRED series to month-end timestamp."""
    s = fred.get_series(series)
    freq = s.index.inferred_freq
    if freq in ('B', 'D'):                 # daily/business daily → take last obs in month
        s = s.resample('M').last()
    else:                                  # monthly (MS or M) → shift to month-end
        s.index = s.index.to_period('M').to_timestamp('M')
    return s

# download & align
aligned = {k: to_month_end(v) for k, v in codes.items()}
df = pd.concat(aligned, axis=1).sort_index()



# trim to window
df = df.loc['1980-01-31':'2025-12-31']

# transforms
df['cpi_yoy'] = df['cpi'].pct_change(12) * 100
df['pce_yoy'] = df['real_pce'].pct_change(12) * 100

# final regression set
final = df[['yield_10y', 'cpi_yoy', 'pce_yoy', 'fedfunds', 'unrate', 'credit_spread']].dropna()
print(final.head(), final.tail(), final.shape)
