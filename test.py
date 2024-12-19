import pandas as pd

# Leer archivo Parquet
df = pd.read_parquet('data/binance_full_crypto_prices.parquet')
print(df.head())