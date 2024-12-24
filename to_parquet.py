import pandas as pd
import os

# Path to store the last download date for each cryptocurrency
DATA_DIR = os.path.join("data")
OUTPUT_FILE = os.path.join(DATA_DIR, "binance_full_crypto_prices.csv")


# Leer el archivo CSV como DataFrame
df = pd.read_csv(OUTPUT_FILE)

# Ruta para guardar el archivo Parquet
parquet_file = os.path.join(DATA_DIR, "binance_full_crypto_prices.parquet")

# Guardar el DataFrame como Parquet
df.to_parquet(parquet_file, engine="fastparquet", index=False, compression='SNAPPY')

print(f"Archivo convertido y guardado como {parquet_file}")
