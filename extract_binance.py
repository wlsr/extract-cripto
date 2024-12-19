import requests
import pandas as pd
import time
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path to store the last download date for each cryptocurrency
DATA_DIR = os.path.join("data")
LAST_DOWNLOAD_FILE = os.path.join(DATA_DIR, "last_download_date.txt")
OUTPUT_FILE = os.path.join(DATA_DIR, "binance_full_crypto_prices.csv")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Function to read the last download date for a specific cryptocurrency
def read_last_download_date(symbol):
    if os.path.exists(LAST_DOWNLOAD_FILE):
        with open(LAST_DOWNLOAD_FILE, "r") as file:
            for line in file.readlines():
                stored_symbol, timestamp = line.strip().split(",")
                if stored_symbol == symbol:
                    return int(timestamp)
    return 946684800000  # Default timestamp: Jan 1, 2000

# Save the last download date for a specific cryptocurrency
def save_last_download_date(symbol, timestamp):
    last_downloads = {}
    if os.path.exists(LAST_DOWNLOAD_FILE):
        with open(LAST_DOWNLOAD_FILE, "r") as file:
            for line in file.readlines():
                stored_symbol, stored_timestamp = line.strip().split(",")
                last_downloads[stored_symbol] = stored_timestamp

    # Update or add the new timestamp
    last_downloads[symbol] = timestamp
    with open(LAST_DOWNLOAD_FILE, "w") as file:
        for sym, ts in last_downloads.items():
            file.write(f"{sym},{ts}\n")

# Fetch historical data from Binance API
def fetch_historical_data(symbol, interval="1d", start_time=None, end_time=None):
    url = "https://api.binance.com/api/v3/klines"
    all_data = []
    limit = 1000

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            logging.error(f"Error {response.status_code}: {response.text}")
            break

        data = response.json()
        if not data:
            break

        # Convert the data to a pandas DataFrame
        print(data)
        df = pd.DataFrame(data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base_asset", "taker_buy_quote_asset", "ignore"
        ])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["symbol"] = symbol[:-4]  # Exclude "USDT"
        all_data.append(df)

        # Update start_time for the next iteration
        start_time = int(data[-1][6]) + 1  # Next start time is the close time + 1
        time.sleep(0.5)  # Sleep to avoid hitting API rate limits

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

# Get all available trading pairs from Binance
def get_binance_symbols(base_asset="USDT"):
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        symbols = [
            item for item in data["symbols"]
            if item["quoteAsset"] == base_asset and item["status"] == "TRADING"
        ]
        df = pd.DataFrame(symbols)
        df["baseAsset"] = df["baseAsset"].str.upper()
        return df[["symbol", "baseAsset", "quoteAsset"]]
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

# Main script
if __name__ == "__main__":
    binance_symbols_df = get_binance_symbols()
    popular_symbols = binance_symbols_df["symbol"].tolist()
    all_data = []

    for symbol in popular_symbols[:15]:
        logging.info(f"Downloading: {symbol}")
        try:
            last_download_date = read_last_download_date(symbol)
            current_time = int(time.time() * 1000)

            # Download new data if the last recorded date is older than the current time
            if last_download_date < current_time:
                historical_data = fetch_historical_data(
                    symbol, interval="1d", start_time=last_download_date, end_time=current_time
                )
                if not historical_data.empty:
                    # Update the last downloaded timestamp
                    last_timestamp_downloaded = int(historical_data["timestamp"].max().timestamp() * 1000)
                    if last_timestamp_downloaded > last_download_date:
                        all_data.append(historical_data)
                        save_last_download_date(symbol, last_timestamp_downloaded)
                    else:
                        logging.info(f"No new data for {symbol}.")
            else:
                logging.info(f"No new data for {symbol}, already up-to-date.")
        except Exception as e:
            logging.error(f"Error with {symbol}: {e}")
        time.sleep(1)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # Write data to the CSV file
        is_new_file = not os.path.exists(OUTPUT_FILE)
        final_df.to_csv(OUTPUT_FILE, mode="a", header=is_new_file, index=False)
        logging.info("Historical data downloaded successfully.")
    else:
        logging.info("No new data was downloaded.")
