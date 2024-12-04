import requests
import pandas as pd
import time
import os

# Ruta del archivo donde guardamos la última fecha descargada por cada criptomoneda
LAST_DOWNLOAD_FILE = "last_download_date.txt"

# Función para leer la última fecha descargada para una criptomoneda
def read_last_download_date(symbol):
    if os.path.exists(LAST_DOWNLOAD_FILE):
        with open(LAST_DOWNLOAD_FILE, 'r') as file:
            last_downloads = file.readlines()
            for line in last_downloads:
                stored_symbol, timestamp = line.strip().split(',')
                if stored_symbol == symbol:
                    return int(timestamp)  # Devuelve la última fecha en milisegundos
    return 946684800000  # Si no existe, devuelve una fecha muy antigua (1 de enero de 2000)

# Función para guardar la última fecha descargada para una criptomoneda
def save_last_download_date(symbol, timestamp):
    # Leer todas las fechas actuales del archivo
    last_downloads = []
    if os.path.exists(LAST_DOWNLOAD_FILE):
        with open(LAST_DOWNLOAD_FILE, 'r') as file:
            last_downloads = file.readlines()
    
    # Actualizar o agregar la fecha de la criptomoneda
    with open(LAST_DOWNLOAD_FILE, 'w') as file:
        found = False
        for line in last_downloads:
            stored_symbol, stored_timestamp = line.strip().split(',')
            if stored_symbol == symbol:
                file.write(f"{symbol},{timestamp}\n")  # Actualizar
                found = True
            else:
                file.write(line)  # Mantener la línea sin cambios
        if not found:
            file.write(f"{symbol},{timestamp}\n")  # Si no se encontró, agregar nueva línea

# Función para descargar datos históricos de Binance
def fetch_historical_data(symbol, interval="1d", start_time=None, end_time=None):
    url = "https://api.binance.com/api/v3/klines"
    all_data = []
    limit = 1000  # Máximo número de registros por solicitud

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit
        }
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.text}")
        
        data = response.json()
        if not data:
            break  # Salir si no hay más datos
        
        # Convertir los datos a un DataFrame
        df = pd.DataFrame(data, columns=[
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base_asset", "taker_buy_quote_asset", "ignore"
        ])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df["symbol"] = symbol
        all_data.append(df)

        # Actualizar el inicio del próximo rango al último cierre
        start_time = int(data[-1][6]) + 1  # Siguiente intervalo
        time.sleep(1)  # Evitar límites de la API

    # Combinar todos los datos descargados
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

# Paso 1: Obtener pares disponibles en Binance
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
        df['baseAsset'] = df['baseAsset'].str.upper()  # Normalizar símbolos a mayúsculas
        return df[["symbol", "baseAsset", "quoteAsset"]]
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

# Obtener pares de Binance y seleccionar criptomonedas populares
binance_symbols_df = get_binance_symbols()
popular_symbols = binance_symbols_df["symbol"].tolist()

# Descargar datos históricos solo de las criptomonedas que han cambiado desde la última fecha
all_data = []

for symbol in popular_symbols[:30]:  # Cambia el rango para más monedas
    print(f"Descargando datos para: {symbol}")
    try:
        # Leer la última fecha descargada para cada criptomoneda
        last_download_date = read_last_download_date(symbol)
        
        # Definir un límite para no descargar más de lo necesario
        current_time = int(time.time() * 1000)  # Tiempo actual en milisegundos

        # Descargar datos solo si faltan datos entre el último timestamp y el presente
        if last_download_date < current_time:
            historical_data = fetch_historical_data(
                symbol, 
                interval="1d", 
                start_time=last_download_date,  # Solo consulta desde la última fecha
                end_time=current_time  # Hasta el tiempo actual
            )
            
            if not historical_data.empty:
                # Verificar si los datos descargados son realmente nuevos
                last_timestamp_downloaded = int(historical_data["timestamp"].max().timestamp() * 1000)
                
                if last_timestamp_downloaded > last_download_date:
                    all_data.append(historical_data)
                    # Actualizar la última fecha de descarga para esta criptomoneda
                    save_last_download_date(symbol, last_timestamp_downloaded)  # Guardar la nueva fecha
                else:
                    print(f"No se descargaron nuevos datos para {symbol}. Última fecha de datos es igual.")
        else:
            print(f"No hay datos nuevos para {symbol}, ya está actualizado.")

    except Exception as e:
        print(f"Error con {symbol}: {e}")
    
    time.sleep(1)  # Evitar límites de tasa

# Combinar todos los datos en un DataFrame único
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Guardar los datos descargados en un archivo CSV (puedes añadir nuevos datos sin sobrescribir todo)
    final_df.to_csv("binance_full_crypto_prices.csv", mode='a', header=False, index=False)
    print("Datos históricos descargados exitosamente.")
else:
    print("No se descargaron datos nuevos.")
