import boto3
from botocore.exceptions import NoCredentialsError
import os

# Inicializa el cliente de S3
s3_client = boto3.client('s3')

# Definir tu bucket de S3 y el archivo local
bucket_name = 'glue-s3-wlsrw'

file_name = 'binance_full_crypto_prices.parquet'
DATA_DIR = os.path.join("data")
file_path = os.path.join(DATA_DIR, file_name)

s3_key = f"folder/{file_name}"  # Ruta dentro de tu bucket

# Verificar que el archivo exista
if os.path.exists(file_path):
    try:
        # Cargar el archivo al bucket de S3
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"File {file_name} successfully uploaded to {bucket_name}/{s3_key}")
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"An error occurred: {e}")
else:
    print(f"File {file_path} does not exist.")
