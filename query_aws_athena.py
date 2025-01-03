import boto3
import time
from delete_file_bucket import *

# Configuración de S3
s3 = boto3.resource('s3')
bucket_name = 'athena-query-results-wlsrw'
prefix = 'cleaned-data/crypto/' # Path of file result of query

# Variables
database_name = "crypto_data"
table_name = "crypto_cleaned"
query_output_location = f"s3://{bucket_name}/results/"
delete_query = f"DROP TABLE IF EXISTS {table_name};"
create_query = f"""
    CREATE TABLE {table_name}
    WITH (
        format = 'PARQUET',
        external_location = 's3://{bucket_name}/{prefix}'
    ) AS 
    SELECT 
        from_unixtime(close_time / 1000) AS close_time_converted,
        CAST(ROUND(open, 2) AS DECIMAL(20, 2)) AS open,
        CAST(ROUND(high, 2) AS DECIMAL(20, 2)) AS high,
        CAST(ROUND(close, 2) AS DECIMAL(20, 2)) AS close,
        CAST(ROUND(volume, 2) AS DECIMAL(20, 2)) AS volume,
        CAST(ROUND(quote_asset_volume, 2) AS DECIMAL(20, 2)) AS quote_asset_volume,
        num_trades,
        CAST(ROUND(taker_buy_base_asset, 2) AS DECIMAL(20, 2)) AS taker_buy_base_asset,
        CAST(ROUND(taker_buy_quote_asset, 2) AS DECIMAL(20, 2)) AS taker_buy_quote_asset,
        symbol
    FROM
        crypto_folder
    ORDER BY 
        close_time_converted;
    """

# Initialize Athena client
athena_client = boto3.client('athena')

def execute_query(query, action):
    """Helper function to execute a query in Athena."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': query_output_location}
    )
    query_execution_id = response['QueryExecutionId']
    print(f"Query started with execution ID: {query_execution_id}")

    # Wait for the query to complete
    timeout = 300  # Timeout in seconds
    start_time = time.time()
    
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        if time.time() - start_time > timeout:
            raise TimeoutError("Athena query time out")
        time.sleep(5)

    if state == 'SUCCEEDED':
        print(f"Query {action} completed successfully. Query Execution ID: {query_execution_id}")
    else:
        print(f"Query {action} failed with state: {state}.")
        raise Exception(f"Query {action} failed: {state}. Query Execution ID: {query_execution_id}")
    
    # Clean up temporary results
    temp_prefix = f"results/{query_execution_id}"
    delete_file_bucket_s3(s3, bucket_name, temp_prefix)
    
# Step 1: Drop the table if it exists
execute_query(delete_query,"delete")

# step 2: Eliminamos archivos existentes en la ubicación S3 para evitar conflictos
delete_file_bucket_s3(s3, bucket_name, 'results/tables/')
delete_file_bucket_s3(s3, bucket_name, prefix)

# Step 3: Create the table
execute_query(create_query,"create")
