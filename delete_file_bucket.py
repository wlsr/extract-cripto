import boto3

# # Configuración de S3
# s3 = boto3.resource('s3')
# bucket_name = 'athena-query-results-wlsrw'
# prefix = 'cleaned-data/crypto/'

def delete_file_bucket_s3(s3, bucket_name, prefix):
    # Borrar archivos en el bucket
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=prefix).delete()

    print(f"Archivos eliminados del bucket {bucket_name}/{prefix}")

#delete_file_bucket_s3(s3, bucket_name, prefix)


