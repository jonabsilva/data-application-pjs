import pandas as pd
#import gcsfs
from google.cloud import storage
import certifi
import os
import io


os.environ['SSL_CERT_FILE'] = certifi.where()


bucket_name = 'db_landzone_idr_00001_pjs_dev'
# path to the GCS file CSV in GCS
csv_file_name = 'db_landzone_idr_00001_pjs_dev/esaj/cabecalho/cabecalho.csv'

client = storage.Client()
# Obtém o bucket
bucket = client.get_bucket(bucket_name)
# Obtém o blob (arquivo) no bucket
blob = bucket.blob(csv_file_name)
# Baixa o conteúdo do blob como bytes
csv_bytes = blob.download_as_bytes()
# Usa io.BytesIO para ler os bytes em um DataFrame pandas
csv_file = io.BytesIO(csv_bytes)
df = pd.read_csv(csv_file)
# Exibe o DataFrame
print(df)