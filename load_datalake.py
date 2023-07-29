import os
import dotenv
from azure.storage.blob import BlobServiceClient

## Carregando dados 
def load_data_azure():
    dotenv.load_dotenv(dotenv.find_dotenv())

    connect = os.getenv('connection_string')

    connection_string = BlobServiceClient.from_connection_string(connect)

    folder = '/home/jesus/Documentos/repos/airflow/dados/'

    for file in os.listdir(folder):
        blob_obj = connection_string.get_blob_client(container='weather-daily-files', blob=file)
        print(f'Carregando dados: {file}...')

        with open(os.path.join(folder,file), mode='rb') as file_data:
            blob_obj.upload_blob(file_data)

    return None

if __name__ == '__main__':
    data = load_data_azure()