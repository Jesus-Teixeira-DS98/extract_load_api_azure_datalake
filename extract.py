import os
import dotenv
from os.path import join
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa


dotenv.load_dotenv(dotenv.find_dotenv())

def extract_weather_data():
    # intervalo de datas
    api_key = os.getenv('api_key')

    data_inicio = datetime.today()
    data_fim = data_inicio + timedelta(days=7)

    # formatando as datas
    data_inicio = data_inicio.strftime('%Y-%m-%d')
    data_fim = data_fim.strftime('%Y-%m-%d')

    city = 'SaoPaulo'
    key = api_key

    URL = join("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
            f"{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv")

    dados = pd.read_csv(URL)
    dados.to_parquet(path=f'/home/jesus/Documentos/repos/airflow/dados/weather_condition={data_inicio}.parquet', engine='pyarrow')
    return None

if __name__ == '__main__':
    dados = extract_weather_data()