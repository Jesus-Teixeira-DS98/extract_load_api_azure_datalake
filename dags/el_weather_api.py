import sys
import os
sys.path.append('../airflow')
from extract import extract_weather_data
from load_datalake import load_data_azure
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
from datetime import datetime, timedelta
import dotenv


dotenv.load_dotenv(dotenv.find_dotenv())

api_key = os.getenv('api_key')

default_args = {
    'retries': 5,  # Defina o número de tentativas desejado aqui
    'retry_delay': timedelta(minutes=1),  # Intervalo entre as tentativas
    # Outros argumentos padrão, se necessário
}

with DAG( dag_id="extract_load_data",
    description="essa Dag coleta dados na API e carrega no Azure Data Lake",
    start_date=datetime(2023,7,28),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag :

    task_1 = EmptyOperator(
        task_id='início'
    )

    task_2 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/home/jesus/Documentos/repos/airflow/dados"'
    )

    task_3 = PythonOperator(
        task_id='extrai_dados_api',
        python_callable=extract_weather_data
    )

    task_4 = PythonOperator(
        task_id='carrega_dados_azure_container',
        python_callable=load_data_azure
    )

    task_5 = BashOperator(
        task_id='exclui_dados_antigos',
        bash_command='rm /home/jesus/Documentos/repos/airflow/dados/*.parquet '
    )

    task_6 = EmptyOperator(
        task_id='fim'
    )

task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6