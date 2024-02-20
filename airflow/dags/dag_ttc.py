from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests

def upload_to_gcs(bucket_name, object_name, local_file):
    """
    Função para upload de arquivo para o GCS.
    """
    hook = GCSHook(gcp_conn_id="google-cloud-storage")
    hook.upload(bucket_name=bucket_name, object_name=object_name, filename=local_file)

# Função para consumir dados da API
def fetch_api_data():
    url = "https://myttc.ca/finch_station.json"  # Substitua pela URL da sua API
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Escreve os dados em um arquivo
        with open("finch.json", "w") as file:  # Substitua pelo caminho desejado
            file.write(str(data))
    else:
        print("Falha ao obter dados da API")

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG
dag = DAG(
    'dag_ttc',
    default_args=default_args,
    description='Uma DAG simples que consome dados de uma API e escreve em um arquivo',
    schedule_interval=timedelta(days=1),
)

# Tarefa para consumir dados da API
fetch_data_task = PythonOperator(
    task_id='fetch_api_data',
    python_callable=fetch_api_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={'bucket_name': 'airflow-dags-estudo',
               'object_name': 'data/raw/finch.json',
               'local_file': 'finch.json'},
    dag=dag,
)


fetch_data_task >> upload_task

