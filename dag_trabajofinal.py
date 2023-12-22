from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
from requests import Session

# Define la función que ejecutará tu script de Python
def ejecutar_script():
    url = 'https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {
        'start': '1',
        'limit': '50',
        'convert': 'ARS'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': 'b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c',  # Reemplaza con tu clave de API real
    }

    session = Session()
    session.headers.update(headers)

    try:
        resp = session.get(url, params=params)

        if resp.status_code == 200:
            data = resp.json()
            resp_json = data  # Asigna data a resp_json para poder usarlo fuera del bloque if
            print("Datos recibidos con éxito.")
        else:
            print("Error al hacer la solicitud:", resp.status_code)

    except Exception as e:
        print(f"Error en la solicitud: {e}")

    # Ahora puedes imprimir o trabajar con resp_json fuera del bloque try-except
    print(resp_json)
    print(resp_json.keys())

    stations_data = resp_json.get("data", [])
    df_stations_new = pd.DataFrame(stations_data)
    df_stations_new.head(10)

# Configura la información del DAG
default_args = {
    'owner': 'jorge_guzzo_coderhouse',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define el DAG
dag = DAG(
    'dag_trabajofinal',
    default_args=default_args,
    description='DAG para ejecutar un script de Python',
    schedule_interval=timedelta(days=1),  # Frecuencia de ejecución
)

# Define el nodo de inicio
inicio_tarea = DummyOperator(
    task_id='inicio_tarea',
    dag=dag,
)

# Define la tarea usando PythonOperator
ejecutar_script_task = PythonOperator(
    task_id='ejecutar_script',
    python_callable=ejecutar_script,
    dag=dag,
)

# Define dos tareas adicionales
otra_tarea_1 = DummyOperator(
    task_id='otra_tarea_1',
    dag=dag,
)

otra_tarea_2 = DummyOperator(
    task_id='otra_tarea_2',
    dag=dag,
)

# Define el nodo de fin
fin_tarea = DummyOperator(
    task_id='fin_tarea',
    dag=dag,
)

# Establece las dependencias
inicio_tarea >> ejecutar_script_task >> [otra_tarea_1, otra_tarea_2] >> fin_tarea

# Guarda el DAG para que Airflow pueda detectarlo
if __name__ == "__main__":
    dag.cli()
