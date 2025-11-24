
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.decorators import dag, task


# def fetch_earthquake_data(**context):
#     url = "http://www.koeri.boun.edu.tr/scripts/lst2.asp"
#     try:
#         #HTTP GET isteği
#         response = requests.get(url)
#         #istek başarısızsa hata fırlat
#         response.raise_for_status()

#         soup = BeautifulSoup(response.content, 'html.parser')

#         #Extract Data

#         pre_text = soup.find('pre').get_text().strip()
#         print(pre_text[:500])

#         lines = pre_text.strip().split("\n")
#         data_lines = [
#             line for line in lines
#             if line.strip() and not line.startswith('-') and not line.startswith('Tarih')
#         ]

    
#         records = []
#         for line in data_lines:
#             if line.strip() and line.strip()[0].isdigit():
#                 parts = line.split()
#                 if len(parts) < 7:
#                     continue
#                 record = {
#                     "tarih": parts[0],
#                     "saat": parts[1],
#                     "enlem": parts[2],
#                     "boylam": parts[3],
#                     "derinlik_km": parts[4],
#                     "MD": parts[5],
#                     "ML": parts[6],
#                     "Mw": parts[7] if len(parts) > 7 else "-",
#                     "yer": " ".join(parts[8:]) if len(parts) > 8 else "-"
#                 }
#                 records.append(record)


#         df = pd.DataFrame(records)
#         #csv
#         csv_file = f'/tmp/scrapped_data_{context["ds"]}.csv'
#         df.to_csv(csv_file, index=False)

#         #json
#         json_file = f'dags/data/scrapped_data_latest.json'
#         df.to_json(json_file, orient='records', date_format='iso')

#         print(df.head())
#         logging.info(f"Successfully scraped {len(records)} items.")
#         return len(records)
#     except Exception as e:
#         logging.error(f"Scraping failed: {e}")
#         raise

   
# def save_to_postgres(**context):
   
#     csv_file = f'/tmp/scrapped_data_{context["ds"]}.csv'

#     df = pd.read_csv(csv_file)

#     hook = PostgresHook(postgres_conn_id = "postgres_localhost")
#     engine = hook.get_sqlalchemy_engine()

#     df.to_sql('scrapped_eartquakes', engine, if_exists= 'replace', index=False, method='multi')

#     print(f"{len(df)} satır veri veritabanına eklendi.")
    


# default_args = {
#     'owner': 'mirac',
#     'retries': 5,
#     'retry_delay': timedelta(minutes=5),
#     'start_date': datetime(2025, 10, 7),
#     'catchup': False
# }

# with DAG(
#     dag_id='kandilli_earthquake_data_pipeline_v4',
#     default_args=default_args,
#     description = 'DAG to scrape earthquake data from Kandilli Rasathanesi and store it in Postgres',
#     schedule = '@hourly'
# )as dag:
#     fetch_task = PythonOperator(
#         task_id = 'fetch_earthquake_data',
#         python_callable=fetch_earthquake_data,
#     )

#     save_to_postgres_task = PythonOperator(
#         task_id = 'save_to_postgres',
#         python_callable=save_to_postgres,
#     )

#     fetch_task >> save_to_postgres_task


# UPDATED CODE WITH DECORATORS

@dag(schedule='@hourly',
     start_date=datetime(2025, 10, 7),
     catchup=False)
def kandilli_earthquake_data_pipeline_v5():
    
    @task()
    def fetch_earthquake_data(**context):
        url = "http://www.koeri.boun.edu.tr/scripts/lst2.asp"
        try:
            #HTTP GET isteği
            response = requests.get(url)
            #istek başarısızsa hata fırlat
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            #Extract Data

            pre_text = soup.find('pre').get_text().strip()
            print(pre_text[:500])

            lines = pre_text.strip().split("\n")
            data_lines = [
                line for line in lines
                if line.strip() and not line.startswith('-') and not line.startswith('Tarih')
            ]

        
            records = []
            for line in data_lines:
                if line.strip() and line.strip()[0].isdigit():
                    parts = line.split()
                    if len(parts) < 7:
                        continue
                    record = {
                        "Tarih": parts[0],
                        "Saat": parts[1],
                        "Enlem": parts[2],
                        "Boylam": parts[3],
                        "Derinlik_km": parts[4],
                        "MD": parts[5],
                        "ML": parts[6],
                        "Mw": parts[7] if len(parts) > 7 else "-",
                        "Lokasyon": " ".join(parts[8:]) if len(parts) > 8 else "-"
                    }
                    records.append(record)


            df = pd.DataFrame(records)
            #csv
            csv_file = f'/tmp/scrapped_data_{context["ds"]}.csv'
            df.to_csv(csv_file, index=False)

            #json
            json_file = f'dags/data/scrapped_data_latest.json'
            df.to_json(json_file, orient='records', date_format='iso')

            print(df.head())
            logging.info(f"Successfully scraped {len(records)} items.")
            return len(records)
        except Exception as e:
            logging.error(f"Scraping failed: {e}")
            raise
    
    @task()
    def save_to_postgres(**context):
   
        csv_file = f'/tmp/scrapped_data_{context["ds"]}.csv'

        df = pd.read_csv(csv_file)

        hook = PostgresHook(postgres_conn_id = "postgres_localhost")
        engine = hook.get_sqlalchemy_engine()

        df.to_sql('scrapped_eartquakes', engine, if_exists= 'replace', index=False, method='multi')

        print(f"{len(df)} satır veri veritabanına eklendi.")
    
    fetch_earthquake_data() >> save_to_postgres()


dag = kandilli_earthquake_data_pipeline_v5()