import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def fetch_data():
    '''
    Fungsi ini digunakan untuk memanggil 
    '''
    # Koneksi 
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    
    # Connect to PostgreSQL
    conn = db.connect(conn_string)
    
    # Execute query and load data into DataFrame
    df = pd.read_sql("SELECT * FROM public.table_m3", conn)
    
    # Save Data
    df.to_csv('/opt/airflow/dags/P2M3_yuzal_data_raw.csv', index=False)

    # Print success message
    print("-------Success------")
    
    # Close connection
    conn.close()

def clean_data():
    '''
    Fungsi untuk menghilangkan missing value
    '''
    # Read Data
    df = pd.read_csv('/opt/airflow/dags/P2M3_yuzal_data_raw.csv')
    
    # Clean Duplicates
    df.drop_duplicates(subset=['Gender', 'Married', 'Dependents', 'Education',
       'Self_Employed', 'ApplicantIncome', 'CoapplicantIncome', 'LoanAmount',
       'Loan_Amount_Term', 'Credit_History', 'Property_Area', 'Loan_Status'], inplace=True)

    # Clean Lower Case
    df.columns=[x.lower() for x in df.columns]

    # Clean Missing Value
    # Mengubah nilai NaN pada kolom self_employed menjadi 'No'
    df['self_employed'].fillna('No', inplace=True)

    # Mengubah nilai NaN pada kolom credit_history menjadi 0
    df['credit_history'].fillna(0, inplace=True)

    # Menghapus baris yang masih memiliki NaN pada kolom lainnya
    df.dropna(inplace=True)
    
    # Save Data
    df.to_csv('/opt/airflow/dags/P2M3_yuzal_data_clean.csv', index=False)

def post_elastic():
    '''
    Fungsi ini digunakan untuk mengupload hasil data clean menuju kibana
    '''
    es = Elasticsearch('http://elasticsearch:9200') 
    df=pd.read_csv('/opt/airflow/dags/P2M3_yuzal_data_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="table_loan", doc_type="doc", body=doc)
        print(res)

default_args = {
    'owner': 'yuzal',
    'start_date': dt.datetime(2024, 8, 18, 23, 22, 0) - dt.timedelta(hours=8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('DATADAG',
         default_args=default_args,
         schedule_interval= '30 6 * * *',
         catchup=False
         ) as dag:
    
    # Define the task
    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data
    )

    cleaning_data= PythonOperator(
        task_id='cleaning_data',
        python_callable=clean_data
    )
    post_elastic_search= PythonOperator(
        task_id='post_elastic_search',
        python_callable=post_elastic
    )

# Set task dependency
fetch_data_task >> cleaning_data >> post_elastic_search