from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys, os

sys.path.insert(0, '/opt/airflow/src')

from scraper import KrishaSeleniumScraper
from cleaner import DataCleaner
from loader import DataLoader

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'krisha_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

def run_scraper():
    scraper = KrishaSeleniumScraper(max_pages=5)
    data = scraper.scrape()
    if data:
        import pandas as pd
        df = pd.DataFrame(data)
        os.makedirs('/opt/airflow/data', exist_ok=True)
        df.to_csv('/opt/airflow/data/raw_apartments.csv', index=False, encoding='utf-8')

def run_cleaner():
    cleaner = DataCleaner(raw_filename='raw_apartments.csv', raw_dir='/opt/airflow/data', cleaned_dir='/opt/airflow/data')
    df_raw = cleaner.load_raw_data()
    if not df_raw.empty:
        df_clean = cleaner.clean_data(df_raw)
        cleaner.save_cleaned_data(df_clean)

def run_loader():
    loader = DataLoader(db_path='/opt/airflow/data/krisha_apartments.db')
    df_cleaned = loader.load_cleaned_data('/opt/airflow/data/cleaned_apartments.csv')
    if not df_cleaned.empty:
        loader.insert_data(df_cleaned)
        loader.verify_data()

t1 = PythonOperator(task_id='scrape_krisha', python_callable=run_scraper, dag=dag)
t2 = PythonOperator(task_id='clean_data', python_callable=run_cleaner, dag=dag)
t3 = PythonOperator(task_id='load_to_db', python_callable=run_loader, dag=dag)

t1 >> t2 >> t3
