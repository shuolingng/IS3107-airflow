# bigquery_dag.py
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import logging
import re
import numpy as np

# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
}

# Define the DAG object
dag = DAG(
    'bigquery_data_pipeline',
    default_args=default_args,
    description='A DAG to process and upload data to BigQuery',
    schedule_interval=timedelta(days=1),
)
#cleaning functions
def clean_text(text):
    """Removes new lines and other excessive whitespace from text, handling None values."""
    if text is None:
        return None
    # Replace new lines and strip unnecessary spaces
    cleaned = text.replace("\n", " ").strip()
    return cleaned


def clean_html(text):
    """Removes HTML tags from text and formats it, ensuring the resulting string is clean."""
    if text is None:
        return None
    cleaned = re.sub(r'<.*?>', '', text)
    return cleaned.strip()

def convert_to_num(string):
    """Converts string to numeric type, considering thousands 'k', handling invalid formats gracefully."""
    try:
        string = str(string).strip().lower().replace('k', '000')
        return float(string) if '.' in string else int(string)
    except ValueError:
        return None
    
def extract_date(date_str):
    """Extracts date from datetime string, returns None if format is incorrect."""
    try:
        datetime_object = datetime.strptime(date_str.strip(), '%d %B %Y')
        return datetime_object.date()
    except ValueError:
        return None
    
def standardise_salary(salary, freq):
    """Converts salary to a standardized monthly amount based on frequency."""
    if pd.isna(salary) or not isinstance(salary, (int, float)):
        return None
    
    conversion = {
        "month": lambda x: x,
        "monthly": lambda x: x,
        "year": lambda x: x / 12,
        "yearly": lambda x: x / 12,
        "day": lambda x: x * 21,
        "hour": lambda x: x * 44 * 4
    }
    return conversion.get(freq, lambda x: None)(salary)

#tasks
def fetch_data(ds, **kwargs):
    """Fetches data from BigQuery into Pandas DataFrames."""
    project_id = "is3107-416813"
    parent_wd = os.path.dirname(os.path.dirname(os.getcwd()))
    cred_path = "./dags/auth/is3107-416813-f8b1bf76ef57.json"
    credentials = service_account.Credentials.from_service_account_file(cred_path)

    # SQL queries to fetch data
    indeed_sql = f"SELECT * FROM `{project_id}.is3107_scraped_data.indeed_data`"
    glassdoor_sql = f"SELECT * FROM `{project_id}.is3107_scraped_data.glassdoor_data`"
    adzuna_sql = f"SELECT * FROM `{project_id}.is3107_scraped_data.adzuna_data`"

    # Reading data into DataFrames
    indeed = pandas_gbq.read_gbq(indeed_sql, project_id=project_id, credentials=credentials)
    glassdoor = pandas_gbq.read_gbq(glassdoor_sql, project_id=project_id, credentials=credentials)
    api = pandas_gbq.read_gbq(adzuna_sql, project_id=project_id, credentials=credentials)

    # Save DataFrames to XComs for other tasks to use
    kwargs['ti'].xcom_push(key='indeed_data', value=indeed)
    kwargs['ti'].xcom_push(key='glassdoor_data', value=glassdoor)
    kwargs['ti'].xcom_push(key='api_data', value=api)

def clean_and_process_data(ds, **kwargs):
    """Cleans and processes the fetched data frames."""
    ti = kwargs['ti']
    indeed = ti.xcom_pull(task_ids='fetch_data', key='indeed_data')
    glassdoor = ti.xcom_pull(task_ids='fetch_data', key='glassdoor_data')
    api = ti.xcom_pull(task_ids='fetch_data', key='api_data')

    # Your existing cleaning functions (already defined earlier)

    #indeed
    initial_rows = len(indeed)
    indeed.dropna(subset=['Date_scraped'], inplace=True)
    print(f"Number of rows dropped due to missing 'Date_scraped': {initial_rows - len(indeed)}")
    cols = indeed.columns[indeed.columns != 'Date_scraped']
    duplicated_before = indeed.duplicated(subset=cols, keep=False).sum()
    indeed = indeed.drop_duplicates(subset=cols, keep='last')
    print(f"Number of duplicated rows dropped (excluding 'Date_scraped'): {duplicated_before}")
    indeed = indeed.groupby('Application_link').agg({
        'Title': 'first',
        'Company': 'first',
        'Salary': 'first',
        'Salary_min': 'first',
        'Salary_max': 'first',
        'Salary_freq': 'first',
        'Type': 'first',
        'Availability_requests': 'first',
        'Requirements_short': 'first',
        'Description': 'first',
        'Requirements_full': 'first',
        'Responsibilities': 'first',
        'Field': 'first',
        'Date_scraped': 'first'
    }).reset_index()
    text_columns = ['Requirements_short', 'Requirements_full', 'Responsibilities', 'Description']
    for col in text_columns:
        indeed[col] = indeed[col].apply(clean_text)
    for col_prefix in ['Salary_min', 'Salary_max']:
        indeed[f'{col_prefix}_month'] = indeed.apply(
            lambda row: standardise_salary(row[col_prefix], row['Salary_freq']), axis=1
        )
    indeed.drop(columns=['Salary_min', 'Salary_max', 'Salary_freq'], inplace=True)
    indeed['Data_source'] = 'Indeed'
    indeed['Date_scraped'] = indeed['Date_scraped'].apply(extract_date)

    #adzuna
    api['Salary_min'] = pd.to_numeric(api['Salary_min'], errors='coerce')
    api['Salary_max'] = pd.to_numeric(api['Salary_max'], errors='coerce')
    cols = api.columns[api.columns != 'Date_scraped']
    duplicated_before = api.duplicated(subset=cols, keep=False).sum()
    api = api.drop_duplicates(subset=cols, keep='last')
    print(f"Number of duplicated rows dropped (excluding 'Date_scraped'): {duplicated_before}")
    api = api.groupby('Application_link').agg({
        'Company': 'first',
        'Created_date': 'first',
        'Description': 'first',
        'Job_id': 'first',
        'Salary_min': 'first',
        'Salary_max': 'first',
        'Title': 'first',
        'Field': 'first', #try put in set
        'Is_internship': 'first',
        'Salary_freq': 'first',
        'Date_scraped': 'first'
    }).reset_index()
    api['Salary_min_month'] = api.apply(lambda row: standardise_salary(row['Salary_min'], row['Salary_freq']), axis=1)
    api['Salary_max_month'] = api.apply(lambda row: standardise_salary(row['Salary_max'], row['Salary_freq']), axis=1)
    api.drop(columns=['Salary_min', 'Salary_max', 'Salary_freq'], inplace=True)
    api['Data_source'] = 'Adzuna'

    #Glassdoor
    glassdoor['Salary_min'] = pd.to_numeric(glassdoor['Salary_min'], errors='coerce')
    glassdoor['Salary_max'] = pd.to_numeric(glassdoor['Salary_max'], errors='coerce')
    cols = glassdoor.columns[glassdoor.columns != 'Date_scraped']
    duplicated_before = glassdoor.duplicated(subset=cols, keep=False).sum()
    glassdoor = glassdoor.drop_duplicates(subset=cols, keep='last')
    print(f"Number of duplicated rows dropped (excluding 'Date_scraped'): {duplicated_before}")
    glassdoor = glassdoor.groupby('Application_link').agg({
        'Company': 'first',
        'Location': 'first',
        'Salary': 'first',
        'Salary_min': 'first',
        'Salary_max': 'first',
        'Salary_freq': 'first',
        'Description': 'first',
        'Size': 'first',
        'Founded': 'first',
        'Type': 'first',
        'Industry': 'first',
        'Sector': 'first',
        'Revenue': 'first',
        'Date_scraped': 'first',
        'Field': 'first',
        'Title': 'first'
    }).reset_index()
    glassdoor['Description'] = glassdoor['Description'].apply(clean_text).apply(clean_html)
    glassdoor['Salary_min_month'] = glassdoor.apply(lambda row: standardise_salary(row['Salary_min'], row['Salary_freq']), axis=1)
    glassdoor['Salary_max_month'] = glassdoor.apply(lambda row: standardise_salary(row['Salary_max'], row['Salary_freq']), axis=1)
    glassdoor.drop(columns=['Salary_min', 'Salary_max', 'Salary_freq'], inplace=True)
    glassdoor['Data_source'] = 'Glassdoor'

    # Combine datasets
    combined = pd.concat([indeed, glassdoor, api], ignore_index=True)

    # Store the combined DataFrame for the next task
    ti.xcom_push(key='combined_data', value=combined)

def upload_to_bigquery(ds, **kwargs):
    """Uploads processed data to BigQuery."""
    ti = kwargs['ti']
    combined_clean = ti.xcom_pull(task_ids='clean_and_process_data', key='combined_data')

    combined_clean = combined_clean.dropna(subset=['Title'])
    priority_order = {'Indeed': 1, 'Glassdoor': 2, 'Adzuna': 3}
    combined_clean['priority'] = combined_clean['Data_source'].map(priority_order)
    combined_sorted = combined_clean.sort_values(
        ['Date_scraped', 'priority'],
        ascending=[False, True]
    )
    combine_clean = combined_sorted.drop_duplicates(
        subset=['Title', 'Company'],
        keep='first',
        ignore_index=True
    )
    combine_clean.drop(columns=['priority'], inplace=True)
    column_order = [
        'Title', 'Company', 'Description', 'Field', 
        'Date_scraped', 'Data_source', 'Application_link',
        'Salary_min_month', 'Salary_max_month', 'Requirements_short', 
        'Requirements_full', 'Type', 'Responsibilities', 'Salary', 
        'Location', 'Size', 'Founded', 'Industry', 'Sector', 'Revenue', 
        'Availability_requests', 'Created_date', 'Job_id'
    ]
    combine_clean = combine_clean[column_order]

    #combine_clean['Field'] = combine_clean['Field'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, (list, set)) else str(x))

    #test to see if it works
    combine_clean.to_csv('./dags/output/combine_clean.csv')

    # Credentials and project ID
    project_id = "is3107-416813"
    parent_wd = os.path.dirname(os.path.dirname(os.getcwd()))
    cred_path = "./dags/auth/is3107-416813-f8b1bf76ef57.json"
    credentials = service_account.Credentials.from_service_account_file(cred_path)
    table_id = 'is3107_scraped_data.final_table'

    schema = [
        {'name': 'Title', 'type': 'STRING'},
        {'name': 'Company', 'type': 'STRING'},
        {'name': 'Description', 'type': 'STRING'},
        {'name': 'Field', 'type': 'STRING'},
        {'name': 'Date_scraped', 'type': 'DATE'},
        {'name': 'Data_source', 'type': 'STRING'},
        {'name': 'Application_link', 'type': 'STRING'},
        {'name': 'Salary_min_month', 'type': 'FLOAT'},
        {'name': 'Salary_max_month', 'type': 'FLOAT'},
        {'name': 'Requirements_short', 'type': 'STRING'},
        {'name': 'Requirements_full', 'type': 'STRING'},
        {'name': 'Type', 'type': 'STRING'},
        {'name': 'Responsibilities', 'type': 'STRING'},
        {'name': 'Salary', 'type': 'STRING'},
        {'name': 'Location', 'type': 'STRING'},
        {'name': 'Size', 'type': 'STRING'},
        {'name': 'Founded', 'type': 'STRING'},
        {'name': 'Industry', 'type': 'STRING'},
        {'name': 'Sector', 'type': 'STRING'},
        {'name': 'Revenue', 'type': 'STRING'},
        {'name': 'Availability_requests', 'type': 'STRING'},
        {'name': 'Created_date', 'type': 'DATE'},
        {'name': 'Job_id', 'type': 'STRING'},
    ]  

    # Upload DataFrame to BigQuery
    pandas_gbq.to_gbq(
        combined_clean,
        table_id,
        project_id=project_id,
        credentials=credentials,
        if_exists='replace',  # Choose 'append' or 'replace' as per your requirement
        table_schema=schema  # Optional: Define if schema needs to be enforced
    )
    logging.info(f"Data successfully uploaded to {table_id}")


# Create a task to fetch data
t1 = PythonOperator(
    task_id='fetch_data',
    provide_context=True,
    python_callable=fetch_data,
    dag=dag,
)
# Create a task to clean and process data
t2 = PythonOperator(
    task_id='clean_and_process_data',
    provide_context=True,
    python_callable=clean_and_process_data,
    dag=dag,
)
# Create a task to upload data to BigQuery
t3 = PythonOperator(
    task_id='upload_to_bigquery',
    provide_context=True,
    python_callable=upload_to_bigquery,
    dag=dag,
)
# Define task dependencies
t1 >> t2 >> t3
