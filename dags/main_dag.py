from indeed_scraper import get_keyword_data_indeed, save_to_db_indeed
from glassdoor_scraping_v1 import get_keyword_data_glassdoor, save_to_db_glassdoor
from adzuna_api import adzuna_get_onepg
from airflow.decorators import dag, task
from datetime import datetime, timedelta, date
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import re
import os

# full_keyword_list = ["Data analyst", "Database administrator", "Data modeler", "Software engineer", "Data engineer", "Data architect", 
#     "Statistician", "Business intelligence developer", "Marketing scientist", "Business analyst", "Quantitative analyst", 
#     "Data scientist", "Computer & information research scientist", "Machine learning engineer"]

SEARCH_QUERY_LIST = ["Software engineer", "Machine learning engineer", "Data analyst", "Data architect", "Business analyst", "Data scientist"]
parent_wd = os.path.dirname(os.getcwd())
cred_path = os.path.join(parent_wd, "airflow", "auth", "is3107-416813-f8b1bf76ef57.json")
credentials = service_account.Credentials.from_service_account_file(cred_path)
project_id = "is3107-416813"

# Cleaning functions
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
        if isinstance(date_str, date):
            return date_str
        else:
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

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=10)
} 

#%% Main DAG
@dag(
    dag_id="main_dag",
    default_args=default_args,
    schedule_interval="@daily",  # daily at 12am
    catchup=False,
    tags=['is3107']
)

def main_dag():
    @task
    def scrape_glassdoor():
        max_jobs_to_scrape = 70
        for job_field in SEARCH_QUERY_LIST:
            total_job_data = get_keyword_data_glassdoor(job_field, max_jobs_to_scrape)
            if len(total_job_data) < 35: # if too little, might have error, so try again
                total_job_data = get_keyword_data_glassdoor(job_field, max_jobs_to_scrape)
            save_to_db_glassdoor(total_job_data)
        return total_job_data[:1]
    
    @task
    def scrape_indeed():
        max_jobs_to_scrape = 75
        for keyword in SEARCH_QUERY_LIST: 
            job_list = get_keyword_data_indeed(keyword, max_jobs_to_scrape)
            print("Num of jobs: ", len(job_list))
            if len(job_list) < 35: # if too little, might have error, so try again
                job_list = get_keyword_data_indeed(keyword, max_jobs_to_scrape)
            save_to_db_indeed(job_list) 
        return job_list[:1]
    
    @task
    def adzuna_api():
        for page_num in range(1, 10): # get latest 10 pgs, each pg has 50 job listings
            adzuna_get_onepg(page_num)
        return None
    
    @task
    def fetch_data(webscrape):
        """Fetches data from BigQuery into Pandas DataFrames."""
        # SQL queries to fetch data
        indeed_sql = f"SELECT * FROM `{project_id}.is3107_scraped_data.indeed_data`"
        glassdoor_sql = f"SELECT * FROM `{project_id}.is3107_scraped_data.glassdoor_data`"
        adzuna_sql = f"SELECT * FROM `{project_id}.is3107_scraped_data.adzuna_data`"

        # Reading data into DataFrames
        indeed = pandas_gbq.read_gbq(indeed_sql, project_id=project_id, credentials=credentials)
        glassdoor = pandas_gbq.read_gbq(glassdoor_sql, project_id=project_id, credentials=credentials)
        api = pandas_gbq.read_gbq(adzuna_sql, project_id=project_id, credentials=credentials)
        return indeed, glassdoor, api
    
    @task
    def clean_and_process_data(fetched_data):
        indeed, glassdoor, api = fetched_data
        """Cleans and processes the fetched data frames."""
        # INDEED
        indeed.dropna(subset=['Date_scraped'], inplace=True)
        # remove duplicates on all col except link (for those with same job but diff link)
        cols = indeed.columns[indeed.columns != 'Application_link']
        indeed = indeed.drop_duplicates(subset=cols, keep='last')
        # clean text fields
        text_columns = ['Requirements_short', 'Requirements_full', 'Responsibilities', 'Description']
        for col in text_columns:
            indeed[col] = indeed[col].apply(clean_text)
        indeed['Data_source'] = 'Indeed'

        # ADZUNA
        api['Salary_min'] = pd.to_numeric(api['Salary_min'], errors='coerce')
        api['Salary_max'] = pd.to_numeric(api['Salary_max'], errors='coerce')
        api = api[api['Field'].str.strip() != ''] # irrelevant roles
        api['Data_source'] = 'Adzuna'

        # GLASSDOOR
        glassdoor['Salary_min'] = pd.to_numeric(glassdoor['Salary_min'], errors='coerce')
        glassdoor['Salary_max'] = pd.to_numeric(glassdoor['Salary_max'], errors='coerce')
        # remove duplicates on all col except link
        cols = glassdoor.columns[glassdoor.columns != 'Application_link']
        glassdoor = glassdoor.drop_duplicates(subset=cols, keep='last')
        glassdoor['Description'] = glassdoor['Description'].apply(clean_text).apply(clean_html)
        glassdoor['Company'] = glassdoor['Company'].replace('null', pd.NA)
        glassdoor['Data_source'] = 'Glassdoor'

        # COMBINE
        combined = pd.concat([indeed, glassdoor, api], ignore_index=True)
        combined = combined.dropna(subset=['Date_scraped', 'Title', 'Company'])
        combined = combined[combined['Title'].str.strip() != ''] # empty strings
        combined['Date_scraped'] = combined['Date_scraped'].apply(extract_date)

        # Deal with duplicates
        priority_order = {'Indeed': 1, 'Glassdoor': 2, 'Adzuna': 3}
        combined['priority'] = combined['Data_source'].map(priority_order)
        combined_sorted = combined.sort_values(['Date_scraped', 'priority'], ascending=[False, True])
        columns_to_agg = [col for col in combined_sorted.columns if col not in ['Title', 'Company', 'Field']]
        aggregation_dict = {col: 'first' for col in columns_to_agg}
        aggregation_dict.update({'Field': lambda x: set(x)})
        combined_clean = combined_sorted.groupby(['Title', 'Company']).agg(aggregation_dict).reset_index()
        combined_clean['Field'] = combined_clean['Field'].apply(lambda x: ' '.join(x))

        # standardise salary
        combined_clean['Salary_min_month'] = combined_clean.apply(lambda row: standardise_salary(row['Salary_min'], row['Salary_freq']), axis=1)
        combined_clean['Salary_max_month'] = combined_clean.apply(lambda row: standardise_salary(row['Salary_max'], row['Salary_freq']), axis=1)
        combined_clean.drop(columns=['Salary_min', 'Salary_max', 'Salary_freq'], inplace=True)
        return combined_clean

    @task
    def upload_to_bigquery(combined_clean):
        """Uploads processed data to BigQuery."""
        column_order = [
            'Title', 'Company', 'Description', 'Field', 
            'Date_scraped', 'Data_source', 'Application_link',
            'Salary_min_month', 'Salary_max_month', 'Requirements_short', 
            'Requirements_full', 'Type', 'Responsibilities', 'Salary', 
            'Location', 'Size', 'Founded', 'Industry', 'Sector', 'Revenue', 
            'Availability_requests', 'Created_date', 'Job_id', 'priority'
        ]
        combined_clean = combined_clean[column_order]

        # Credentials and project ID
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
        print(f"Data successfully uploaded to {table_id}")
        return None   

    webscrape = [scrape_glassdoor(), scrape_indeed(), adzuna_api()]
    fetched_data = fetch_data(webscrape)
    combined_data = clean_and_process_data(fetched_data)
    upload_to_bigquery(combined_data)


dag_instance = main_dag()
