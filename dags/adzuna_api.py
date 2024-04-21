from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urlencode
import pandas as pd
import os
import re
import time
from datetime import date, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas_gbq
from google.oauth2 import service_account
import numpy as np

def adzuna_get_onepg(page_num):
    # get api response
    url2 = f'https://api.adzuna.com/v1/api/jobs/sg/search/{page_num}?app_id=f71b8ea3&app_key=8bd547e209d7c487903c9431adc44162&results_per_page=50&what=data' # 50 jobs

    response2 = requests.get(url2)
    if response2.status_code == 200:
        adzuna_job_data_page_2 = response2.json()
    else:
        print('Failed to retrieve data:', response2.status_code)

    # Save the results to a dictionary
    try:
        job_dict_2 = adzuna_job_data_page_2['results']
    except UnboundLocalError as e:
        print(e)
        return None

    # read the response data into lists
    redirect_url = []
    category_label = []
    company_display_name = []
    created = []
    description = []
    id = []
    salary_min = []
    salary_max = []
    title = []


    for i in range(len(job_dict_2)):
        redirect_url.append(job_dict_2[i]['redirect_url'])
        category_label.append(job_dict_2[i]['category']['label'])
        company_display_name.append(job_dict_2[i]['company']['display_name'])
        created.append(job_dict_2[i]['created'])
        description.append(job_dict_2[i]['description'])
        id.append(job_dict_2[i]['id'])
        title.append(job_dict_2[i]['title'])

        try:
            salary_min.append(job_dict_2[i]['salary_min'])
            salary_max.append(job_dict_2[i]['salary_max'])
        except:
            salary_min.append(None)
            salary_max.append(None)

    # store response in a dataframe
    job_df_2 = pd.DataFrame({
        'category_label': category_label,
        'company_display_name': company_display_name,
        'created': created,
        'description': description,
        'id': id,
        'salary_min': salary_min,
        'salary_max': salary_max,
        'title': title,
        'redirect_url': redirect_url
    })

    # editing category_label
    keywords = ['data', 'analyst', 'database', 'software', 'engineer', 'architect', 'modeler', 'administrator', 'statistician', 'marketing', 'scientist', 'business', 'intelligence',
                'quantitative', 'computer', 'information', 'research', 'machine', 'learning']

    extra = job_df_2.copy()
    extra = extra.drop(['category_label'], axis=1)

    def retain_words(title):
        words = title.split()
        retained_words = [word for word in words if word in keywords]
        return ' '.join(retained_words)

    extra['category_label'] = extra['title'].str.lower().apply(retain_words)

    # adding column whether is_internship or not
    extra['is_internship'] = np.where(extra['title'].str.lower().str.contains('intern'), 1, 0)
    extra.head()

    extra['salary_frequency'] = np.where(extra['is_internship'] == 1,
                                        np.where(extra['salary_max'] > 48000, 'yearly', 'monthly'),
                                        np.where(extra['salary_max'] > 60000, 'yearly', 'monthly'))

    # Including date scraped
    from datetime import datetime

    new_rows = []
    if 'date_scraped' not in extra.columns:
        # If the column does not exist, add it with today's date for all existing rows
        extra['date_scraped'] = datetime.today().date()
    else:
        # If adding new rows to the DataFrame
        # Assume `new_rows` is a DataFrame containing the new rows to be added
        # First, add the 'date_scraped' column to these new rows with today's date
        new_rows['date_scraped'] = datetime.today().date()

        # Then append the new rows to the existing DataFrame
        df = pd.concat([df, new_rows], ignore_index=True)
    
    extra.columns = ["Company", "Created_date", "Description", "Job_id", "Salary_min", "Salary_max", "Title", "Application_link", "Field", "Is_internship",
                "Salary_freq", "Date_scraped"]
    
    extra['Date_scraped'] = pd.to_datetime(extra['Date_scraped'])
    extra['Created_date'] = pd.to_datetime(extra['Created_date'])
    
    print(extra.head())

    # initialise bigquery
    parent_wd = os.path.dirname(os.getcwd())
    cred_path = os.path.join(parent_wd, "airflow", "auth", "is3107-416813-f8b1bf76ef57.json")
    credentials = service_account.Credentials.from_service_account_file(cred_path) 

    # bigquery tables
    project_id = "is3107-416813"
    adzuna_id = 'is3107_scraped_data.adzuna_data'

    # push to bigquery 
    pandas_gbq.to_gbq(extra, adzuna_id, project_id, credentials = credentials, if_exists='append')

#%% DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
} 

@dag(
    dag_id="scrape_adzuna_api",
    default_args=default_args,
    schedule='@weekly', # runs weekly on sunday
    catchup=False,
    tags=['scraper'],
)

def scrape_adzuna_api():
    
    @task(task_id="adzuna_api")
    def adzuna_api():
        # get latest 10 pages of data
        for page_num in range(1, 10):
            adzuna_get_onepg(page_num)

    adzuna_api()

scrape_adzuna_api()

