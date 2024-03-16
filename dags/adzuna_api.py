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

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}

@dag(
    dag_id="scrape_adzuna_api",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['scraper'],
)

def scrape_adzuna_api():
    
    @task(task_id="adzuna_api")
    def adzuna_api():

        # getting API response
        url2 = 'https://api.adzuna.com/v1/api/jobs/sg/search/8?app_id=f71b8ea3&app_key=8bd547e209d7c487903c9431adc44162&results_per_page=50&what=data' # 50 jobs

        response2 = requests.get(url2)
        if response2.status_code == 200:
            adzuna_job_data_page_2 = response2.json()
        else:
            print('Failed to retrieve data:', response2.status_code)

        # Save the results to a dictionary
        job_dict_2 = adzuna_job_data_page_2['results']

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
            'title': title
        })

        job_df_2.to_csv('sg_jobs_page_8.csv', index=False)

        
        
    adzuna_api()

scrape_adzuna_api()

