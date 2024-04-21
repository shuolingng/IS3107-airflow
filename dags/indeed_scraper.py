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
from google.oauth2 import service_account
import pandas_gbq

#%% Define Indeed Functions
max_jobs_to_scrape = 75

#Chrome options
user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.2 (KHTML, like Gecko) Chrome/22.0.1216.0 Safari/537.2'
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument(f'user-agent={user_agent}') # To run headless mode
chrome_options.add_argument('--headless')  # Run Chrome in headless mode
chrome_options.add_argument("--incognito")
chrome_options.add_argument('--disable-gpu')  # Disable GPU acceleration
chrome_options.add_argument('--disable-blink-features=AutomationControlled') # To avoid bot-detection 
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
remote_webdriver = 'remote_chromedriver'
today = date.today()
today_str = today.strftime("%d %B %Y")

# Helper Functions
def get_search_url(keyword, offset=0):
    parameters = {"q": keyword, "l": "Singapore", "sort": "date", "start": offset}
    return "https://sg.indeed.com/jobs?" + urlencode(parameters)

def clean(text):
    cleaned = text.replace("\n", " ")
    cleaned = re.sub(r'\+1$', '', cleaned)
    return cleaned

def clean_num(num):
    cleaned = num.replace(",", "").replace("$", "").replace(" ", "")
    return float(cleaned)

def deal_with_salary(salary):
    salary_range, salary_freq = re.split(r'\s+a\s+|\s+an\s+', salary)
    if "from" in salary_range.lower(): # for those "From $..."
        salary_min, salary_max = clean_num(salary_range[6:]), None
    else:
        salary_range = salary_range.split(" - ")
        salary_min = clean_num(salary_range[0])
        salary_max = clean_num(salary_range[1])
    return salary_min, salary_max, salary_freq


# Main Functions
def save_to_db_indeed(job_list):
    # Job info
    df = pd.DataFrame(job_list)
    if len(df) == 0:
        return "NOTE: No data scraped!" 
    df.dropna(subset=['Title'], inplace=True)

    # bigquery authentication
    parent_wd = os.path.dirname(os.getcwd())
    cred_path = os.path.join(parent_wd, "airflow", "auth", "is3107-416813-f8b1bf76ef57.json")
    credentials = service_account.Credentials.from_service_account_file(cred_path)
    # write dataframe to bigquery table
    project_id = "is3107-416813"
    table_id = 'is3107-416813.is3107_scraped_data.indeed_data'
    schema = [{'name': 'Salary_min', 'type': 'FLOAT64'}]
    pandas_gbq.to_gbq(df, table_id, project_id, credentials = credentials, if_exists='append', table_schema=schema)

    return None

def get_keyword_data_indeed(keyword, max_jobs_to_scrape):
    with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options) as driver:
        job_list = []
        for offset in range(0, max_jobs_to_scrape, 15): # for each page, for 10 pages
            search_page = get_search_url(keyword, offset)
            try:
                driver.get(search_page) 
                print("Successfully navigated to:", search_page)
                time.sleep(10)

                try: 
                    ul_element = WebDriverWait(driver, 20).until( # Wait up to 20s for page to load & element to be found
                        EC.presence_of_element_located((By.CLASS_NAME, "css-zu9cdh"))
                    )
                    li_element = WebDriverWait(ul_element, 20).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "css-5lfssm"))
                    )
                except Exception as e:
                    print(e)
                    continue

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'lxml')
                
                all_data = soup.find("ul", {"class": "css-zu9cdh eu4oa1w0"})
                all_li = all_data.find_all("li",{"class":"css-5lfssm eu4oa1w0"})

                for i in range(0, len(all_li)-1): # last item is empty
                    job = {}

                    try: job["Title"]=all_li[i].find("a",{"class":"jcs-JobTitle css-jspxzf eu4oa1w0"}).text
                    except: continue # item is empty

                    try: 
                        job["Company"]=all_li[i].find("div", {"class":"company_location"}).find("span",{"data-testid":"company-name"}).text
                    except: 
                        job["Company"]=None

                    try: 
                        salary=all_li[i].find("div",{"class":"salary-snippet-container"}).text
                        job["Salary"]=salary
                        job["Salary_min"], job["Salary_max"], job["Salary_freq"] = deal_with_salary(salary)
                    except: 
                        job["Salary"]=None
                        job["Salary_min"]=None
                        job["Salary_max"]=None
                        job["Salary_freq"]=None

                    try: 
                        other_details = all_li[i].find_all("div",{"class":"metadata css-5zy3wz eu4oa1w0"})
                        job["Type"]=clean(other_details[0].text)
                        if len(other_details) > 1:
                            job["Availability_requests"]=clean(other_details[1].text)
                        else:
                            job["Availability_requests"]=None
                    except: 
                        job["Type"]=None
                        job["Availability_requests"]=None

                    try: job["Requirements_short"]=clean(all_li[i].find("div",{"class":"css-9446fg eu4oa1w0"}).find("ul").text)
                    except: job["Requirements_short"]=None

                    # Get more details
                    link_ele = all_li[i].find("a",{"class":"jcs-JobTitle css-jspxzf eu4oa1w0"}, href=True)
                    link = "https://sg.indeed.com/" + link_ele["href"]
                    driver.get(link) 
                    element = WebDriverWait(driver, 20).until( # Wait up to 20s for page to load & element to be found
                        EC.presence_of_element_located((By.ID, "jobDescriptionText"))
                    )
                    details_soup = BeautifulSoup(driver.page_source, 'lxml')

                    job_description_block = details_soup.find("div", id="jobDescriptionText")
                    job["Description"] = clean(job_description_block.text)

                    req_texts = job_description_block.find_all(string=lambda text: text and ('requirement' in text.lower() or 'qualification' in text.lower()))
                    for text in req_texts:
                        try: job["Requirements_full"] = text.find_next('ul').text
                        except: job["Requirements_full"] = None
                        
                    responsibility_texts = job_description_block.find_all(string=lambda text: text and 'responsibilities' in text.lower())
                    for text in responsibility_texts:
                        try: job["Responsibilities"] = text.find_next('ul').text
                        except: job["Responsibilities"] = None

                    try: job["Application_link"]=details_soup.find("button",{"class":"css-1oxck4n e8ju0x51"}, href=True)["href"]
                    except: job["Application_link"]=None

                    job["Field"]=keyword
                    job["Date_scraped"]=today_str
                    job_list.append(job)
                    # if last page
                    if len(all_li) < 10:
                        break
                    time.sleep(1)
                
            except Exception as e:
                print("Error: ", e)
                continue

    return job_list



#%% Main dag
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0, # for testing
    "retry_delay": timedelta(seconds=10)
}

@dag(
    dag_id="indeed_scraper",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['scraper'],
)


def indeed_scraper():
    
    @task(task_id="scrape_indeed_data1")
    def scrape_indeed_data1():
        keyword_list = ["Data analyst", "Data scientist", "Business analyst"]
        for keyword in keyword_list: # for each job field
            job_list = get_keyword_data_indeed(keyword, max_jobs_to_scrape)
            print("Num of jobs: ", len(job_list))
            save_to_db_indeed(job_list) 
        return job_list[:1]

    @task(task_id="scrape_indeed_data2")
    def scrape_indeed_data2():
        keyword_list = ["Software engineer", "Data engineer", "Data architect"]
        for keyword in keyword_list: # for each job field
            job_list = get_keyword_data_indeed(keyword, max_jobs_to_scrape)
            print("Num of jobs: ", len(job_list))
            save_to_db_indeed(job_list) 
        return job_list[:1]
    

    # Execute tasks
    [scrape_indeed_data1(), scrape_indeed_data2()]

dag_instance = indeed_scraper()

