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
# keyword_list = ["Data analyst", "Database administrator", "Data modeler", "Software engineer", "Data engineer", "Data architect", 
#     "Statistician", "Business intelligence developer", "Marketing scientist", "Business analyst", "Quantitative analyst", 
#     "Data scientist", "Computer & information research scientist", "Machine learning engineer"]

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

def save_to_excel(job_list, company_info_list):
    # Job info
    df = pd.DataFrame(job_list) 
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

    # Company info
    company_df = pd.DataFrame(company_info_list) 
    company_df = company_df.dropna(subset=['Company']).drop_duplicates(subset=['Company'])
    company_df.to_excel(f"./Output/Company.xlsx", index=False)
    return None


# Main Functions
def get_job_info(i, all_li, driver, keyword):
    job = {}
    company_info = {}

    try: job["Title"]=all_li[i].find("a",{"class":"jcs-JobTitle css-jspxzf eu4oa1w0"}).text
    except: # item is empty
        return job, company_info

    try: 
        company=all_li[i].find("div", {"class":"company_location"}).find("span",{"data-testid":"company-name"}).text
        job["Company"]=company
        company_info["Company"]=company
    except: 
        job["Company"]=None
        company_info["Company"]=None

    try: company_info["Company_rating"]=all_li[i].find("div",{"class":"company_location"}).find("span",{"data-testid":"holistic-rating"}).text
    except: company_info["Company_rating"]=None

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

    return job, company_info


def get_keyword_data(keyword, job_list, company_info_list):
    with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options) as driver:
        # rmb to change 2nd num to 100 after testing phase!
        for offset in range(0, 10, 15): # for each page, for 10 pages
            search_page = get_search_url(keyword, offset)
            try:
                driver.get(search_page) 
                print("Successfully navigated to:", search_page)

                try: 
                    ul_element = WebDriverWait(driver, 20).until( # Wait up to 20s for page to load & element to be found
                        EC.presence_of_element_located((By.CLASS_NAME, "css-zu9cdh"))
                    )
                    li_element = WebDriverWait(ul_element, 20).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "css-5lfssm"))
                    )
                except:
                    continue

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, 'lxml')
                
                all_data = soup.find("ul", {"class": "css-zu9cdh eu4oa1w0"})
                all_li = all_data.find_all("li",{"class":"css-5lfssm eu4oa1w0"})

                for i in range(0, len(all_li)-1): # last item is empty
                    job, company_info= get_job_info(i, all_li, driver, keyword)
                    job_list.append(job)
                    company_info_list.append(company_info)
                    # if last page
                    if len(all_li) < 10:
                        break
                
            except Exception as e:
                print("Error: ", e)
                continue

    return job_list, company_info_list



#%% Main dag
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
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
        # Define variables
        keyword_list = ["Data analyst"]
        company_info_list = []
        job_list = []
        # Get data
        for keyword in keyword_list: # for each job field
            job_list, company_info_list = get_keyword_data(keyword, job_list, company_info_list)
        # Save info
        save_to_excel(job_list, company_info_list) 
        return job_list[:1]
    

    @task(task_id="scrape_indeed_data2")
    def scrape_indeed_data2():
        # Define variables
        keyword_list = ["Software engineer", "Data engineer"]
        company_info_list = []
        job_list = []
        # Get data
        for keyword in keyword_list: # for each job field
            job_list, company_info_list = get_keyword_data(keyword, job_list, company_info_list)
        # Save info
        save_to_excel(job_list, company_info_list) 
        return job_list[:1]
    
    @task(task_id="scrape_indeed_data3")
    def scrape_indeed_data3():
        # Define variables
        keyword_list = ["Statistician"]
        company_info_list = []
        job_list = []
        # Get data
        for keyword in keyword_list: # for each job field
            job_list, company_info_list = get_keyword_data(keyword, job_list, company_info_list)
        # Save info
        save_to_excel(job_list, company_info_list) 
        return job_list[:1]
    

    # Execute tasks
    scrape_indeed_data1()
    # [scrape_indeed_data1(), scrape_indeed_data2(), scrape_indeed_data3()]

dag_instance = indeed_scraper()

