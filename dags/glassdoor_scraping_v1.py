from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
#from fake_useragent import UserAgent

# Global variables for configurability and easy maintenance
date_scraped = datetime.now().strftime("%Y-%m-%d")
BASE_URL = 'https://www.glassdoor.sg/Job/index.htm'
SEARCH_QUERY_LIST = ["Software engineer", "Data analyst", "Database administrator", "Data modeler", "Data engineer", "Data architect",
                "Statistician","Business intelligence (BI) developer", "Marketing scientist", "Business analyst", "Quantitative analyst",
                "Data scientist","Computer & information research scientist", "Machine learning engineer"]
#user_agent = UserAgent().random
#service = Service(executable_path=ChromeDriverManager().install())
#user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.2 (KHTML, like Gecko) Chrome/22.0.1216.0 Safari/537.2'
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

def init_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(f'user-agent={user_agent}')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    remote_webdriver = 'http://remote_chromedriver:4444/wd/hub'
    driver = webdriver.Remote(command_executor=remote_webdriver, options=chrome_options)
    print("Webdriver Initialized")
    return driver


def dismiss_modal(driver):
    try:
        close_button = driver.find_element(By.CSS_SELECTOR, ".closeButtonWrapper .CloseButton")
        close_button.click()
        print("Modal closed")
        time.sleep(2)
    except (NoSuchElementException, ElementClickInterceptedException):
        pass

def click_show_more_until_done(driver):
    while True:
        try:
            dismiss_modal(driver)
            show_more_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.JobsList_buttonWrapper__ticwb button"))
            )
            show_more_button.click()
            print("showing more jobs")
            time.sleep(5)  # Sleep to allow for the page to load after clicking
        except (NoSuchElementException, ElementClickInterceptedException, TimeoutException):
            break
    return driver


def click_show_more_job_description(driver):
    try:
        show_more_button = driver.find_element(By.CSS_SELECTOR, "button.JobDetails_showMore___Le6L")
        show_more_button.click()
        time.sleep(2)
    except (NoSuchElementException, ElementClickInterceptedException, TimeoutException):
        pass

def parse_salary(salary_str):
    if salary_str == 'null':
        return [None, None]
    # Identify if the salary string contains a range indicated by "-"
    if "-" in salary_str:
        # Extract the minimum and maximum values from the salary range
        salary_range = salary_str.split('-')
        salary_min = salary_range[0].strip().split(' ')[1]  # Removing currency and extra spaces
        salary_max = salary_range[1].strip().split(' ')[0]  # Removing currency and extra spaces
    else:
        # For salaries without a range, set both min and max to the same value
        salary_parts = salary_str.split(' ')
        salary_min = salary_max = salary_parts[1]
    
    return [salary_min, salary_max]

def interpret_pay_period(pay_period_str):
    if "/mo" in pay_period_str:
        return "month"
    elif "/yr" in pay_period_str:
        return "year"
    elif "/hr" in pay_period_str:
        return "hour"
    else:
        return None

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=1)
}
@dag(
    dag_id="glassdoor_scraper",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['scraper'],
)
def glassdoor_scraper_dag():
    @task
    def scrape_jobs_task():
        max_jobs_to_scrape = 150
        total_job_data = []

        for index in SEARCH_QUERY_LIST:
            total_job_data_in_each_field = []

            job_field = SEARCH_QUERY_LIST[index]
            driver = init_driver()
            driver.get(BASE_URL)
            search_bar = driver.find_element(By.ID, "searchBar-jobTitle")
            search_bar.clear()
            search_bar.send_keys(job_field)
            search_bar.send_keys(Keys.ENTER)
            time.sleep(5)

            print("Now attempting to show all the jobs")
            click_show_more_until_done(driver)
            print("Now attemping to scrape all the jobs listings")

            job_listings = driver.find_elements(By.CSS_SELECTOR, "li.JobsList_jobListItem__wjTHv")[:-1]
            print(f"Total available job listings to scrape: {len(job_listings)}")

            for index, job_listing in enumerate(job_listings):
                if index >= max_jobs_to_scrape:
                    break
                try:            
                    job_listings = driver.find_elements(By.CSS_SELECTOR, "li.JobsList_jobListItem__wjTHv")
                    job_listing = job_listings[index]
                    job_listing.click()
                    click_show_more_job_description(driver)
                    
                    #Scraping logic
                    try:
                        # Targeting the h4 element with its class names
                        employer_name = job_listings[index].find_element(By.CSS_SELECTOR, "h4.heading_Heading__BqX5J.heading_Subhead__Ip1aW").text
                    except NoSuchElementException:
                        employer_name = 'null'

                    try: job_title = job_listings[index].find_element(By.CSS_SELECTOR, "a.JobCard_jobTitle___7I6y").text
                    except NoSuchElementException: job_title = 'null'

                    try:
                        job_link_element = job_listings[index].find_element(By.CSS_SELECTOR, "a.JobCard_jobTitle___7I6y")
                        job_link = job_link_element.get_attribute("href")
                    except NoSuchElementException:
                        job_link = 'null'

                    try: location = job_listings[index].find_element(By.CSS_SELECTOR, "div.JobCard_location__rCz3x").text
                    except NoSuchElementException: location = 'null'

                    try: salary_estimate = job_listings[index].find_element(By.CSS_SELECTOR, "div.JobCard_salaryEstimate__arV5J").text
                    except NoSuchElementException: salary_estimate = 'null'
                    salary_min, salary_max = parse_salary(salary_estimate)

                    try: pay_period_section = driver.find_element(By.CSS_SELECTOR, "div.SalaryEstimate_payPeriod__RsvG_").text
                    except NoSuchElementException: pay_period_section = 'null'
                    pay_period = interpret_pay_period(pay_period_section)


                    job_description_section = driver.find_element(By.CSS_SELECTOR, "div.JobDetails_jobDescription__uW_fK")
                    job_description = job_description_section.get_attribute('innerHTML')

                    company_overview_items = driver.find_elements(By.CSS_SELECTOR, "div.JobDetails_companyOverviewGrid__3t6b4 div.JobDetails_overviewItem__cAsry")
                    company_overview = {item.find_element(By.CSS_SELECTOR, "span.JobDetails_overviewItemLabel__KjFln").text: item.find_element(By.CSS_SELECTOR, "div.JobDetails_overviewItemValue__xn8EF").text for item in company_overview_items}

                    total_job_data_in_each_field.append({
                        "job_title": job_title,
                        "employer_names": employer_name,
                        "location": location,
                        "salary_estimate": salary_estimate,
                        "salary_min": salary_min,
                        "salary_max": salary_max,
                        "salary_freq": pay_period,
                        "description": job_description,
                        "size": company_overview.get("Size", "N/A"),
                        "founded": company_overview.get("Founded", "N/A"),
                        "type": company_overview.get("Type", "N/A"),
                        "industry": company_overview.get("Industry", "N/A"),
                        "sector": company_overview.get("Sector", "N/A"),
                        "revenue": company_overview.get("Revenue", "N/A"),
                        "job_link": job_link,
                        "date_scraped": date_scraped,
                        "field": job_field
                    })

                    time.sleep(2)
                    print(f"job counter {index + 1}")

                except (NoSuchElementException, TimeoutException):
                    time.sleep(5)
                    continue
            total_job_data.extend(total_job_data_in_each_field)
            driver.quit()

        return total_job_data

    @task
    def save_to_csv_task(job_data):
        filename = 'glassdoor_job_listings.csv'
        df = pd.DataFrame(job_data)
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}.")

    # Task dependencies
    job_data = scrape_jobs_task()
    save_to_csv_task(job_data)

dag_instance = glassdoor_scraper_dag()
