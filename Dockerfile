FROM apache/airflow:2.8.2-python3.9

COPY requirements.txt .

RUN pip install -r requirements.txt

# RUN pip install matplotlib
# RUN pip install kaggle
# RUN pip install beautifulsoup4
# RUN pip install selenium
# RUN pip install pandas