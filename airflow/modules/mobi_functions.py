import pandas as pd
import sys
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import gdown
import logging

LOGGER = logging.getLogger("airflow.task")

from airflow.hooks.S3_hook import S3Hook

import configparser
import pathlib
import psycopg2
from psycopg2 import sql

script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/configuration.conf")

USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_hostname")
PORT = parser.get("aws_config", "redshift_port")
REDSHIFT_ROLE = parser.get("aws_config", "redshift_role")
DATABASE = parser.get("aws_config", "redshift_database")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
ACCOUNT_ID = parser.get("aws_config", "account_id")


def download_link(**kwargs):
    """Downloads a single csv monthly by parsing the Mobi website and 
    downloading the google drive link.

    Arguments:
    start_month -- start of month data; inclusive (yyyy-mm)
    end_month -- end of month data; inclusive (yyyy-mm)
    """

    response = requests.get("https://www.mobibikes.ca/en/system-data")
    soup = BeautifulSoup(response.text, 'html.parser')

    execution_month = (kwargs['logical_date'].replace(day=1) - timedelta(days=1)).strftime("%Y-%m")

    # # find all <li> elements with class p1
    links = soup.find_all("li", class_="p1")
    for link in links:
        try:
            # convert month to datetime format
            if 'ALL' in link.get_text():
                # 2017 data is all combined to just use 2017-12
                month = datetime.strptime('DECEMBER 2017', "%B %Y").strftime('%Y-%m')
            else:
                month = datetime.strptime(link.get_text(), "%B %Y").strftime('%Y-%m')

            if 'google' in link.find('a').get('href') and (execution_month == month):
                url = link.find('a').get('href')
                print(f'retreiving link for {month}')

        except:
            # to parse out elements without links
            pass

    output = f'/tmp/{execution_month}.csv'

    # use requests if spreadsheet
    if 'spreadsheets' in url:
        # adjust to download spreadsheet as csv
        pattern = r'(.+/d/[^/]+/)'
        url = re.search(pattern, url)[0]
        url = url + 'export?format=csv' 

        response = requests.get(url)
        with open(output, 'wb') as f:
            f.write(response.content)
    else:
        gdown.download(url, output, fuzzy = True, quiet=False)

def upload_to_s3(**kwargs):
    execution_month = (kwargs['logical_date'].replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    
    hook = S3Hook()
    hook.load_file(
        filename=f"/tmp/{execution_month}.csv",
        key=f'{execution_month}.csv',
        bucket_name=BUCKET_NAME,
        replace=True
    )

def copy_to_redshift(**kwargs):
    execution_month = (kwargs['logical_date'].replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    file_path = f's3://{BUCKET_NAME}/{execution_month}.csv'
    role_string = f"arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_ROLE}"
    try:
        rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
        )
    except Exception as e:
        print(f"Unable to connect to Redshift. Error {e}")
        sys.exit(1)
    with rs_conn:
        cur = rs_conn.cursor()
        cur.execute(open(f"{script_path}/sql/create_raw_and_stage.sql", "r").read())
        LOGGER.info("airflow.task >>> 2 - INFO logger test")
        cur.execute(f"COPY staging_table FROM '{file_path}' IAM_ROLE '{role_string}' IGNOREHEADER 1 DELIMITER ',' DATEFORMAT 'auto' CSV;")
        cur.execute(open(f"{script_path}/sql/clean_stage.sql", "r").read())
        cur.execute(open(f"{script_path}/sql/insert_and_drop_stage.sql", "r").read())
        rs_conn.commit()
        