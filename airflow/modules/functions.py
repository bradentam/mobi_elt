import pandas as pd
import sys
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import gdown

import boto3

# no secret key
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
    gdown.download(url, output, fuzzy = True, quiet=False)

def upload_to_s3(**kwargs):
    execution_month = (kwargs['logical_date'].replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    
    conn = boto3.resource("s3")
    conn.meta.client.upload_file(
        Filename=f"/tmp/{execution_month}.csv", 
        Bucket='bradentam-test-bucket', Key=f'{execution_month}.csv'
    )