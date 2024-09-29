import pandas as pd
from datetime import datetime, timedelta
import os
import requests
import time
from requests.exceptions import Timeout, RequestException
import json
from google.cloud import bigquery
from google.oauth2 import service_account

def get_bq_credentials(envName: str):
    service_account_info = os.getenv(envName)
    if service_account_info is None:
        raise Exception(
            "No service account info found in connection " + envName
        )
    return service_account.Credentials.from_service_account_info(
        json.loads(service_account_info)
    )

def upload_to_bq(dataframe,project,dataset,table,credentials,if_exists):
  dataframe.to_gbq(f'{dataset}.{table}',f'{project}',credentials=credentials,if_exists=if_exists)
  return print('File is uploaded')

def get_access_token(client_id, client_secret):
    url = 'https://reporting.fyber.com/auth/v1/token'
    headers = {'Content-Type': 'application/json'}
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.json()['accessToken']

def create_report(access_token, report_params, timeout=300):
    url = 'https://reporting.fyber.com/api/v1/report?format=csv'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    try:
        response = requests.post(url, json=report_params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Timeout:
        raise Timeout("The request timed out")
    except RequestException as e:
        raise RequestException(f"Request failed: {e}")

ds = datetime.today()
start_date = ds + timedelta(days=-44)
start_date = start_date.strftime('%Y-%m-%d')
end_date = ds.strftime('%Y-%m-%d')

client_id = "your_client_id"
client_secret = "your_client_secret"

report_params = {
    "source": "mediation",
    "dateRange": {
        "start": f"{start_date}",
        "end": f"{end_date}"
    },
    "metrics": [
        "Revenue (USD)"
    ],
    "splits": [
        "Date",
        "Fyber App ID",
        "App Name",
        "Device OS"
    ],
    "filters": []
}

def retry_create_report(access_token, report_params, max_retries=5, delay=5, timeout=10):
    for attempt in range(max_retries):
        try:
            report_response = create_report(access_token, report_params, timeout=timeout)
            return report_response
        except Timeout:
            print(f"Attempt {attempt + 1} timed out")
        except RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
        time.sleep(delay)
    
    raise Exception("Max retries exceeded")

token = get_access_token(client_id, client_secret)

try:
    report_response = retry_create_report(token, report_params)
    print("Report created successfully:", report_response)
except Exception as e:
    print("Failed to create report:", e)

report_url = report_response['url']
response = requests.get(report_url)

if response.status_code == 200:
    with open('report.csv', 'wb') as file:
        file.write(response.content)
    print('Report downloaded successfully.')
else:
    print('Failed to download the report.')

df = pd.read_csv('report.csv')

def convert_to_snake_case(text):
    import re
    text = re.sub(r'[\s\W]+', '_', text)
    return text.lower()

rev_col = []

for col in df.columns:
    rev_col.append(convert_to_snake_case(col))

df.columns = rev_col

project = 'your_project'
dataset = 'your_dataset'
table = 'your_raw_table'

credentials = get_bq_credentials('service_account_secrets.json')

upload_to_bq(df,project,dataset,table,credentials,'replace')
