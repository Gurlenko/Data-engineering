import os
import requests
import json


# AUTH_TOKEN = os.environ['AUTH_TOKEN']
AUTH_TOKEN = '2b8d97ce57d401abd89f45b0079d8790edd940e6' #need to remake
API_url = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
API_date = '2022-08-11'
JOB_1_port = 8081
JOB_2_port = 8082
post_url = 'http://localhost:8081/'

def get_data():
    total_data = {}
    page = 1
    while True:
        data = requests.get(url=API_url,
                            params={'date': API_date, 'page': page},
                            headers={'Authorization': AUTH_TOKEN})
        if data.status_code == 200:
            total_data[f'{API_date}_{page}'] = data.json()
            print(f"Response status code on page {page}: ", data.status_code)
            page += 1
        else:
            print(f'Total pages: {page-1}')
            break
    post_data(total_data)

def post_data(data: dict):
    response = requests.post(url=f'http://localhost:{JOB_1_port}/', json=data)
    if response.status_code == 200:
        print("Data successfully posted for Job1.")
    else:
        print("Failed to post data for Job1. Status code:", response.status_code)


def read_send_data(read_dir):
    data = {}
    for json_file in os.listdir(read_dir):
        if json_file.endswith('.json'):
            file_path = os.path.join(read_dir, json_file)
            key = os.path.splitext(json_file)[0]

            with open(file_path, 'r') as json_data:
                file_data = json.load(json_data)
                data[key] = file_data
    response = requests.post(url=f'http://localhost:{JOB_2_port}/', json=data)
    if response.status_code == 200:
        print("Data successfully posted to Job_2.")
    else:
        print("Failed to post data to Job_2. Status code:", response.status_code)
    return data


if __name__ == "__main__":
    get_data()