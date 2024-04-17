from flask import Flask, request
import os
import json
from data_sender import read_send_data


BASE_PATH = '/Users/admin/Desktop/Take data from API/pythonProject' #need to remake
app = Flask(__name__)


@app.route('/', methods=['POST'])
def handle_post_request():
    if request.method == 'POST':
        data = request.json
        print("Received data:", data)

        file_storage = os.path.join(BASE_PATH, 'file_storage')
        raw_dir = os.path.join(file_storage, 'raw')
        sales_raw = os.path.join(raw_dir, 'sales')

        if os.path.exists(sales_raw):
            for file_name in os.listdir(sales_raw):
                file_path = os.path.join(sales_raw, file_name)
                os.remove(file_path)

        os.makedirs(sales_raw, exist_ok=True)

        for key, value in data.items():
            file_path = os.path.join(sales_raw, f'{key}.json')
            with open(file_path, 'w') as file:
                json.dump(value, file)

        read_send_data(sales_raw)
        return "Data received and saved successfully for Job1", 200
    else:
        return "Only POST requests are allowed on Job1", 405



if __name__ == "__main__":
    app.run(port=8081, debug=True)