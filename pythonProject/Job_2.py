from flask import Flask, request
import os
import fastavro.schema as avro_schema
from fastavro import writer


app = Flask(__name__)

BASE_RAW_DATA = '/Users/admin/Desktop/Take data from API/pythonProject/file_storage/raw/sales' #need to remake
BASE_STG = '/Users/admin/Desktop/Take data from API/pythonProject/'

@app.route('/', methods=['Post'])
def avro_post_request():
    if request.method == 'POST':
        data = request.json
        print("Received data:", data)

        file_storage = os.path.join(BASE_STG, 'file_storage')
        raw_dir = os.path.join(file_storage, 'stg')
        sales_stg = os.path.join(raw_dir, 'sales')

        if os.path.exists(sales_stg):
            for file_name in os.listdir(sales_stg):
                file_path = os.path.join(sales_stg, file_name)
                os.remove(file_path)

        os.makedirs(sales_stg, exist_ok=True)

        write_avro_data(data, sales_stg)

        return "Data received and saved successfully", 200
    else:
        return "Only POST requests are allowed", 405

def write_avro_data(data, output_dir):
    schema = {
        "type": "record",
        "name": "Purchase",
        "fields": [
            {"name": "client", "type": "string"},
            {"name": "purchase_date", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "int"}
        ]
    }

    for key, value in data.items():
        avro_file_path = os.path.join(output_dir, f'{key}.avro')


        if isinstance(value, list):
            with open(avro_file_path, 'wb') as avro_file:
                writer(avro_file, schema, value)
        else:
            print(f"Value for key '{key}' is not a list of records.")


if __name__ == "__main__":
    app.run(port=8082, debug=True)