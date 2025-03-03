import json
import boto3
import psycopg2
from io import StringIO
#Get apt keys
content = open('/opt/airflow/script/config.json')
config = json.load(content)

def create_pgconnection():
    conn = psycopg2.connect(
        dbname = 'etl_db',
        user = 'admin',
        password = 'admin',
        host = 'main_database',
        port = '5432'
    )

    return conn

CHECKPOINT_FILE = "/opt/airflow/script/checkpoint.txt"
def read_batch_file():
    import os
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as file:
            lines = file.readlines()
            batch_number = lines[1].strip()
            return batch_number

def get_data(conn):
    cur = conn.cursor()

    batch_number = read_batch_file()

    get_data_query = """
        SELECT * FROM product
        WHERE batch = %s
    """

    try:
        cur.execute(get_data_query, (batch_number,))
        rows = cur.fetchall()
        conn.commit()
        return rows

    except Exception as e:
        print(f"An error occurred: {e}")


def load_data_to_s3(rows):
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']
    csv_buffer = StringIO()

    batch_number = read_batch_file()

    for row in rows:
        csv_buffer.write(','.join(map(str, row)))
        csv_buffer.write('\n')

    s3_client = boto3.client(
    's3',
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_access_key,
        region_name = 'us-west-2'
    )

    s3_bucket_name = "pg-to-s3-bucket-davidntd"
    s3_file_path = f"datalake/raw_csv_file/pg_{batch_number}.csv"

    try:
        s3_client.put_object(Bucket = s3_bucket_name, Key = s3_file_path, Body = csv_buffer.getvalue())
        print("Successfully push file into s3 !!!")

    except Exception as e:
        print(f"An error occurred: {e}")

def load_from_db_to_s3():
    conn = create_pgconnection()
    rows = get_data(conn)
    load_data_to_s3(rows)

if __name__ == "__main__":
    pass