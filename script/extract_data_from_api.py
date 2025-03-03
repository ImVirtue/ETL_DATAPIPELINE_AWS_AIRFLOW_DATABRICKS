import psycopg2
import os
import random
from math import ceil

CHECKPOINT_FILE = "/opt/airflow/script/checkpoint.txt"
# CHECKPOINT_FILE = 'checkpoint.txt'

def create_pgconnection():
    conn = psycopg2.connect(
        dbname = 'etl_db',
        user = 'admin',
        password = 'admin',
        host = 'main_database',
        # host = 'localhost',
        port = '5432'
    )

    return conn

def check_create_table(conn):
    cur = conn.cursor()

    create_table_query = """
           CREATE TABLE IF NOT EXISTS product (
               id integer primary key,
               title varchar(50),
               category varchar(50),
               total_sold integer,
               percent_discount DOUBLE PRECISION,
               price DOUBLE PRECISION,
               stock integer,
               brand varchar(50),
               weight DOUBLE PRECISION,
               width DOUBLE PRECISION,
               height DOUBLE PRECISION,
               comment TEXT[],
               avg_rating DOUBLE PRECISION,
               image_links TEXT[],
               batch integer
           )
       """
    try:
        cur.execute(create_table_query)
        conn.commit()
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def insert_data(conn, data):
    cur = conn.cursor()

    insert_query = """
        INSERT INTO product (id, title, category, total_sold, percent_discount, price, stock, 
                             brand, weight, width, height, comment, avg_rating, image_links, batch)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        cur.execute(insert_query, data)
        conn.commit()
        print('Successfully inserted')

    except Exception as e:
        print(f"An error occurred, {e}")



def write_to_checkpoint_file(check_number, batch_numer):
    with open(CHECKPOINT_FILE, "w") as file:
        file.write(str(check_number) + "\n")
        file.write(str(batch_numer) + "\n")

def load_to_checkpoint_file():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as file:
            number = file.readline().strip()
            if number.isdigit():
                print(number)
                return number
            else:
                print("empty file exists!")
                return 1
    else:
        with open(CHECKPOINT_FILE, "w") as file:
            file.write("1")
        print("Successfully created file!")
        return 1

def extract_and_transform_data():
    import requests
    base_url = 'https://dummyjson.com/products'

    checkpoint_number = load_to_checkpoint_file()

    url = f"{base_url}/{checkpoint_number}"
    data = requests.get(url)
    data = data.json()
    # print(data)

    checkpoint_number = int(checkpoint_number) + 1
    print(checkpoint_number)
    write_to_checkpoint_file(checkpoint_number, ceil(data.get('id') / 10))

    comment_list = []
    total_rating_from_cus = 0
    total_of_reviews = len(data['reviews'])

    for i in range(total_of_reviews):
        comment_list.append(data['reviews'][i]['comment'])
        total_rating_from_cus += data['reviews'][i]['rating']

    avg_rating_from_cus = total_rating_from_cus / total_of_reviews

    total_sold = 0
    if avg_rating_from_cus <= 3.5:
        total_sold = random.randint(300, 400)
    elif avg_rating_from_cus > 3.5 and avg_rating_from_cus <= 4.5:
        total_sold = random.randint(1000, 2000)
    else:
        total_sold = random.randint(5000, 10000)

    return (
        data.get('id'),
        data.get('title'),
        data.get('category'),
        total_sold,
        data.get('discountPercentage'),
        data.get('price'),
        data.get('stock'),
        data.get('brand'),  # Nếu 'brand' không tồn tại, trả về None
        data.get('weight'),
        data.get('dimensions', {}).get('width'),  # Xử lý nested dictionary
        data.get('dimensions', {}).get('height'),  # Xử lý nested dictionary
        comment_list,
        round(avg_rating_from_cus, 2),
        data.get('images'),
        ceil(data.get('id') / 10)
    )


def load_to_db(data):
    conn = create_pgconnection()
    try:
        if check_create_table(conn):
            insert_data(conn, data)
        else:
            print('Cannot load due to table')
    except Exception as e:
        print(f'Cannot load to db. An error occurred, {e}')

def etl():
    for i in range(10):
        data = extract_and_transform_data()
        load_to_db(data)

if __name__ == '__main__':
    for i in range(3):
        etl()
    # pass

