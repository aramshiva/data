import mysql.connector
import os
import csv
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def find_database(db_config):
    conn = mysql.connector.connect(**db_config)
    c = conn.cursor()
    
    database_name = os.getenv('DATABASE_NAME')
    c.execute(f"USE {database_name};")
    conn.commit()
    return conn

def insert_pets_data(conn, data_batch):
    c = conn.cursor()
    insert_query = '''
        INSERT INTO pets (license_issue_date, license_number, animals_name, species, primary_breed, secondary_breed, zip_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''
    data_to_insert = [
        (
            datetime.strptime(data['license_issue_date'], '%B %d %Y').strftime('%Y-%m-%d %H:%M:%S'), data['license_number'], data['animals_name'], data['species'], data['primary_breed'], data['secondary_breed'], data['zip_code']
        )
        for data in data_batch
    ]
    c.executemany(insert_query, data_to_insert)
    conn.commit()
    
def get_pets_data(file_path):
    pets_data = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            pets_data.append({
                'license_issue_date': row['License Issue Date'],
                'license_number': row['License Number'],
                'animals_name': row["Animal's Name"],
                'species': row['Species'],
                'primary_breed': row['Primary Breed'],
                'secondary_breed': row['Secondary Breed'],
                'zip_code': row['ZIP Code']
            })
    return pets_data

def batch_insert_pets_data(conn, pets_data, batch_size=100):
    for i in range(0, len(pets_data), batch_size):
        batch = pets_data[i:i + batch_size]
        insert_pets_data(conn, batch)

pets_data = get_pets_data(os.getenv('DATA_FILE_PATH'))
print(pets_data)

db_config = {'user': os.getenv('DATABASE_USR'),'password': os.getenv('DATABASE_PWD'),'host': os.getenv('DATABASE_HST'),'raise_on_warnings': True}
conn = find_database(db_config)

batch_insert_pets_data(conn, pets_data)

conn.close()

# CREATE TABLE IF NOT EXISTS pets (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     license_issue_date DATETIME NOT NULL,
#     license_number VARCHAR(255) NOT NULL,
#     animals_name VARCHAR(255) NOT NULL,
#     species VARCHAR(255) NOT NULL,
#     primary_breed VARCHAR(255),
#     secondary_breed VARCHAR(255),
#     zip_code VARCHAR(10)
# );