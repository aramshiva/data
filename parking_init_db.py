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

def insert_parking_data(conn, data_batch):
    c = conn.cursor()
    insert_query = '''
        INSERT INTO parking (transaction_id, meter_code, transaction_datetime, payment_mean, amount_paid, duration_in_minutes, blockface_name, side_of_street, element_key, parking_space_number, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    data_to_insert = [
        (
            data['transaction_id'], data['meter_code'], datetime.strptime(data['transaction_datetime'], '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S'), data['payment_mean'], data['amount_paid'], data['duration_in_minutes'], data['blockface_name'], data['side_of_street'], data['element_key'], data['parking_space_number'], float(data['latitude']), float(data['longitude'])
        )
        for data in data_batch
    ]
    c.executemany(insert_query, data_to_insert)
    conn.commit()
    
def get_parking_data(file_path):
    parking_data = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            parking_data.append({
                'transaction_id': row['Transaction ID'],
                'meter_code': row['Meter Code'],
                'transaction_datetime': row['Transaction DateTime'],
                'payment_mean': row['Payment Mean'],
                'amount_paid': row['Amount Paid'],
                'duration_in_minutes': row['Duration In Minutes'],
                'blockface_name': row['Blockface Name'],
                'side_of_street': row['Side Of Street'],
                'element_key': row['Element key'],
                'parking_space_number': row['Parking Space Number'],
                'latitude': row['Latitude'],
                'longitude': row['Longitude']
            })
    return parking_data

def batch_insert_parking_data(conn, parking_data, batch_size=100):
    for i in range(0, len(parking_data), batch_size):
        batch = parking_data[i:i + batch_size]
        insert_parking_data(conn, batch)

parking_data = get_parking_data(os.getenv('DATA_FILE_PATH'))
print(parking_data)

db_config = {'user': os.getenv('DATABASE_USR'),'password': os.getenv('DATABASE_PWD'),'host': os.getenv('DATABASE_HST'),'raise_on_warnings': True}
conn = find_database(db_config)

batch_insert_parking_data(conn, parking_data)

conn.close()

# CREATE TABLE IF NOT EXISTS parking (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     transaction_id INT NOT NULL,
#     meter_code VARCHAR(255) NOT NULL,
#     transaction_datetime DATETIME NOT NULL,
#     payment_mean VARCHAR(255) NOT NULL,
#     amount_paid DECIMAL(10, 2) NOT NULL,
#     duration_in_minutes INT NOT NULL,
#     blockface_name VARCHAR(255) NOT NULL,
#     side_of_street VARCHAR(255) NOT NULL,
#     element_key VARCHAR(255) NOT NULL,
#     parking_space_number VARCHAR(255) NOT NULL,
#     latitude DECIMAL(10, 8) NOT NULL,
#     longitude DECIMAL(11, 8) NOT NULL
# );