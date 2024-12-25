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

def insert_ev_data(conn, data_batch):
    c = conn.cursor()
    insert_query = '''
        INSERT INTO electric_vehicles (vin_prefix, county, city, state, postal_code, model_year, make, model, electric_vehicle_type, cafv_eligibility, electric_range, base_msrp, legislative_district, dol_vehicle_id, vehicle_location, electric_utility, census_tract)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    data_to_insert = [
        (
            data['vin_prefix'], data['county'], data['city'], data['state'], data['postal_code'], data['model_year'], data['make'], data['model'], data['electric_vehicle_type'], data['cafv_eligibility'], data['electric_range'], data['base_msrp'], data['legislative_district'], data['dol_vehicle_id'], data['vehicle_location'], data['electric_utility'], data['census_tract']
        )
        for data in data_batch
    ]
    c.executemany(insert_query, data_to_insert)
    conn.commit()
    
def get_ev_data(file_path):
    ev_data = []
    with open(file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            ev_data.append({
                'vin_prefix': row['VIN (1-10)'],
                'county': row['County'],
                'city': row['City'],
                'state': row['State'],
                'postal_code': row['Postal Code'],
                'model_year': row['Model Year'],
                'make': row['Make'],
                'model': row['Model'],
                'electric_vehicle_type': row['Electric Vehicle Type'],
                'cafv_eligibility': row['Clean Alternative Fuel Vehicle (CAFV) Eligibility'],
                'electric_range': row['Electric Range'],
                'base_msrp': row['Base MSRP'],
                'legislative_district': row['Legislative District'],
                'dol_vehicle_id': row['DOL Vehicle ID'],
                'vehicle_location': row['Vehicle Location'],
                'electric_utility': row['Electric Utility'],
                'census_tract': row['2020 Census Tract']
            })
    return ev_data

def batch_insert_ev_data(conn, ev_data, batch_size=100):
    for i in range(0, len(ev_data), batch_size):
        batch = ev_data[i:i + batch_size]
        insert_ev_data(conn, batch)

ev_data = get_ev_data(os.getenv('DATA_FILE_PATH'))
print(ev_data)

db_config = {'user': os.getenv('DATABASE_USR'),'password': os.getenv('DATABASE_PWD'),'host': os.getenv('DATABASE_HST'),'raise_on_warnings': True}
conn = find_database(db_config)

batch_insert_ev_data(conn, ev_data)

conn.close()

# CREATE TABLE IF NOT EXISTS electric_vehicles (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     vin_prefix VARCHAR(10),
#     county VARCHAR(100),
#     city VARCHAR(100),
#     state VARCHAR(2),
#     postal_code VARCHAR(10),
#     model_year INT,
#     make VARCHAR(100),
#     model VARCHAR(100),
#     electric_vehicle_type VARCHAR(100),
#     cafv_eligibility VARCHAR(100),
#     electric_range INT,
#     base_msrp DECIMAL(10,2),
#     legislative_district VARCHAR(50),
#     dol_vehicle_id VARCHAR(100),
#     vehicle_location VARCHAR(255),
#     electric_utility VARCHAR(255),
#     census_tract VARCHAR(100)
# );