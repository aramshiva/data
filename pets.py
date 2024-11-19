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

db_config = {'user': os.getenv('DATABASE_USR'),'password': os.getenv('DATABASE_PWD'),'host': os.getenv('DATABASE_HST'),'raise_on_warnings': True}

conn = find_database(db_config)

conn.close()