import mysql.connector
import os
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

c = conn.cursor()
c.execute("SELECT AVG(amount_paid) FROM parking;")
average_amount_paid = c.fetchone()[0]
c.execute("SELECT AVG(duration_in_minutes) FROM parking;")
duration_in_minutes = c.fetchone()[0]
print(f"The average amount paid is: {average_amount_paid}")
print(f"The average duration in minutes is: {duration_in_minutes}")

conn.close()