'''
extracts brand yearly performance from postgreSQL table and save to excel file
'''
import psycopg2
import pandas as pd

def get_connection():
    try:
        return psycopg2.connect(
            database="postgres",
            user="postgres",
            password="Arm@nd40",
            host="127.0.0.1",
            port=5432,
        )
    except:
        return False
conn = get_connection()
if conn:
    print("Connection to the PostgreSQL established successfully.")
else:
    print("Connection to the PostgreSQL encountered and error.")

sql_query = '''
SELECT brand, year, count(*) aASs number_of_reviews FROM review_month_year
WHERE brand = any(array['Amazon Basics', 'SanDisk', 'Apple', 'SAMSUNG', 'Logitech', 'TP-Link', 'Sony', 'Anker', 'UGREEN', 'JBL'])
GROUP BY brand, year
ORDERder BY brand, year;
'''

cur = conn.cursor()

df = pd.read_sql_query(sql_query, con=conn)



conn.commit()
cur.close()
conn.close()

df.to_excel('brand_yearly_performance.xlsx')