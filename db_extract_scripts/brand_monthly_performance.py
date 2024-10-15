'''
extracts brand monthly performance from postgreSQL table and save to excel file
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
SELECT brand, month, count(*) AS number_of_reviews FROM review_month_year
WHERE brand = ANY(ARRAY['Amazon Basics', 'SanDisk', 'Apple', 'SAMSUNG', 'Logitech', 'TP-Link', 'Sony', 'Anker', 'UGREEN', 'JBL'])
GROUP BY brand, month
ORDER BY brand, month;
'''

cur = conn.cursor()

df = pd.read_sql_query(sql_query, con=conn)



conn.commit()
cur.close()
conn.close()

df.to_excel('brand_monthly_performance.xlsx')