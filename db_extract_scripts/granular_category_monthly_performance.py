'''
extracts granular category monthly performance from postgreSQL table and save to excel file
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
SELECT granular_category, month, count(*) AS number_of_reviews FROM review_month_year
WHERE granular_category = any(array['Cases', 'Earbud Headphones', 'Lightning Cables', 'USB Cables', 'Micro SD Cards', 'Arm & Wristband Accessories', 'Portable Bluetooth Speakers', 'Remote Controls', 'Mice', 'USB Flash Drives'])
GROUP BY granular_category, month
ORDER BY granular_category, month;
'''

cur = conn.cursor()

df = pd.read_sql_query(sql_query, con=conn)



conn.commit()
cur.close()
conn.close()

df.to_excel('granular_category_monthly_performance.xlsx')