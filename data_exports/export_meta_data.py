'''
transfers jasonl item metadata to a postgreSQL table called ITEM_METADATA
'''
import json
import psycopg2

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

cur = conn.cursor()

file = "meta_Electronics.jsonl"

with open(file, 'r') as fp:

    for line in fp:

        data = json.loads(line)
        try:
            granular_cat = data['categories'][-1]
        except:
            granular_cat = 'unknown'

        store = data['store']
        if type(store) is str and len(store) > 50:
            store = 'unknown'

        try:
            brand = data['details']['Brand']
        except:
            brand = 'unknown'


        cur.execute('''INSERT INTO ITEM_METADATA (parent_asin, main_category, brand, average_rating, rating_number, price, store_name, granular_category)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s);''',
                (data['parent_asin'], data['main_category'], brand, data['average_rating'], data['rating_number'], data['price'], store, granular_cat))
        conn.commit()

        


cur.close()
conn.close()

