'''
transfers jasonl item review data to a postgreSQL table called item_reviews
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

file = "Electronics.jsonl"

with open(file, 'r') as fp:

    for line in fp:

        data = json.loads(line)
        
        cur.execute('''INSERT INTO item_reviews (rating, asin, parent_asin, user_id, time_stamp, verified_purchase, helpful_vote)
                VALUES (%s,%s,%s,%s,%s,%s,%s);''',
                (data['rating'], data['asin'], data['parent_asin'], data['user_id'], data['timestamp'], data['verified_purchase'], data['helpful_vote']))


conn.commit()
cur.close()
conn.close()

