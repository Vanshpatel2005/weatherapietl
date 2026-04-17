import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def check_clean_ids():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5433,
            database='weather_warehouse',
            user='postgres',
            password=os.getenv('POSTGRES_PASSWORD', 'vanshdb'),
        )
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT city_id FROM weather_clean;")
        rows = cur.fetchall()
        print("city_id in weather_clean:")
        for r in rows:
            print(r[0])
            
        cur.execute("SELECT COUNT(*) FROM weather_clean;")
        count = cur.fetchone()[0]
        print(f"Total records: {count}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_clean_ids()
