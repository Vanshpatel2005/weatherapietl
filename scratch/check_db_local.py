import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def check_cities():
    try:
        conn = psycopg2.connect(
            host='localhost', # Hardcode for local check
            port=5432,
            database=os.getenv('POSTGRES_DB', 'weather_warehouse'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD'),
        )
        cur = conn.cursor()
        cur.execute("SELECT id, city_name, country_code, state FROM cities;")
        rows = cur.fetchall()
        print("ID | City | Country | State")
        print("-" * 30)
        for r in rows:
            print(f"{r[0]} | {r[1]} | {r[2]} | {r[3]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_cities()
