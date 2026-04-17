import psycopg2
from config.config import Config

def check_cities():
    try:
        conn = psycopg2.connect(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD,
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
