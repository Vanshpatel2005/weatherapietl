import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def repair_cities():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5433,
            database='weather_warehouse',
            user='postgres',
            password=os.getenv('POSTGRES_PASSWORD', 'vanshdb'),
        )
        cur = conn.cursor()
        
        # 1. Update cities table using data from weather_clean
        # This will fix any cities that already have clean records with correct country codes
        print(">>> Attempting to sync country_code from weather_clean to cities table...")
        cur.execute("""
            UPDATE cities c
            SET country_code = sub.country_code
            FROM (
                SELECT DISTINCT ON (city_id) city_id, country_code
                FROM weather_clean
                WHERE country_code IS NOT NULL
                ORDER BY city_id, recorded_at DESC
            ) sub
            WHERE c.id = sub.city_id
              AND c.country_code != sub.country_code;
        """)
        print(f"Rows updated: {cur.rowcount}")
        
        # 2. Manual fixes for known incorrect ones if they haven't been fetched yet
        print(">>> Applying manual fixes...")
        manual_fixes = [
            ("Kyiv", "UA"),
            ("New York", "US"),
            ("Romania", "RO"),
            ("London", "GB"),
            ("Tokyo", "JP"),
            ("Paris", "FR"),
            ("Sydney", "AU")
        ]
        for city, country in manual_fixes:
            cur.execute("UPDATE cities SET country_code = %s WHERE city_name = %s AND country_code = 'IN';", (country, city))
            if cur.rowcount > 0:
                print(f"Fixed {city} -> {country}")
        
        conn.commit()
        cur.close()
        conn.close()
        print("✓ Repair complete")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    repair_cities()
