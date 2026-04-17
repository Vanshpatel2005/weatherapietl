import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def check_records_breakdown():
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5433,
            database='weather_warehouse',
            user='postgres',
            password=os.getenv('POSTGRES_PASSWORD', 'vanshdb'),
        )
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM weather_clean;")
        total = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM weather_clean WHERE deleted_at IS NOT NULL;")
        deleted = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM weather_clean wc
            INNER JOIN cities c ON wc.city_id = c.id
            WHERE c.active = TRUE AND wc.deleted_at IS NULL;
        """)
        active_quality_total = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM weather_clean wc
            LEFT JOIN cities c ON wc.city_id = c.id
            WHERE c.id IS NULL;
        """)
        orphaned = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) 
            FROM weather_clean wc
            INNER JOIN cities c ON wc.city_id = c.id
            WHERE c.active = FALSE;
        """)
        inactive_city_records = cur.fetchone()[0]

        print(f"Total in weather_clean: {total}")
        print(f"Soft deleted in weather_clean: {deleted}")
        print(f"Orphaned (no matching city): {orphaned}")
        print(f"Records for Inactive Cities: {inactive_city_records}")
        print(f"Total Active Data Quality records (matches frontend): {active_quality_total}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_records_breakdown()
