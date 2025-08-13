import psycopg2
import json
import logging

# Setup logging with emojis
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
INFO = lambda msg: logging.info(f"🟢 {msg}")
WARN = lambda msg: logging.warning(f"⚠️ {msg}")
ERROR = lambda msg: logging.error(f"❌ {msg}")

DB_CONFIG = {
    "dbname": "kafka_demo_db",
    "user": "kafka_sql_user",
    "password": "kafka_demo",
    "host": "localhost",
    "port": 5432
}

ROUTES_DATA = [
    {
        "xpath": "//stk:StockName",
        "ns": {"stk": "https://www.example.org/stock"},
        "value": "IBM",
        "dest_topic": "ibm-topic"
    },
    {
        "xpath": "//stk:StockName",
        "ns": {"stk": "https://www.example.org/stock"},
        "value": "AAPL",
        "dest_topic": "apple-topic"
    }
]

def get_connection():
    INFO("Connecting to PostgreSQL database...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        INFO("Connection established ✅")
        return conn
    except Exception as e:
        ERROR(f"Failed to connect: {e}")
        raise

def create_table(conn, sql_file="sql/routes.sql"):
    INFO("Creating 'routes' table if not exists...")
    try:
        with open(sql_file, "r") as f:
            sql = f.read()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        INFO("Table ready ✅")
    except Exception as e:
        ERROR(f"Table creation failed: {e}")
        raise

def insert_routes(conn, routes):
    INFO("Inserting routes data...")
    cur = conn.cursor()
    for route in routes:
        cur.execute(
            "INSERT INTO routes (xpath, namespace, value, dest_topic) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (route["xpath"], json.dumps(route["ns"]), route["value"], route["dest_topic"])
        )
    conn.commit()
    cur.close()
    INFO("Data insertion completed ✅")

def read_routes(conn):
    INFO("Reading routes data...")
    cur = conn.cursor()
    cur.execute("SELECT * FROM routes")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    cur.close()
    INFO("Read operation completed ✅")

def main():
    conn = get_connection()
    try:
        create_table(conn)
        insert_routes(conn, ROUTES_DATA)
        read_routes(conn)
    finally:
        conn.close()
        INFO("Connection closed 🔒")

if __name__ == "__main__":
    main()
