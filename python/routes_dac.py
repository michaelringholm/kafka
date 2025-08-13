import psycopg2
import json
import logging

# Logging with emoji
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
INFO = lambda msg: logging.info(f"🟢 {msg}")
ERROR = lambda msg: logging.error(f"❌ {msg}")

DB_CONFIG = {
    "dbname": "kafka_demo_db",
    "user": "kafka_sql_user",
    "password": "kafka_demo",
    "host": "localhost",
    "port": 5432
}

def get_connection():
    INFO("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    INFO("Connection established ✅")
    return conn

def fetch_routing_matrix():
    """Reads the 'routes' table and returns a list of dicts like ROUTING_MATRIX."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT xpath, namespace, value, dest_topic FROM routes")
        rows = cur.fetchall()
        cur.close()
        
        routing_matrix = []
        for xpath, namespace, value, dest_topic in rows:
            routing_matrix.append({
                "xpath": xpath,
                "ns": namespace,  # instead of json.loads(namespace_json)
                "value": value,
                "dest_topic": dest_topic
            })
        INFO(f"Fetched {len(routing_matrix)} routes ✅")
        return routing_matrix
    finally:
        conn.close()
        INFO("Connection closed 🔒")

# Example usage
if __name__ == "__main__":
    ROUTING_MATRIX = fetch_routing_matrix()
    for r in ROUTING_MATRIX:
        print(r)
