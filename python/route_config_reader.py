import configparser
import json
from pathlib import Path
import logger_factory

log = logger_factory.create_logger(__name__)

def fetch_routing_matrix(filename: str = "config/routes.ini"):
    config = configparser.ConfigParser()
    if not Path(filename).exists():
        raise FileNotFoundError(f"Config file '{filename}' not found")

    config.read(filename)

    routing_matrix = []
    for section in config.sections():
        ns_raw = config[section].get("ns", "{}")
        try:
            ns = json.loads(ns_raw)
        except json.JSONDecodeError:
            ns = {}
        route = {
            "xpath": config[section].get("xpath", ""),
            "ns": ns,
            "value": config[section].get("value", ""),
            "dest_topic": config[section].get("dest_topic", ""),
        }
        routing_matrix.append(route)

    log.info(f"Fetched {len(routing_matrix)} routes ✅")
    return routing_matrix

# Example usage
if __name__ == "__main__":
    ROUTING_MATRIX = fetch_routing_matrix()
    for r in ROUTING_MATRIX:
        print(r)
