import configparser
import ast
import logging

# Logging with emoji
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
INFO = lambda msg: logging.info(f"🟢 {msg}")
ERROR = lambda msg: logging.error(f"❌ {msg}")

class OPTIONS:
    def __init__(self, ini_path: str):
        self.config = configparser.ConfigParser()
        self.config.read(ini_path)
        self._load_values()

    def _load_values(self):
        section = self.config["DEFAULT"]
        self.KAFKA_BROKER = section.get("KAFKA_BROKER")
        self.KAFKA_TOPIC = section.get("KAFKA_TOPIC")
        self.XSD_SCHEMA_PATH = section.get("XSD_SCHEMA_PATH")
        self.XPATH_EXPR = section.get("XPATH_EXPR")
        self.FLASK_HOST = section.get("FLASK_HOST")
        self.FLASK_PORT = section.getint("FLASK_PORT")
        # Use ast.literal_eval to safely parse dictionary strings
        self.NAMESPACES = ast.literal_eval(section.get("NAMESPACES", "{}"))
        INFO(f"Loaded configuration from {section}")

# Example usage
if __name__ == "__main__":
    options = OPTIONS("config.ini")
    print(options.KAFKA_BROKER)
    print(options.NAMESPACES)
