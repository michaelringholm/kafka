import configparser

def fetch_transformations():
    CONFIG_FILE = "config/transformation_config.ini"

    # Load INI
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    # Convert TRANSFORMATIONS section into a dict
    TRANSFORMATIONS = dict(config["TRANSFORMATIONS"])
    return TRANSFORMATIONS

# Example usage
if __name__ == "__main__":
    print(f"{fetch_transformations()}")
