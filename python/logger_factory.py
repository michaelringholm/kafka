import logging

def create_logger(name : str, level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logging.getLogger("kafka").setLevel(logging.WARNING) # Suppress Kafka logs
    logger = logging.getLogger(name)
    return logger