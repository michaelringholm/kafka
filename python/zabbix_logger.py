import logging
import configparser
from zabbix_utils import Sender, ItemValue

# logging setup
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
log = logging.getLogger("metric_logger")

class ZabbixLogger:
    def __init__(self, config_path="metric_logger.ini"):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)

        # read zabbix params
        self.server = self.config.get("zabbix", "server", fallback="127.0.0.1")
        self.port = self.config.getint("zabbix", "port", fallback=10051)
        self.hostname = self.config.get("zabbix", "hostname", fallback="default_host")

        log.info("⚙️ MetricLogger initialized with server=%s:%s host=%s", self.server, self.port, self.hostname)
        self.sender = Sender(server=self.server, port=self.port)

    def send(self, key: str, value):
        """Send a single metric to Zabbix"""
        resp = self.sender.send_value(self.hostname, key, value)
        log.info("📤 Sent metric [%s=%s] → %s:%s (resp=%s)", key, value, self.server, self.port, resp)
        return resp

    def send_batch(self, metrics: list):
        """
        Send batch metrics.
        metrics = [{"key": "queue.depth", "value": 42}, ...]
        """
        items = [
            ItemValue(self.hostname, m["key"], m["value"], m.get("clock"))
            for m in metrics
        ]
        resp = self.sender.send(items)
        log.info("📦 Sent batch metrics (%d items) → %s:%s (resp=%s)", len(items), self.server, self.port, resp)
        return resp


# Example usage
if __name__ == "__main__":
    ml = ZabbixLogger("config/metric_logger.ini")

    # single metric
    ml.send_metric("queue.depth", 42)

    # batch metrics
    ml.send_batch([
        {"key": "queue.depth", "value": 42},
        {"key": "kafka.lag", "value": 5},
    ])
