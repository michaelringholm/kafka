from protocols.metric_logger import MetricLogger
from zabbix_logger import ZabbixLogger

def test_send_metrics():
    metric_logger:MetricLogger = ZabbixLogger("config/metric_logger.ini")
    metric_logger.send("queue.depth", 42)
    metric_logger.send("err.cnt", 3)
    assert True 