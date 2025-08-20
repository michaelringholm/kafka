from zabbix_utils.sender import ZabbixSender, ZabbixMetric

# Zabbix server details
ZABBIX_SERVER = "127.0.0.1"   # or your server hostname/IP
ZABBIX_PORT = 10051           # default trapper port

# Host and key must match your Zabbix configuration
HOSTNAME = "my_kafka_adapter"  # same as in Zabbix frontend
ITEM_KEY = "queue.depth"       # key of your Zabbix trapper item

def send_queue_depth(depth: int):
    """Send Kafka queue depth metric to Zabbix"""
    sender = ZabbixSender(ZABBIX_SERVER, ZABBIX_PORT)
    metric = ZabbixMetric(HOSTNAME, ITEM_KEY, depth)

    result = sender.send([metric])
    print("✅ Sent metric:", result)

if __name__ == "__main__":
    # Example: pretend we measured queue depth = 42
    send_queue_depth(42)
