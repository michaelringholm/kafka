from zabbix_utils import Sender, ItemValue

# Zabbix server details
# ZABBIX_SERVER = "127.0.0.1"   # or your server hostname/IP
# ZABBIX_PORT = 10051           # default trapper port

# # Host and key must match your Zabbix configuration
# HOSTNAME = "my_kafka_adapter"  # same as in Zabbix frontend
# ITEM_KEY = "queue.depth"       # key of your Zabbix trapper item

# def send_queue_depth(depth: int):
#     """Send Kafka queue depth metric to Zabbix"""
#     sender = ZabbixSender(ZABBIX_SERVER, ZABBIX_PORT)
#     metric = ZabbixMetric(HOSTNAME, ITEM_KEY, depth)

#     result = sender.send([metric])
#     print("✅ Sent metric:", result)



def send_metric_to_zabbix(host: str, key: str, value, zabbix_server="127.0.0.1", port=10051):
    sender = Sender(server=zabbix_server, port=port)
    resp = sender.send_value(host, key, value)
    print("Single metric response:", resp)

def send_batch_metrics(metrics: list, zabbix_server="127.0.0.1", port=10051):
    sender = Sender(server=zabbix_server, port=port)
    items = [ItemValue(m["host"], m["key"], m["value"], m.get("clock")) for m in metrics]
    resp = sender.send(items)
    print("Batch response:", resp)

# Example usage
if __name__ == "__main__":
    send_metric_to_zabbix("my_app", "queue.depth", 42)
