from kafka import KafkaProducer

from process.model.retailer_config import DatabaseConfig
from process.service.base_service import BaseService
from process.util.util import name_with_prefix

class KafkaExecutor(object):
    def __init__(self, config: dict):
        super(KafkaExecutor, self).__init__()
        self.config = config

    def send_message(self, topic_name: str, msg: str, key: str = None):
        env_prefix = self.config.get("prefix", None)
        topic_name = name_with_prefix(env_prefix, topic_name, seperator="_")
        bootstrap_servers = self.config["kafka"]["bootstrap_servers"]
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        real_value = msg.encode("utf-8")
        print(topic_name)
        print(real_value)

        if key is None:
            future = producer.send(topic_name, value=real_value)
        else:
            future = producer.send(topic_name, key=key, value=real_value)
        future.get(20)

if __name__ == "__main__":
    maintenance_session = BaseService.initialize_maintenance_objects()
    database_config = DatabaseConfig.current_config(maintenance_session)
    kfk = KafkaExecutor(database_config)
    kfk.send_message(topic_name="fast_ss_notify", msg="test_kafka")
