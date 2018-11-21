import json
import uuid
from typing import Dict
import pika



class JoowingEventSender(object):
    def __init__(self, config: Dict) -> None:
        super(JoowingEventSender, self).__init__()
        self.config = config

    def send_event(self, event_name, event_memo: Dict) -> None:
        rabbitmq_config = self.config.get("rabbitmq", {})
        host = rabbitmq_config.get("host", None)
        port = rabbitmq_config.get("port", None)
        vhost = rabbitmq_config.get("vhost", None)
        user = rabbitmq_config.get("user", None)
        password = rabbitmq_config.get("pass", None)
        credential = pika.PlainCredentials(username=user, password=password)
        params = pika.ConnectionParameters(
            host=host, port=port, virtual_host=vhost, credentials=credential
        )
        connection = pika.BlockingConnection(parameters=params)
        channel = connection.channel()
        message = {
            "title": event_name,
            "body": event_memo,
            "mime": "json",
            "uuid": str(uuid.uuid1()),
            "targets": []
        }
        channel.basic_publish(exchange='joowing_event',
                              routing_key='',
                              body=message)
        channel.close()

