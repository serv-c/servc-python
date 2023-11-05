import time
import unittest

from servc.com.bus.rabbitmq import BusRabbitMQ
from servc.com.cache.redis import CacheRedis
from servc.com.consumer import ConsumerComponent as Consumer
from servc.com.service import ComponentType
from servc.config import bus_url, cache_url
from servc.io.input import InputPayload, InputType

class TestConsumer(unittest.TestCase):
    def test_plain_message(self):
        bus = BusRabbitMQ(bus_url)
        cache = CacheRedis(cache_url)
        
        route = "test_plain_message_1"
        message_original: InputPayload = {
            "id": "test",
            "type": InputType.INPUT.value,
            "route": route,
            "argumentId": "plain",
            "inputs": {
                "method": "test",
                "inputs": {"message": "test"},
            }
        }

        bus.connect()
        channel = bus._conn.channel()
        channel.queue_declare(route, durable=True, exclusive=False, auto_delete=False)
        channel.close()
        bus.publishMessage(route, message_original, lambda x, y: x)

        consumer = Consumer(
            route,
            route,
            {
                "test": lambda id, bus, cache, children, message, emit: self.assertEqual(message["message"], "test"),
            },
            {},
            lambda x, y, channel, tag: channel.basic_cancel(consumer_tag=tag),
            None,
            bus,
            cache,
        )
        consumer.connect()
        time.sleep(10)


if __name__ == "__main__":
    unittest.main()
