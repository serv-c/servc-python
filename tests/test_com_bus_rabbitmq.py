import unittest

from servc.com.bus.rabbitmq import BusRabbitMQ
from servc.com.service import ComponentType
from servc.config import bus_url
from servc.io.input import InputType


def inputProcessor(self, message):
    self.assertEqual(message["message"], "test")
    self.assertEqual(message["type"], InputType.INPUT.value)
    return True


class TestRabbitMQ(unittest.TestCase):
    def setUp(self):
        self.bus = BusRabbitMQ(bus_url)

    def tearDown(self):
        self.bus.close()

    def test_component_type(self):
        result = self.bus.type
        self.assertEqual(result, ComponentType.BUS)

    def test_publish_message(self):
        route = "test"
        message = {
            "type": InputType.INPUT.value,
            "message": "test",
            "route": route,
        }
        self.bus.connect()
        channel = self.bus._conn.channel()
        channel.queue_declare(route, durable=True, exclusive=False, auto_delete=False)
        channel.close()

        self.bus.publishMessage(route, message, lambda x, y: x)
        self.bus.subscribe(
            route,
            lambda m: inputProcessor(self, m),
            lambda x, y, channel, tag: channel.basic_cancel(consumer_tag=tag),
            lambda j: True,
        )


if __name__ == "__main__":
    unittest.main()
