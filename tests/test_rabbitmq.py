import unittest

import pika

from servc.svc.com.bus.rabbitmq import BusRabbitMQ, queue_declare
from servc.svc.config import Config
from servc.svc.io.input import EventPayload, InputType


class TestRabbitMQ(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config = Config()
        cls.bus = BusRabbitMQ(config.get("conf.bus"))

        params = pika.URLParameters(config.get("conf.bus.url"))
        cls.conn = pika.BlockingConnection(params)
        cls.channel = cls.conn.channel()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.bus.close()
        cls.channel.close()
        cls.conn.close()

    def setUp(self) -> None:
        self.bus._prefix = ""
        self.bus._routeMap = {}

    def test_get_route(self):
        route = "test_route"
        prefix = "prefix"
        mapPrefix = {
            "test_route": "test_route2",
            "my_route": "my_route2",
        }

        self.assertEqual(self.bus.getRoute(route), route)

        self.bus._prefix = prefix
        self.assertTrue(self.bus.getRoute(route).startswith(prefix))

        self.bus._routeMap = mapPrefix
        self.assertEqual(self.bus.getRoute(route), "".join([prefix, mapPrefix[route]]))
        self.assertEqual(
            self.bus.getRoute("fake_route"), "".join([prefix, "fake_route"])
        )

    def test_send_message_no_existence(self):
        route = "test_route"
        message = "test_message"

        self.channel.queue_delete(queue=route)
        self.bus.publishMessage(route, message)
        queue = self.channel.queue_declare(
            queue=route, passive=False, durable=True, exclusive=False
        )
        self.assertEqual(queue.method.message_count, 0)

    def test_send_message_existence(self):
        route = "test_route"
        message = "test_message"
        self.channel.queue_delete(queue=route)
        queue_declare(self.channel, route, True)

        self.bus.publishMessage(route, message)
        queue = self.channel.queue_declare(
            queue=route, passive=True, durable=True, exclusive=False
        )
        self.assertEqual(queue.method.message_count, 1)

    def test_fanout_exchange(self):
        routes = ["test_route1", "test_route2", "test_route3"]
        message: EventPayload = {
            "type": InputType.EVENT,
            "route": "test_route",
            "event": "test_event",
            "details": {
                "test": "test",
            },
            "instanceId": "test_instanceId",
        }

        for route in routes:
            self.channel.queue_delete(queue=route)
            queue_declare(self.channel, route, True)
        self.bus.publishMessage("testing", message)

        for route in routes:
            queue = self.channel.queue_declare(
                queue=route, passive=True, durable=True, exclusive=False
            )
            self.assertEqual(queue.method.message_count, 1)
            self.channel.queue_delete(queue=route)

    def test_close_twice(self):
        self.bus.close()
        self.bus.close()

    def test_get_fresh_channel(self):
        self.bus.close()
        self.bus.get_channel(None, None)

    def test_nonexistent_queue_length(self):
        self.assertEqual(self.bus.get_queue_length("test_queue"), 0)

    def test_existent_queue_length(self):
        route = "test_route"
        self.bus.delete_queue(route)
        self.bus.create_queue(route, False)

        self.assertEqual(self.bus.get_queue_length(route), 0)

        self.bus.publishMessage(route, "test_message")
        self.assertEqual(self.bus.get_queue_length(route), 1)

        self.bus.delete_queue(route)


if __name__ == "__main__":
    unittest.main()
