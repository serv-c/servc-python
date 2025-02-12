import unittest

import pika

from servc.svc.com.bus.rabbitmq import BusRabbitMQ
from servc.svc.com.cache.redis import CacheRedis
from servc.svc.com.worker.hooks.oncomplete import process_complete_hook
from servc.svc.config import Config
from servc.svc.io.hooks import CompleteHookType
from servc.svc.io.input import ArgumentArtifact, InputPayload, InputType
from tests import get_route_message

message: InputPayload = {
    "id": "123",
    "type": InputType.INPUT.value,
    "route": "test",
    "argumentId": "",
}
art: ArgumentArtifact = {
    "method": "test",
    "inputs": {"id": "123"},
    "hooks": {
        "on_complete": [
            {
                "type": CompleteHookType.SENDMESSAGE,
                "method": "test",
                "route": "random",
            }
        ]
    },
}


class TestCompleteHook(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config = Config()
        cls.bus = BusRabbitMQ(config.get("conf.bus"))
        cls.cache = CacheRedis(config.get("conf.cache"))

        params = pika.URLParameters(config.get("conf.bus.url"))
        cls.conn = pika.BlockingConnection(params)
        cls.channel = cls.conn.channel()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.bus.delete_queue("random")
        cls.cache.close()
        cls.bus.close()
        cls.channel.close()
        cls.conn.close()

    def setUp(self):
        self.bus.create_queue("random", False)

    def tearDown(self):
        self.bus.delete_queue("random")

    def test_complete_hook_simple(self):
        res = process_complete_hook(
            self.bus, self.cache, message, art, art["hooks"]["on_complete"][0]
        )
        body, _ = get_route_message(self.channel, self.cache, "random")

        self.assertTrue(body["argument"]["inputs"]["inputs"], art["inputs"])
        self.assertTrue(res)

    def test_w_hook_override(self):
        hook = {
            "type": CompleteHookType.SENDMESSAGE,
            "method": "test",
            "route": "random",
            "inputs": True,
        }
        res = process_complete_hook(self.bus, self.cache, message, art, hook)
        body, _ = get_route_message(self.channel, self.cache, "random")

        self.assertTrue(body["argument"]["inputs"], True)
        self.assertTrue(res)


if __name__ == "__main__":
    unittest.main()
