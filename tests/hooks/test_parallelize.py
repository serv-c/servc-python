import json
import unittest

import pika

from servc.svc.com.bus.rabbitmq import BusRabbitMQ
from servc.svc.com.cache.redis import CacheRedis
from servc.svc.com.worker.hooks.parallelize import (
    evaluate_part_pre_hook,
    process_post_part_hook,
)
from servc.svc.com.worker.types import EMIT_EVENT, RESOLVER_MAPPING
from servc.svc.config import Config
from servc.svc.io.hooks import PartHook
from servc.svc.io.input import ArgumentArtifact, InputPayload, InputType

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
        "part": {
            "part_queue": "test_part",
            "part_id": "test_part",
            "total_parts": 2,
        }
    },
}
partHook: PartHook = art["hooks"]["part"]

testMapping: RESOLVER_MAPPING = {
    "mymethod": lambda _m, _b, _c, p, *y: len(p),
    "mymethod_part": lambda _m, _b, _c, p, *y: [x for x in p],
    "myothermethod": lambda *z: 1,
}

emit: EMIT_EVENT = lambda x, y: None


class TestParallelize(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config = Config()
        cls.bus = BusRabbitMQ(config.get("conf.bus.url"), {}, "")
        cls.cache = CacheRedis(config.get("conf.cache.url"))

        params = pika.URLParameters(config.get("conf.bus.url"))
        cls.conn = pika.BlockingConnection(params)
        cls.channel = cls.conn.channel()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.bus.delete_queue("test_part")
        cls.cache.close()
        cls.bus.close()
        cls.channel.close()
        cls.conn.close()

    def tearDown(self):
        self.bus.delete_queue("test_part")

    def test_part_queue(self):
        res = process_post_part_hook(self.bus, self.cache, message, art, partHook)
        self.assertFalse(res)

    def test_existing_part_queue(self):
        self.bus.create_queue("test_part")
        self.bus.publishMessage("test_part", "test")

        res = process_post_part_hook(self.bus, self.cache, message, art, partHook)
        self.assertTrue(res)

    def test_greater_than_total_parts(self):
        self.bus.create_queue("test_part")
        self.bus.publishMessage("test_part", "test")
        self.bus.publishMessage("test_part", "test")

        res = process_post_part_hook(self.bus, self.cache, message, art, partHook)
        self.assertFalse(res)

    def test_pre_hook_method_check(self):
        # continue because there is no part method
        res = evaluate_part_pre_hook(
            "test", testMapping, self.bus, self.cache, message, art, [], emit
        )
        self.assertTrue(res)

    def test_check_w_part_method(self):
        art2 = json.loads(json.dumps(art))
        art2["method"] = "mymethod"
        del art2["hooks"]["part"]

        # not because there is a part method
        res = evaluate_part_pre_hook(
            "test", testMapping, self.bus, self.cache, message, art2, [], emit
        )
        self.assertFalse(res)

    def test_check_w_part_method_and_hook(self):
        # true because the hook exists already
        res = evaluate_part_pre_hook(
            "test", testMapping, self.bus, self.cache, message, art, [], emit
        )
        self.assertTrue(res)

    def test_non_list_partifier(self):
        art2 = json.loads(json.dumps(art))
        art2["method"] = "mymethod"
        del art2["hooks"]["part"]
        new_mapping: RESOLVER_MAPPING = {
            "mymethod": lambda _m, _b, _c, p, *y: len(p),
            "mymethod_part": lambda _m, _b, _c, p, *y: len(p),
            "myothermethod": lambda *z: 1,
        }

        # true because resolver did not return a list
        res = evaluate_part_pre_hook(
            "test", new_mapping, self.bus, self.cache, message, art2, [], emit
        )
        self.assertTrue(res)

    def test_w_on_complete_hook(self):
        art2 = json.loads(json.dumps(art))
        art2["method"] = "mymethod"
        del art2["hooks"]["part"]
        art2["hooks"]["on_complete"] = [
            {"type": "test", "route": "test", "method": "test"}
        ]

        res = evaluate_part_pre_hook(
            "test", testMapping, self.bus, self.cache, message, art2, [], emit
        )
        self.assertFalse(res)


if __name__ == "__main__":
    unittest.main()
