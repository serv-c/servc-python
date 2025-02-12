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
    "mymethod": lambda _m, p, _c: len(p),
    "mymethod_part": lambda _m, p, _c: [x for x in p],
    "myothermethod": lambda *z: 1,
}

emit: EMIT_EVENT = lambda x, y: None


class TestParallelize(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config = Config()
        cls.bus = BusRabbitMQ(config.get("conf.bus"))
        cls.cache = CacheRedis(config.get("conf.cache"))
        cls.context = {
            "bus": cls.bus,
            "cache": cls.cache,
            "middlewares": [],
            "config": config,
        }

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
        self.bus.create_queue("test_part", False)
        self.bus.publishMessage("test_part", "test")

        res = process_post_part_hook(self.bus, self.cache, message, art, partHook)
        self.assertTrue(res)

    def test_greater_than_total_parts(self):
        self.bus.create_queue("test_part", False)
        self.bus.publishMessage("test_part", "test")
        self.bus.publishMessage("test_part", "test")

        res = process_post_part_hook(self.bus, self.cache, message, art, partHook)
        self.assertFalse(res)

    def test_pre_hook_method_check(self):
        # continue because there is no part method
        self.bus._route = "test"
        res = evaluate_part_pre_hook(testMapping, message, art, self.context)
        self.assertTrue(res)

    def test_check_w_part_method(self):
        self.bus._route = "test"
        art2 = json.loads(json.dumps(art))
        art2["method"] = "mymethod"
        del art2["hooks"]["part"]

        # not because there is a part method
        res = evaluate_part_pre_hook(testMapping, message, art2, self.context)
        self.assertFalse(res)

    def test_check_w_part_method_and_hook(self):
        self.bus._route = "test"
        # true because the hook exists already
        res = evaluate_part_pre_hook(testMapping, message, art, self.context)
        self.assertTrue(res)

    def test_non_list_partifier(self):
        self.bus._route = "test"
        art2 = json.loads(json.dumps(art))
        art2["method"] = "mymethod"
        del art2["hooks"]["part"]
        new_mapping: RESOLVER_MAPPING = {
            "mymethod": lambda _m, p, _c: len(p),
            "mymethod_part": lambda _m, p, _c: len(p),
            "myothermethod": lambda *z: 1,
        }

        # true because resolver did not return a list
        self.assertRaises(
            Exception,
            lambda: evaluate_part_pre_hook(new_mapping, message, art2, self.context),
        )

    def test_w_on_complete_hook(self):
        self.bus._route = "test"

        art2 = json.loads(json.dumps(art))
        art2["method"] = "mymethod"
        del art2["hooks"]["part"]
        art2["hooks"]["on_complete"] = [
            {"type": "test", "route": "test", "method": "test"}
        ]

        res = evaluate_part_pre_hook(testMapping, message, art2, self.context)
        self.assertFalse(res)


if __name__ == "__main__":
    unittest.main()
