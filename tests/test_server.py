import json
import os
import signal
import time
import unittest
import urllib.request

from servc.com.bus.rabbitmq import BusRabbitMQ
from servc.com.cache.redis import CacheRedis
from servc.com.server.server import start_server
from servc.config import bus_url, cache_url
from servc.io.client.poll import pollMessage
from servc.io.client.send import sendMessage
from servc.io.idgenerator.simple import simpleIDGenerator
from servc.io.input import InputType
from servc.io.output import StatusCode


def inputProcessor(id, bus, cache, components, message, emit):
    return message


class TestServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.route = "test_consumer"
        cls.instance_id = "test_consumer_instance_id"
        cls.bus = BusRabbitMQ(bus_url)
        cls.cache = CacheRedis(cache_url)
        cls.server = start_server(
            cls.route,
            {
                "passthrough": inputProcessor,
            },
            returnProcess=True,
            port=5000,
        )

    def setUp(self):
        time.sleep(5)

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.server is not None:
            for process in cls.server:
                if process is not None and process.pid is not None:
                    os.kill(process.pid, signal.SIGTERM)

    def tearDown(self):
        time.sleep(5)

    def test_health_http(self):
        time.sleep(5)
        contents = urllib.request.urlopen("http://localhost:5000/healthz").read()
        self.assertEqual(contents, b"OK")

    def test_plaintext_ack(self):
        message = {
            "type": InputType.INPUT.value,
            "route": self.route,
            "id": "",
            "argumentId": "plain",
            "inputs": {
                "method": "passthrough",
                "inputs": {
                    "test": "test",
                },
            },
        }
        id = sendMessage(message, self.bus, self.cache, simpleIDGenerator, force=True)
        result = pollMessage(id, self.cache)
        self.assertEqual(result["progress"], 100)
        self.assertEqual(result["isError"], False)
        self.assertEqual(result["statusCode"], StatusCode.OK.value)
        self.assertEqual(result["responseBody"]["test"], "test")

    def test_simple_ack(self):
        argumentId = "asdasd"
        self.cache.setKey(
            argumentId,
            {
                "method": "passthrough",
                "inputs": 10,
            },
        )
        message = {
            "type": InputType.INPUT.value,
            "route": self.route,
            "id": "",
            "argumentId": argumentId,
            "inputs": "",
        }
        id = sendMessage(message, self.bus, self.cache, simpleIDGenerator, force=True)
        result = pollMessage(id, self.cache)
        self.assertEqual(result["progress"], 100)
        self.assertEqual(result["isError"], False)
        self.assertEqual(result["statusCode"], StatusCode.OK.value)
        self.assertEqual(result["responseBody"], 10)

    def test_non_existing_method(self):
        message = {
            "type": InputType.INPUT.value,
            "route": self.route,
            "id": "",
            "argumentId": "plain",
            "inputs": {
                "method": "nonExistingMethod",
                "inputs": {
                    "test": "test",
                },
            },
        }
        id = sendMessage(message, self.bus, self.cache, simpleIDGenerator, force=True)
        result = pollMessage(id, self.cache)
        self.assertEqual(result["progress"], 100)
        self.assertEqual(result["isError"], True)
        self.assertEqual(result["statusCode"], StatusCode.METHOD_NOT_FOUND.value)
        self.assertEqual(result["responseBody"], "Method not found")

    def test_get_response(self):
        message = {
            "type": InputType.INPUT.value,
            "route": self.route,
            "id": "",
            "argumentId": "",
            "inputs": {
                "method": "passthrough",
                "inputs": 100,
            },
        }
        id = sendMessage(message, self.bus, self.cache, simpleIDGenerator)
        result = pollMessage(id, self.cache)
        response = (
            urllib.request.urlopen(f"http://localhost:5000/id/{id}")
            .read()
            .decode("utf-8")
        )
        result = json.loads(response)

        self.assertEqual(result["progress"], 100)
        self.assertEqual(result["isError"], False)
        self.assertEqual(result["statusCode"], StatusCode.OK.value)
        self.assertEqual(result["responseBody"], 100)


if __name__ == "__main__":
    unittest.main()
