import json
import os
import unittest
import uuid

from servc.svc.com.bus.rabbitmq import BusRabbitMQ
from servc.svc.com.cache.redis import CacheRedis
from servc.svc.com.http.blob import HTTPUpload
from servc.svc.com.storage.blob import BlobStorage
from servc.svc.config import Config
from servc.svc.io.output import ResponseArtifact
from tests import get_route_message

queue = str(uuid.uuid4())
payload = {
    "type": "input",
    "route": queue,
    "argumentId": "plain",
    "force": True,
    "instanceId": None,
    "inputs": {
        "method": "upload",
        "inputs": {
            "type": "chess",
            "files": "and this also",
        },
    },
}


class MyBlob(BlobStorage):
    _basepath: str = "/tmp"

    def exists(self, container, prefix):
        return os.path.exists(os.path.join(self._basepath, container, prefix))

    def get_file(self, container, prefix):
        if not self.exists(container, prefix):
            return None
        with open(os.path.join(self._basepath, container, prefix), "rb") as f:
            return f.read()

    def put_file(self, container, prefix, data):
        if not os.path.exists(os.path.join(self._basepath, container)):
            os.makedirs(os.path.join(self._basepath, container))
        with open(os.path.join(self._basepath, container, prefix), "wb") as f:
            f.write(data)

    def delete_file(self, container, prefix):
        if self.exists(container, prefix):
            os.remove(os.path.join(self._basepath, container, prefix))

    def list_files(self, container, prefix=""):
        folder = os.path.join(self._basepath, container, str(prefix))
        if not os.path.exists(folder):
            return []
        return os.listdir(folder)


class TestBasicUpload(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config = Config()
        cls.bus = BusRabbitMQ(config.get(f"conf.{BusRabbitMQ.name}"))
        cls.cache = CacheRedis(config.get(f"conf.{CacheRedis.name}"))
        cls.blob = MyBlob(config.get(f"conf.{MyBlob.name}"))
        cls.http = HTTPUpload(
            config.get(f"conf.{HTTPUpload.name}"),
            cls.bus,
            cls.cache,
            None,
            {},
            {},
            [cls.blob],
        )
        cls.http.bindRoutes()
        cls.bus.connect()

        cls.http._server.testing = True
        cls.appctx = cls.http._server.app_context()
        cls.appctx.push()
        cls.client = cls.http._server.test_client()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.appctx.pop()
        cls.http._server = None
        cls.appctx = None
        cls.client = None
        cls.bus.close()
        cls.cache.close()

    def setUp(self):
        self.channel = self.bus._conn.channel()
        self.channel.queue_declare(queue=queue, durable=True)

    def tearDown(self):
        self.channel.queue_delete(queue=queue)
        self.channel.close()

    def test_downloading_file(self):
        response: ResponseArtifact = {
            "id": "12345",
            "isError": False,
            "progress": 100,
            "responseBody": {
                "file": "test2.txt",
                "message": "File downloaded successfully",
            },
            "statusCode": 200,
        }
        self.blob.put_file(
            "uploads", response["responseBody"]["file"], b"Hello, World!"
        )
        self.cache.setKey(response["id"], response)

        res = self.client.get(f"/fid/{response["id"]}")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.data, b"Hello, World!")

    def test_downloading_custom_containerfile(self):
        response: ResponseArtifact = {
            "id": "1234",
            "isError": False,
            "progress": 100,
            "responseBody": {
                "file": "test.txt",
                "container": "custom_container",
                "message": "File downloaded successfully",
            },
            "statusCode": 200,
        }
        self.blob.put_file("custom_container", "test.txt", b"Hello, World!")
        self.cache.setKey(response["id"], response)

        res = self.client.get(f"/fid/{response["id"]}")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.data, b"Hello, World!")

    def test_uploading_files(self):
        response = self.client.post(
            "/",
            data={
                "json": json.dumps(payload),
                "file1": open("requirements.txt", "rb"),
                "file2": open("requirements.txt", "rb"),
                "file3": open("README.md", "rb"),
            },
        )

        self.assertEqual(response.status_code, 200)
        id = response.text
        self.assertIsInstance(id, str)

        message, count = get_route_message(self.channel, self.cache, queue)
        self.assertEqual(count, 1)
        self.assertIsInstance(message, dict)

        inputs = message["argument"]["inputs"]
        self.assertEqual(inputs["type"], "chess")
        self.assertIsInstance(inputs["files"], list)
        self.assertEqual(len(inputs["files"]), 3)

        files = self.blob.list_files("uploads")
        for file in inputs["files"]:
            file = os.path.join(file)
            self.assertIn(file, files)

            filecontent = self.blob.get_file("uploads", file)
            self.assertIsNotNone(filecontent)

            with open(os.path.basename(file)) as f:
                contents = f.read()
                self.assertEqual(contents, filecontent.decode("utf8"))


if __name__ == "__main__":
    unittest.main()
