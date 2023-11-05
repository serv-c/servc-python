import unittest

from servc.com.cache.redis import CacheRedis
from servc.com.service import ComponentType
from servc.config import cache_url

redis = CacheRedis(cache_url)
id = "1234"
value = {
    "key": "value",
    "nested": [
        {
            "key": "value",
        },
    ],
}


class TestRedis(unittest.TestCase):
    def test_simple_set(self):
        result = redis.setKey(id, value)
        self.assertEqual(result, id)

    def test_simple_get(self):
        redis.close()
        redis.setKey(id, value)
        result = redis.getKey(id)
        self.assertEqual(result["nested"][0]["key"], value["nested"][0]["key"])

    def test_blank_get(self):
        result = redis.getKey("12345")
        self.assertEqual(result, None)

    def test_connect_ready(self):
        result = redis.isReady
        self.assertEqual(result, True)

    def test_type(self):
        result = redis.type
        self.assertEqual(result, ComponentType.CACHE)

    def test_connect(self):
        result = redis.connect()
        self.assertEqual(result, None)


if __name__ == "__main__":
    unittest.main()
