import datetime
import decimal
import unittest

from servc.svc.com.cache.redis import CacheRedis
from servc.svc.config import Config


class TestRedis(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config = Config()
        cls.cache = CacheRedis(config.get("conf.cache"))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.cache.close()

    def test_get_key(self):
        key = "test_key"
        value = datetime.datetime.now()
        self.cache.setKey(key, value)
        self.assertEqual(self.cache.getKey(key), value.isoformat())

    def test_set_key(self):
        key = "test_key2"
        value = decimal.Decimal(10.5)
        self.cache.connect()
        self.cache.setKey(key, value)

        self.cache.close()
        self.assertEqual(self.cache.getKey(key), value)

    def test_connect_twice(self):
        self.cache.connect()
        self.cache.connect()
        self.assertTrue(self.cache.isReady)

    def test_fake_key(self):
        self.assertIsNone(self.cache.getKey("fake_key"))

    def test_delete_key(self):
        key = "test_key3"
        value = "test_value"
        self.cache.close()
        self.cache.setKey(key, value)
        self.assertIsNotNone(self.cache.getKey(key))

        self.cache.close()
        self.assertTrue(self.cache.deleteKey(key))
        self.assertIsNone(self.cache.getKey(key))

    def test_close_twice(self):
        self.cache.close()
        self.cache.close()
        self.assertFalse(self.cache.isReady)


if __name__ == "__main__":
    unittest.main()
