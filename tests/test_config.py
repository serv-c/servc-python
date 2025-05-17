import unittest

from servc.svc.config import Config


class TestConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = Config()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.config.setValue("conf.bus.prefix", "")

    def test_get_defaults(self):
        self.assertEqual(self.config.get("conf.file"), "/config/config.yaml")
        self.assertEqual(self.config.get("conf.bus.routemap"), {})
        self.assertEqual(self.config.get("conf.bus.prefix"), "")

    def test_value(self):
        self.config.setValue("conf.bus.prefix", "test")
        self.assertEqual(self.config.get("conf.bus.prefix"), "test")

        self.config.setValue("conf.bus.routemap.api", "test_route")
        self.assertEqual(self.config.get("conf.bus.routemap.api"), "test_route")

        self.config.setValue("conf.bus.routemap_DOT_api", "test_route")
        self.assertEqual(self.config.get("conf.bus.routemap_DOT_api"), "test_route")
        self.assertIn("routemap.api", self.config.get("conf.bus"))

    def test_wrong_location(self):
        try:
            Config("config.test.yaml")
        except FileNotFoundError:
            return self.assertTrue(True)
        self.assertTrue(False)


if __name__ == "__main__":
    unittest.main()
