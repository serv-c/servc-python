import unittest

from servc.svc.idgen.simple import simple


class TestIDGen(unittest.TestCase):
    def test_simple_idgen(self):
        route = "/test"
        message = {"test": "test"}
        self.assertIsInstance(simple(route, [], message), str)


if __name__ == "__main__":
    unittest.main()
