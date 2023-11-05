import unittest

from servc.io.idgenerator.simple import simpleIDGenerator


class TestIdGenerator(unittest.TestCase):
    def test_generator_simple(self):
        id = simpleIDGenerator("test", [], {"test": "test"})
        self.assertEqual(type(id), str)


if __name__ == "__main__":
    unittest.main()
