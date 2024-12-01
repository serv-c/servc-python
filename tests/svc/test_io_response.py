import unittest

from servc.svc.io.output import StatusCode
from servc.svc.io.response import (
    generateResponseArtifact,
    getAnswerArtifact,
    getErrorArtifact,
    getProgressArtifact,
)


class TestResponse(unittest.TestCase):
    def test_fractional_progress(self):
        response = generateResponseArtifact("1", 0.5, "response", StatusCode.OK, False)
        self.assertEqual(response["progress"], 50)

    def test_error_response(self):
        response = getErrorArtifact("1", "response", StatusCode.OK)
        self.assertTrue(response["isError"])
        self.assertEqual(response["progress"], 100)

    def test_indirect_progress(self):
        response = getProgressArtifact("1", 0.5, "response")
        self.assertEqual(response["progress"], 50)

    def test_answer_response(self):
        response = getAnswerArtifact("1", "response")
        self.assertFalse(response["isError"])
        self.assertEqual(response["progress"], 100)


if __name__ == "__main__":

    unittest.main()
