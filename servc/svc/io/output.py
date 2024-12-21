from enum import Enum
from typing import Any, TypedDict


class StatusCode(Enum):
    OK = 200
    USER_ERROR = 400
    METHOD_NOT_FOUND = 404
    NOT_AUTHORIZED = 401
    INVALID_INPUTS = 422
    SERVER_ERROR = 500
    NO_PROCESSING = 204


class ResponseArtifact(TypedDict):
    id: str
    statusCode: int
    progress: float
    responseBody: Any
    isError: bool


class NotAuthorizedException(Exception):
    def __init__(self, message: str = "Not authorized"):
        self.message = message

    def __str__(self):
        return self.message


class InvalidInputsException(Exception):
    def __init__(self, message: str = "Invalid inputs"):
        self.message = message

    def __str__(self):
        return self.message


class NoProcessingException(Exception):
    def __init__(self, message: str = "No processing"):
        self.message = message

    def __str__(self):
        return self.message


class MethodNotFoundException(Exception):
    def __init__(self, message: str = "Method not found"):
        self.message = message

    def __str__(self):
        return self.message
