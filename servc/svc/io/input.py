from enum import Enum
from typing import Any, NotRequired, TypedDict


class InputType(Enum):
    INPUT = "input"
    EVENT = "event"


class GenericInput(TypedDict):
    type: str
    route: str
    force: NotRequired[bool]


class InputPayload(GenericInput):
    id: str
    argumentId: str
    instanceId: NotRequired[str]
    argument: NotRequired[Any]


class EventPayload(GenericInput):
    event: str
    details: Any
    instanceId: str


class ArgumentArtifact(TypedDict):
    method: str
    inputs: Any
