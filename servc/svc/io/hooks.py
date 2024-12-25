from enum import Enum
from typing import Any, NotRequired, TypedDict


class CompleteHookType(Enum):
    SENDMESSAGE = "sendmessage"


class CompleteHookArgument(TypedDict):
    id: NotRequired[str]
    method: NotRequired[str]
    inputs: NotRequired[Any]


class OnCompleteHook(TypedDict):
    type: CompleteHookType
    route: str
    method: str
    inputs: NotRequired[CompleteHookArgument | Any]


class PartHook(TypedDict):
    part_id: int
    total_parts: int
    part_queue: str


class Hooks(TypedDict):
    on_complete: NotRequired[list[OnCompleteHook]]
    part: NotRequired[PartHook]
