from enum import Enum
from typing import NotRequired, TypedDict


class CompleteHookType(Enum):
    SENDMESSAGE = "sendmessage"


class OnCompleteHook(TypedDict):
    type: CompleteHookType
    route: str
    method: str


class PartHook(TypedDict):
    part_id: int
    total_parts: int
    part_queue: str


class Hooks(TypedDict):
    on_complete: NotRequired[list[OnCompleteHook]]
    part: NotRequired[PartHook]
