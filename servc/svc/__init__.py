from __future__ import annotations

from enum import Enum
from typing import Callable, List

from servc.svc.config import Config


class ComponentType(Enum):
    BUS = "bus"
    CACHE = "cache"
    WORKER = "worker"
    INTERFACE = "interface"
    DATABASE = "database"
    STORAGE = "storage"


class Middleware:
    _children: List[Middleware]

    name: str

    _isReady: bool

    _isOpen: bool

    _type: ComponentType

    _connect: Callable[..., None]

    _close: Callable[..., bool]

    def __init__(self, _config: Config):
        self._children = []
        self._isReady = False
        self._isOpen = False

    @property
    def isReady(self) -> bool:
        isReadyCheck = self._isReady
        for child in self._children:
            isReadyCheck = isReadyCheck and child.isReady

        return isReadyCheck

    @property
    def isOpen(self) -> bool:
        isOpen = self._isOpen
        for child in self._children:
            isOpen = isOpen and child.isOpen

        return isOpen

    @property
    def type(self) -> ComponentType:
        return self._type

    def connect(self):
        for child in self._children:
            child.connect()
        return self._connect()

    def close(self):
        return self._close()

    def getChild(
        self, filter: ComponentType | None = None, name: str | None = None
    ) -> Middleware:
        for child in self._children:
            if (filter and child.type == filter) or (name and child.name == name):
                return child
        raise Exception("Child of not found")
