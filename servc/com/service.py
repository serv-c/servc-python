from __future__ import annotations

from enum import Enum
from typing import Callable, List


class ComponentType(Enum):
    BUS = "bus"
    CACHE = "cache"
    CONSUMER = "consumer"
    INTERFACE = "interface"
    DATABASE = "database"


class ServiceComponent:
    _children: List[ServiceComponent]

    _isReady: bool

    _isOpen: bool

    _type: ComponentType

    _connect: Callable[..., None]

    _close: Callable[..., bool]

    def __init__(self):
        self._children = []
        self._isReady = False
        self._isOpen = False

    @property
    def isReady(self) -> bool:
        isReady = self._isReady
        for child in self._children:
            isReady = isReady and child.isReady

        return isReady

    @property
    def isOpen(self) -> bool:
        isOpen = self._isOpen
        for child in self._children:
            isOpen = isOpen and child.isOpen

        return isOpen

    @isReady.setter
    def isReady(self, value: bool):
        self._isReady = value

    @isOpen.setter
    def isOpen(self, value: bool):
        self._isOpen = value

    @property
    def type(self) -> ComponentType:
        return self._type

    def connect(self):
        for child in self._children:
            child.connect()
        return self._connect()

    def close(self):
        for child in self._children:
            child.close()
        return self._close()

    def getChild(self, filter: ComponentType) -> ServiceComponent:
        for child in self._children:
            if child.type == filter:
                return child
        raise Exception(f"Child of type {filter} not found")
