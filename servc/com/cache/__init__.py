from typing import Any

from servc.com.service import ComponentType, ServiceComponent


class CacheComponent(ServiceComponent):
    _type: ComponentType = ComponentType.CACHE

    _url: str

    def __init__(self, url: str):
        super().__init__()

        self._url = url

    def setKey(self, id: str, value: Any) -> str:
        pass

    def getKey(self, id: str) -> Any | None:
        pass

    def deleteKey(self, id: str) -> bool:
        pass
