from typing import Any

from servc.svc import ComponentType, Middleware
from servc.svc.config import Config
from servc.svc.io.output import StatusCode
from servc.svc.io.response import generateResponseArtifact


class CacheComponent(Middleware):
    name: str = "cache"

    _type: ComponentType = ComponentType.CACHE

    def __init__(self, config: Config):
        super().__init__(config)

    def setKey(self, id: str, value: Any) -> str:
        return ""

    def getKey(self, id: str) -> Any | None:
        return None

    def deleteKey(self, id: str) -> bool:
        return False

    def setProgress(self, id: str, progress: float, message: str) -> bool:
        return not not self.setKey(
            id,
            generateResponseArtifact(
                id,
                progress,
                message,
                StatusCode.OK,
                False,
            ),
        )
