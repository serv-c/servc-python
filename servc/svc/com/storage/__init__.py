from typing import Any, Dict

from servc.svc import ComponentType, Middleware
from servc.svc.io.output import StatusCode


class StorageComponent(Middleware):
    _type: ComponentType = ComponentType.STORAGE

    _config: Dict[str, str]

    def __init__(self, config: Dict[str, Any] | None):
        super().__init__()

        if config is None:
            config = {}
        self._config = config

    # def list(self, path: str) -> Tuple[str]:
    #     return tuple([])

    def delete(self, path: str) -> bool:
        return False

    def get(self, path: str) -> Any:
        return None

    def upload(self, path: str, data: Any) -> StatusCode:
        return StatusCode.OK
