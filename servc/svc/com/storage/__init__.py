from servc.svc import ComponentType, Middleware
from servc.svc.config import Config


class StorageComponent(Middleware):
    _type: ComponentType = ComponentType.STORAGE

    def __init__(self, config: Config):
        super().__init__(config)
