from typing import Any, Callable, Dict, Union

from servc.svc import ComponentType, Middleware
from servc.svc.config import Config
from servc.svc.io.input import EventPayload, InputPayload, InputType
from servc.svc.io.output import StatusCode

InputProcessor = Callable[..., StatusCode]

OnConsuming = Union[Callable[[str], None], None]


class BusComponent(Middleware):
    name: str = "bus"

    _type: ComponentType = ComponentType.BUS

    _url: str

    _routeMap: Dict[str, str]

    _prefix: str

    _instanceId: str

    _route: str

    def __init__(self, config: Config):
        super().__init__(config)

        self._url = str(config.get("url"))
        self._prefix = str(config.get("prefix"))
        self._instanceId = str(config.get("instanceid"))
        self._route = str(config.get("route"))

        routemap = config.get("routemap")
        if routemap is None or not isinstance(routemap, dict):
            routemap = {}
        self._routeMap = routemap

    @property
    def instanceId(self) -> str:
        return self._instanceId

    @property
    def route(self) -> str:
        return self._route

    def getRoute(self, route: str) -> str:
        if route in self._routeMap:
            return "".join([self._prefix, self._routeMap[route]])
        return "".join([self._prefix, route])

    def publishMessage(self, route: str, message: InputPayload | EventPayload) -> bool:
        return True

    def emitEvent(self, event: str, details: Any) -> bool:
        return self.publishMessage(
            self.getRoute(event),
            {
                "type": InputType.EVENT.value,
                "route": self.getRoute(event),
                "event": event,
                "details": details,
                "instanceId": self._instanceId,
            },
        )

    def create_queue(self, queue: str, bindEventExchange: bool) -> bool:
        return False

    def delete_queue(self, queue: str) -> bool:
        return False

    def get_queue_length(self, queue: str) -> int:
        return 0

    def subscribe(
        self,
        route: str,
        inputProcessor: InputProcessor,
        onConsuming: OnConsuming | None,
        bindEventExchange: bool,
    ) -> bool:
        return True
