from typing import Any, Callable, Union

from servc.svc import ComponentType, Middleware
from servc.svc.io.input import EventPayload, InputPayload, InputType
from servc.svc.io.output import StatusCode

InputProcessor = Callable[..., StatusCode]

OnConsuming = Union[Callable[[str], None], None]


class BusComponent(Middleware):
    _type: ComponentType = ComponentType.BUS

    _url: str

    _routeMap: dict

    _prefix: str

    def __init__(self, url: str, routeMap: dict, prefix: str):
        super().__init__()

        self._url = url
        self._routeMap = routeMap
        self._prefix = prefix

    def getRoute(self, route: str) -> str:
        if route in self._routeMap:
            return "".join([self._prefix, self._routeMap[route]])
        return "".join([self._prefix, route])

    def publishMessage(self, route: str, message: InputPayload | EventPayload) -> bool:
        return True

    def emitEvent(self, event: str, instanceId: str, details: Any) -> bool:
        return self.publishMessage(
            self.getRoute(event),
            {
                "type": InputType.EVENT.value,
                "route": self.getRoute(event),
                "event": event,
                "details": details,
                "instanceId": instanceId,
            },
        )

    def create_queue(self, queue: str, bindEventExchange: bool = True) -> bool:
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
        bindEventExchange: bool = True,
    ) -> bool:
        return True
