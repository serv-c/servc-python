from typing import Any, Callable, Union

from servc.com.service import ComponentType, ServiceComponent
from servc.io.output import StatusCode

EmitFunction = Union[Callable[[Any, str, StatusCode | int], None], None]

InputProcessor = Callable[[Any], StatusCode]

OnConsuming = Union[Callable[[str], None], None]


class BusComponent(ServiceComponent):
    _type: ComponentType = ComponentType.BUS

    _url: str

    def __init__(self, url: str):
        super().__init__()

        self._url = url

    def publishMessage(
        self, route: str, message: Any, emitFunction: EmitFunction = None
    ) -> bool:
        return True

    def subscribe(
        self,
        route: str,
        inputProcessor: InputProcessor,
        emitFunction: EmitFunction = None,
        onConsuming: OnConsuming = None,
    ) -> bool:
        return True
