from typing import Any, Callable, Union

from servc.com.service import ComponentType, ServiceComponent
from servc.io.output import StatusCode

EmitFunction = Union[Callable[[Any, str], None], None]

InputProcessor = Callable[[Any], StatusCode]

OnConsuming = Union[Callable[[str], None], None]


class BusComponent(ServiceComponent):
    _type: ComponentType = ComponentType.BUS

    _url: str

    def __init__(self, url: str):
        super().__init__()

        self._url = url

    def publishMessage(
        self, route: str, message: Any, emitFunction: EmitFunction
    ) -> bool:
        return False

    def subscribe(
        self,
        route: str,
        inputProcessor: InputProcessor,
        emitFunction: EmitFunction,
        onConsuming: OnConsuming,
    ) -> bool:
        return False
