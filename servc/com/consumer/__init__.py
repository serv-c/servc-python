from typing import Any, Callable, Dict, List, Union

from servc.com.bus import BusComponent, EmitFunction, OnConsuming
from servc.com.cache import CacheComponent
from servc.com.service import ComponentType, ServiceComponent
from servc.io.input import InputPayload, InputType
from servc.io.output import StatusCode
from servc.io.response import getAnswerArtifact, getErrorArtifact

EMIT_EVENT = Callable[[str, Any], None]

RESOLVER = Callable[
    [str, BusComponent, CacheComponent, List[ComponentType], Any, EMIT_EVENT],
    Union[StatusCode, Any, None],
]

RESOLVER_MAPPING = Dict[str, RESOLVER]


class ConsumerComponent(ServiceComponent):
    _type: ComponentType = ComponentType.CONSUMER

    _route: str

    _instanceId: str

    _resolvers: RESOLVER

    _eventResolvers: RESOLVER

    _bus: BusComponent

    _cache: CacheComponent

    _emitFunction: EmitFunction

    _onConsuming: OnConsuming

    def __init__(
        self,
        route: str,
        instanceId: str,
        resolvers: RESOLVER_MAPPING,
        eventResolvers: RESOLVER_MAPPING,
        emitFunction: EmitFunction,
        onConsuming: OnConsuming,
        bus: BusComponent,
        cache: CacheComponent,
        otherComponents: List[ServiceComponent] = [],
    ):
        super().__init__()
        self._route = route
        self._instanceId = instanceId
        self._resolvers = resolvers
        self._eventResolvers = eventResolvers
        self._emitFunction = emitFunction
        self._onConsuming = onConsuming
        self._bus = bus
        self._cache = cache

        self._resolvers["healthz"] = lambda *args: self.healthz(self, *args)

        self._children.extend(otherComponents)
        self._children.append(bus)
        self._children.append(cache)

    def _connect(self):
        self.isReady = True
        self.isOpen = True

    def _close(self):
        self.isReady = False
        self.isOpen = False
        return True

    def connect(self):
        super().connect()

        self._bus.subscribe(
            self._route,
            self.inputProcessor,
            self._emitFunction,
            self._onConsuming,
        )

    def healthz(
        self,
        route: str,
        bus: BusComponent,
        cache: CacheComponent,
        otherComponents: List[ServiceComponent],
        inputs: Any,
        emitEvent: EMIT_EVENT,
    ):
        for component in [bus, cache, *otherComponents]:
            if not component.isReady:
                return StatusCode.SERVER_ERROR
        return StatusCode.OK

    def emitEvent(self, eventName: str, details: Any):
        bus = self._bus
        eventMessage: InputPayload = {
            "type": InputType.EVENT.value,
            "event": eventName,
            "details": details,
            "route": self._route,
            "instanceId": self._instanceId,
        }

        bus.publishMessage(self._route, eventMessage, self._emitFunction)

    def inputProcessor(self, message: Any) -> StatusCode:
        bus = self._bus
        cache = self._cache

        if "type" not in message or "route" not in message:
            return StatusCode.INVALID_INPUTS

        if message["type"] in [InputType.EVENT.value, InputType.EVENT]:
            if (
                "event" not in message
                or "details" not in message
                or "instanceId" not in message
            ):
                return StatusCode.INVALID_INPUTS
            if message["event"] not in self._eventResolvers:
                return StatusCode.METHOD_NOT_FOUND
            self._eventResolvers[message["event"]](
                "",
                bus,
                cache,
                self._children,
                {**message},
                self.emitEvent,
            )
            return StatusCode.OK

        if message["type"] in [InputType.INPUT.value, InputType.INPUT]:
            if "id" not in message:
                return StatusCode.INVALID_INPUTS
            if "argumentId" not in message:
                cache.setKey(
                    message["id"],
                    getErrorArtifact(
                        message["id"],
                        "Invalid input type. Id and argumentId not specified",
                        StatusCode.INVALID_INPUTS,
                    ),
                )
                return StatusCode.INVALID_INPUTS
            if "instanceId" in message and message["instanceId"] != self._instanceId:
                return StatusCode.NO_PROCESSING

            if message["argumentId"] in ["raw", "plain"] and message["inputs"]:
                artifact = message["inputs"]
            else:
                artifact = cache.getKey(message["argumentId"])
            if artifact is None or "method" not in artifact or "inputs" not in artifact:
                cache.setKey(
                    message["id"],
                    getErrorArtifact(
                        message["id"],
                        "Invalid argument. Need to specify method and inputs in payload",
                        StatusCode.USER_ERROR,
                    ),
                )
                return StatusCode.USER_ERROR
            if artifact["method"] not in self._resolvers:
                cache.setKey(
                    message["id"],
                    getErrorArtifact(
                        message["id"], "Method not found", StatusCode.METHOD_NOT_FOUND
                    ),
                )
                return StatusCode.METHOD_NOT_FOUND

            try:
                response = self._resolvers[artifact["method"]](
                    message["id"],
                    bus,
                    cache,
                    self._children,
                    artifact["inputs"],
                    self.emitEvent,
                )
                cache.setKey(message["id"], getAnswerArtifact(message["id"], response))
                return StatusCode.OK
            except Exception as e:
                cache.setKey(
                    message["id"],
                    getErrorArtifact(message["id"], str(e), StatusCode.SERVER_ERROR),
                )
                return StatusCode.SERVER_ERROR

        cache.setKey(
            message["id"],
            getErrorArtifact(
                message["id"], "Invalid input type", StatusCode.INVALID_INPUTS
            ),
        )
        return StatusCode.INVALID_INPUTS
