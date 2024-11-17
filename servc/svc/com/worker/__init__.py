from typing import Any, Callable, Dict, List, Union

from servc.svc import ComponentType, Middleware
from servc.svc.com.bus import BusComponent, OnConsuming
from servc.svc.com.cache import CacheComponent
from servc.svc.config import Config
from servc.svc.io.input import EventPayload, InputType
from servc.svc.io.output import StatusCode
from servc.svc.io.response import getAnswerArtifact, getErrorArtifact

RESOLVER = Callable[
    [str, BusComponent, CacheComponent, Any, List[Middleware]],
    Union[StatusCode, Any, None],
]

RESOLVER_MAPPING = Dict[str, RESOLVER]


def HEALTHZ(
    _id: str, bus: BusComponent, cache: CacheComponent, _any: Any, c: List[Middleware]
) -> StatusCode:
    for component in [bus, cache, *c]:
        if not component.isReady:
            return StatusCode.SERVER_ERROR
    return StatusCode.OK


class WorkerComponent(Middleware):
    _type: ComponentType = ComponentType.WORKER

    _route: str

    _instanceId: str

    _resolvers: RESOLVER_MAPPING

    _eventResolvers: RESOLVER_MAPPING

    _bus: BusComponent

    _cache: CacheComponent

    _onConsuming: OnConsuming

    _config: Config

    _bindToEventExchange: bool

    def __init__(
        self,
        route: str,
        instanceId: str,
        resolvers: RESOLVER_MAPPING,
        eventResolvers: RESOLVER_MAPPING,
        onConsuming: OnConsuming,
        bus: BusComponent,
        cache: CacheComponent,
        config: Config,
        otherComponents: List[Middleware] = [],
    ):
        super().__init__()
        self._route = route
        self._instanceId = instanceId
        self._resolvers = resolvers
        self._eventResolvers = eventResolvers
        self._onConsuming = onConsuming
        self._bus = bus
        self._cache = cache
        self._config = config
        self._bindToEventExchange = (
            config.get("conf.bus.bindtoeventexchange")
            if len(self._eventResolvers.keys()) > 0
            else False
        )

        self._resolvers["healthz"] = lambda *args: HEALTHZ(*args)

        self._children.extend(otherComponents)
        self._children.append(bus)
        self._children.append(cache)

    def _connect(self):
        self._isReady = True
        self._isOpen = True

    def _close(self):
        self._isReady = False
        self._isOpen = False
        return True

    def connect(self):
        super().connect()

        print("Consumer now Subscribing", flush=True)
        print(" Route:", self._route, flush=True)
        print(" InstanceId:", self._instanceId, flush=True)
        print(" Resolvers:", self._resolvers.keys(), flush=True)
        print(" Event Resolvers:", self._eventResolvers.keys(), flush=True)
        print(" Bind to Event Exchange:", self._bindToEventExchange, flush=True)

        self._bus.subscribe(
            self._route,
            self.inputProcessor,
            self._onConsuming,
            bindEventExchange=self._bindToEventExchange,
        )

    def emitEvent(self, eventName: str, details: Any):
        eventMessage: EventPayload = {
            "type": InputType.EVENT.value,
            "event": eventName,
            "details": details,
            "route": self._route,
            "instanceId": self._instanceId,
        }

        self._bus.publishMessage(self._route, eventMessage)

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
                {**message},
                self._children,
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
                    artifact["inputs"],
                    self._children,
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
