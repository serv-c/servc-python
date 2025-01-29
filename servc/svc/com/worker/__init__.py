from typing import Any, List, Tuple

from servc.svc import ComponentType, Middleware
from servc.svc.com.bus import BusComponent, OnConsuming
from servc.svc.com.cache import CacheComponent
from servc.svc.com.worker.hooks import evaluate_post_hooks, evaluate_pre_hooks
from servc.svc.com.worker.types import RESOLVER, RESOLVER_CONTEXT, RESOLVER_MAPPING
from servc.svc.config import Config
from servc.svc.io.input import InputType
from servc.svc.io.output import (
    InvalidInputsException,
    MethodNotFoundException,
    NoProcessingException,
    NotAuthorizedException,
    ResponseArtifact,
    StatusCode,
)
from servc.svc.io.response import getAnswerArtifact, getErrorArtifact


def HEALTHZ(_id: str, _any: Any, c: RESOLVER_CONTEXT) -> StatusCode:
    for component in [c["bus"], c["cache"], *c["middlewares"]]:
        if not component.isReady:
            return StatusCode.SERVER_ERROR
    return StatusCode.OK


class WorkerComponent(Middleware):
    name: str = "worker"

    _type: ComponentType = ComponentType.WORKER

    _resolvers: RESOLVER_MAPPING

    _eventResolvers: RESOLVER_MAPPING

    _bus: BusComponent

    _cache: CacheComponent

    _onConsuming: OnConsuming

    _config: Config

    _bindToEventExchange: bool

    _busClass: type[BusComponent]

    def __init__(
        self,
        resolvers: RESOLVER_MAPPING,
        eventResolvers: RESOLVER_MAPPING,
        onConsuming: OnConsuming,
        bus: BusComponent,
        busClass: type[BusComponent],
        cache: CacheComponent,
        config: Config,
        otherComponents: List[Middleware] = [],
    ):
        super().__init__(config)
        self._resolvers = resolvers
        self._eventResolvers = eventResolvers
        self._onConsuming = onConsuming
        self._bus = bus
        self._busClass = busClass
        self._cache = cache
        self._config = config
        self._bindToEventExchange = (
            config.get(f"conf.{self.name}.bindtoeventexchange")
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
        print(" Route:", self._bus.route, flush=True)
        print(" InstanceId:", self._bus.instanceId, flush=True)
        print(" Resolvers:", self._resolvers.keys(), flush=True)
        print(" Event Resolvers:", self._eventResolvers.keys(), flush=True)
        print(" Bind to Event Exchange:", self._bindToEventExchange, flush=True)

        self._bus.subscribe(
            self._bus.route,
            self.inputProcessor,
            self._onConsuming,
            bindEventExchange=self._bindToEventExchange,
        )

    def run_resolver(
        self, method: RESOLVER, context: RESOLVER_CONTEXT, args: Tuple[str, Any]
    ) -> Tuple[StatusCode, ResponseArtifact | None]:
        id, payload = args
        try:
            response = method(id, payload, context)
            return StatusCode.OK, getAnswerArtifact(id, response)
        except NotAuthorizedException as e:
            return StatusCode.NOT_AUTHORIZED, getErrorArtifact(
                id, str(e), StatusCode.NOT_AUTHORIZED
            )
        except InvalidInputsException as e:
            return StatusCode.INVALID_INPUTS, getErrorArtifact(
                id, str(e), StatusCode.INVALID_INPUTS
            )
        except NoProcessingException:
            return StatusCode.NO_PROCESSING, None
        except MethodNotFoundException as e:
            return StatusCode.METHOD_NOT_FOUND, getErrorArtifact(
                id, str(e), StatusCode.METHOD_NOT_FOUND
            )
        except Exception as e:
            if self._config.get(f"conf.{self.name}.exiton5xx"):
                print("Exiting due to 5xx error", e, flush=True)
                exit(1)
            return StatusCode.SERVER_ERROR, getErrorArtifact(
                id, str(e), StatusCode.SERVER_ERROR
            )

    def inputProcessor(self, message: Any) -> StatusCode:
        bus = self._busClass(
            self._config.get(f"conf.{self._bus.name}"),
        )
        cache = self._cache
        context: RESOLVER_CONTEXT = {
            "bus": bus,
            "cache": cache,
            "middlewares": self._children,
            "config": self._config,
        }

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

            status_code, response = self.run_resolver(
                self._eventResolvers[message["event"]],
                context,
                ("", {**message}),
            )

            return status_code

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
            if "instanceId" in message and message["instanceId"] != bus.instanceId:
                return StatusCode.NO_PROCESSING

            if message["argumentId"] in ["raw", "plain"] and message["inputs"]:
                artifact = message["argument"]
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

            continueExecution = evaluate_pre_hooks(
                self._resolvers,
                message,
                artifact,
                context,
            )
            if not continueExecution:
                return StatusCode.OK

            statusCode, response = self.run_resolver(
                self._resolvers[artifact["method"]],
                context,
                (message["id"], artifact["inputs"]),
            )
            if statusCode == StatusCode.NO_PROCESSING:
                return StatusCode.NO_PROCESSING

            cache.setKey(message["id"], response)
            evaluate_post_hooks(bus, cache, message, artifact)
            return statusCode

        cache.setKey(
            message["id"],
            getErrorArtifact(
                message["id"], "Invalid input type", StatusCode.INVALID_INPUTS
            ),
        )
        return StatusCode.INVALID_INPUTS
