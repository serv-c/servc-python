from typing import Any, List, Tuple

from servc.svc import ComponentType, Middleware
from servc.svc.com.bus import BusComponent, OnConsuming
from servc.svc.com.cache import CacheComponent
from servc.svc.com.worker.hooks import evaluate_post_hooks, evaluate_pre_hooks
from servc.svc.com.worker.methods import evaluate_exit, get_artifact
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
    ) -> Tuple[StatusCode, ResponseArtifact | None, Any | None]:
        id, payload = args
        statuscode: StatusCode = StatusCode.OK
        response: ResponseArtifact | None = None
        error: Any = None

        try:
            response = getAnswerArtifact(id, method(id, payload, context))
        except NotAuthorizedException as e:
            error = e
            statuscode = StatusCode.NOT_AUTHORIZED
            response = getErrorArtifact(id, str(e), StatusCode.NOT_AUTHORIZED)
        except InvalidInputsException as e:
            error = e
            statuscode = StatusCode.INVALID_INPUTS
            response = getErrorArtifact(id, str(e), StatusCode.INVALID_INPUTS)
        except NoProcessingException:
            statuscode = StatusCode.NO_PROCESSING
        except MethodNotFoundException as e:
            error = e
            statuscode = StatusCode.METHOD_NOT_FOUND
            response = getErrorArtifact(id, str(e), StatusCode.METHOD_NOT_FOUND)
        except Exception as e:
            error = e
            statuscode = StatusCode.SERVER_ERROR
            response = getErrorArtifact(id, str(e), StatusCode.SERVER_ERROR)

        return statuscode, response, error

    def inputProcessor(self, message: Any) -> StatusCode:
        workerConfig = self._config.get(f"conf.{self.name}")
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

        status_code: StatusCode = StatusCode.OK
        response: ResponseArtifact | None = None
        error: Any | None = None

        if "type" not in message or "route" not in message:
            return StatusCode.INVALID_INPUTS

        if message["type"] in [InputType.EVENT.value, InputType.EVENT]:
            if (
                "event" not in message
                or "details" not in message
                or "instanceId" not in message
            ):
                status_code = StatusCode.INVALID_INPUTS
                response = getErrorArtifact(
                    message["id"] if "id" in message else "",
                    "Invalid input type for event. event, details or instanceId not specified",
                    StatusCode.INVALID_INPUTS,
                )
            if message["event"] not in self._eventResolvers:
                return StatusCode.METHOD_NOT_FOUND

            status_code, response, error = self.run_resolver(
                self._eventResolvers[message["event"]],
                context,
                ("", {**message}),
            )

        elif message["type"] in [InputType.INPUT.value, InputType.INPUT]:
            if "id" not in message or "argumentId" not in message:
                status_code = StatusCode.INVALID_INPUTS
                response = getErrorArtifact(
                    message["id"] if "id" in message else "",
                    "Invalid input type. Id and argumentId not specified",
                    StatusCode.INVALID_INPUTS,
                )
                status_code = StatusCode.INVALID_INPUTS
            if "instanceId" in message and message["instanceId"] != bus.instanceId:
                return StatusCode.NO_PROCESSING

            # get the artifact from the message
            artifact = get_artifact(message, cache)
            if isinstance(artifact, tuple):
                status_code, response = artifact
            else:
                if artifact["method"] not in self._resolvers:
                    status_code = StatusCode.METHOD_NOT_FOUND
                    response = getErrorArtifact(
                        message["id"], "Method not found", StatusCode.METHOD_NOT_FOUND
                    )
                else:
                    continueExecution = evaluate_pre_hooks(
                        self._resolvers,
                        message,
                        artifact,
                        context,
                    )
                    if not continueExecution:
                        return StatusCode.OK

                    status_code, response, error = self.run_resolver(
                        self._resolvers[artifact["method"]],
                        context,
                        (message["id"], artifact["inputs"]),
                    )
                    if status_code == StatusCode.NO_PROCESSING:
                        return StatusCode.NO_PROCESSING

                    evaluate_exit(
                        message, response, cache, status_code, workerConfig, error
                    )
                    evaluate_post_hooks(bus, cache, message, artifact)

        evaluate_exit(message, response, cache, status_code, workerConfig, error)

        return StatusCode.INVALID_INPUTS
