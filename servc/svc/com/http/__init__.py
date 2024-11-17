import os
from multiprocessing import Process
from typing import Dict, Tuple, TypedDict

from flask import Flask, jsonify, request  # type: ignore

from servc.svc import ComponentType, Middleware
from servc.svc.client.send import sendMessage
from servc.svc.com.bus import BusComponent
from servc.svc.com.cache import CacheComponent
from servc.svc.com.worker import RESOLVER_MAPPING
from servc.svc.idgen.simple import simple
from servc.svc.io.input import InputPayload, InputType
from servc.svc.io.output import StatusCode


class ServiceInformation(TypedDict):
    instanceId: str
    queue: str
    methods: Dict[str, Tuple[str, ...]]
    eventHandlers: Dict[str, Tuple[str, ...]]


def methodGrabber(m: RESOLVER_MAPPING) -> Dict[str, Tuple[str, ...]]:
    j: Dict[str, Tuple[str, ...]] = {}
    for key, value in m.items():
        j[key] = value.__code__.co_varnames
    return j


class HTTPInterface(Middleware):
    _type: ComponentType = ComponentType.INTERFACE

    _port: int

    _server: Flask

    _bus: BusComponent

    _cache: CacheComponent

    _consumer: Process

    _route: str

    _instanceId: str

    _info: ServiceInformation

    def __init__(
        self,
        port: int,
        bus: BusComponent,
        cache: CacheComponent,
        route: str,
        instanceId: str,
        consumerthread: Process,
        resolvers: RESOLVER_MAPPING,
        eventResolvers: RESOLVER_MAPPING,
    ):
        super().__init__()
        self._port = port
        self._server = Flask(__name__)

        self._bus = bus
        self._cache = cache
        self._children.append(self._bus)
        self._children.append(self._cache)

        self._route = route
        self._instanceId = instanceId
        self._consumer = consumerthread

        self._info = {
            "instanceId": self._instanceId,
            "queue": self._bus.getRoute(self._route),
            "methods": methodGrabber(resolvers),
            "eventHandlers": methodGrabber(eventResolvers),
        }

    def _connect(self):
        self.bindRoutes()
        self._isOpen = True
        self._isReady = True
        print("Listening on port", self._port, flush=True)
        self._server.run(port=self._port, host="0.0.0.0")

    def _close(self):
        self._consumer.terminate()
        self._consumer.close()
        func = request.environ.get("werkzeug.server.shutdown")
        if func is None:
            raise RuntimeError("Not running with the Werkzeug Server")
        func()
        self._isOpen = False
        self._isReady = False
        return True

    def start(self):
        self.connect()

    def _health(self):
        consumerAlive = False
        try:
            consumerAlive = self._consumer.is_alive()
        except AssertionError:
            pid = self._consumer.pid
            try:
                os.kill(pid, 0)
            except OSError:
                consumerAlive = False
            else:
                consumerAlive = True
        print("health check:", self.isReady, consumerAlive)
        if self.isReady and consumerAlive:
            return "OK"
        else:
            return "Not OK", StatusCode.SERVER_ERROR.value

    def _getResponse(self, id: str):
        return jsonify(self._cache.getKey(id))

    def _postMessage(self):
        content_type = request.headers.get("Content-Type", None)
        if request.method == "GET":
            return self._getInformation()
        if content_type == "application/json":
            body = request.json

            must_have_keys = ("type",)
            for key in must_have_keys:
                if key not in body:
                    return f"missing key {key}", StatusCode.INVALID_INPUTS.value

            if body["type"] == InputType.EVENT.value:
                must_have_keys = ("event", "details")
                for key in must_have_keys:
                    if key not in body:
                        return f"missing key {key}", StatusCode.INVALID_INPUTS.value
                instanceId = (
                    body["instanceId"] if "instanceId" in body else self._instanceId
                )

                id = self._bus.emitEvent(
                    body["event"], instanceId, body["details"])
                return id
            elif body["type"] == InputType.INPUT.value:
                must_have_keys = ("route", "argument")
                for key in must_have_keys:
                    if key not in body:
                        return f"missing key {key}", StatusCode.INVALID_INPUTS.value
                payload: InputPayload = {
                    "type": body["type"],
                    "route": body["route"],
                    "argumentId": "",
                    "id": body["id"] if "id" in body else "",
                    "argument": body["argument"],
                }

                id = sendMessage(
                    payload,
                    self._bus,
                    self._cache,
                    simple,
                    True if "force" in body and body["force"] else False,
                    [],
                )
                return id
            else:
                return "bad request", StatusCode.INVALID_INPUTS.value

        return "Content-Type not supported"

    def _getInformation(self):
        return jsonify(self._info)

    def bindRoutes(self):
        self._server.add_url_rule(
            "/healthz", "healthz", self._health, methods=["GET"])
        self._server.add_url_rule(
            "/readyz", "readyz", self._health, methods=["GET"])
        self._server.add_url_rule(
            "/id/<id>", "_getResponse", self._getResponse, methods=["GET"]
        )
        self._server.add_url_rule(
            "/", "", self._postMessage, methods=["POST", "GET"])
