from flask import Flask, jsonify, request

from servc.com.bus import BusComponent
from servc.com.cache import CacheComponent
from servc.com.consumer import ConsumerComponent
from servc.com.service import ComponentType, ServiceComponent
from servc.io.client.send import sendMessage
from servc.io.idgenerator.simple import simpleIDGenerator
from servc.io.input import InputPayload, InputType


class HTTPInterface(ServiceComponent):
    _type: ComponentType = ComponentType.INTERFACE

    _port: int

    _server: Flask

    _consumer: ConsumerComponent

    _bus: BusComponent

    _cache: CacheComponent

    def __init__(self, port: int, consumer: ConsumerComponent):
        super().__init__()
        self._port = port
        self._server = Flask(__name__)
        self._consumer = consumer
        self._children.append(consumer)

        self._bus = consumer._bus
        self._cache = consumer._cache
        self.bindRoutes()

    def _connect(self):
        self._server.run(port=self._port, host="0.0.0.0")
        self._isOpen = True
        self._isReady = True

    def _close(self):
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
        return str(self.isReady)

    def _getResponse(self, id: str):
        return jsonify(self._cache.getKey(id))

    def _postMessage(self):
        content_type = request.headers["Content-Type"]
        if content_type == "application/json":
            body = request.json
            if body and body["route"] and body["inputs"]:
                force = False if "force" not in body else body["force"]
                value: InputPayload = {
                    "id": "0",
                    "route": body["route"],
                    "inputs": body["inputs"],
                    "argumentId": body["argumentId"] if body["argumentId"] else "plain",
                    "type": InputType.INPUT.value,
                }
                if body["instanceId"]:
                    value["instanceId"] = body["instanceId"]
                else:
                    return "bad request", 400
                id = sendMessage(
                    value,
                    self._bus,
                    self._cache,
                    force,
                    simpleIDGenerator,
                    self._consumer._children,
                )
                return id
        return "Content-Type not supported"

    def bindRoutes(self):
        self._server.add_url_rule("/healthz", "/healthz", self._health, methods=["GET"])
        self._server.add_url_rule("/readyz", "/readyz", self._health, methods=["GET"])
        self._server.add_url_rule(
            "/id/<id>", "/id/<id>", self._getResponse, methods=["GET"]
        )
        self._server.add_url_rule("/", "/", self._postMessage, methods=["POST"])
