from __future__ import annotations

import functools
import json
import threading
from enum import Enum
from typing import Any, Callable, Tuple, TypedDict

import pika
import simplejson

from servc.com.bus import BusComponent, EmitFunction, InputProcessor, OnConsuming
from servc.com.cache.redis import decimal_default
from servc.io.input import InputType
from servc.io.output import StatusCode


class ExchangeTypes(Enum):
    DIRECT = "direct"
    FANOUT = "fanout"


class DeliveryMethod(TypedDict):
    delivery_tag: str


PayloadHandler = Callable[
    [
        pika.BlockingConnection,
        InputProcessor,
        EmitFunction,
        Any,
        bytes,
        DeliveryMethod,
    ],
    None,
]


def reply(
    channel,
    basic_deliver: DeliveryMethod,
    payload: Any,
    result: StatusCode,
    emitFunction: EmitFunction,
):
    if channel and channel.is_open:
        if result == StatusCode.NO_PROCESSING:
            channel.basic_nack(basic_deliver.delivery_tag)
        else:
            channel.basic_ack(basic_deliver.delivery_tag)

    if emitFunction:
        emitFunction(payload, payload["route"] if "route" in payload else "", result)


def payload_non_block(
    connection: pika.BlockingConnection,
    inputProcessor: InputProcessor,
    emitFunction: EmitFunction,
    channel: Any,
    body: bytes,
    basic_deliver: DeliveryMethod,
):
    payload = json.loads(body.decode("utf-8"))
    result = inputProcessor(payload)

    callback = functools.partial(
        reply, channel, basic_deliver, payload, result, emitFunction
    )
    connection.add_callback_threadsafe(callback)


def consume_non_block(
    arg: Tuple[
        pika.BlockingConnection,
        InputProcessor,
        EmitFunction,
    ],
    channel: Any,
    method: DeliveryMethod,
    properties: Any,
    body: Any,
):
    function_to_handle: PayloadHandler = payload_non_block
    (connection, inputProcessor, emitFunction) = arg
    thread = threading.Thread(
        target=function_to_handle,
        args=(connection, inputProcessor, emitFunction, channel, body, method),
    )
    thread.start()


class BusRabbitMQ(BusComponent):
    _url: str

    _conn: pika.BlockingConnection | None = None

    @BusComponent.isReady.getter
    def isReady(self) -> bool:
        return self._conn is not None and self._conn.is_open

    @BusComponent.isOpen.getter
    def isOpen(self) -> bool:
        return self.isReady

    def _connect(self):
        if not self.isOpen:
            params = pika.URLParameters(self._url)
            self._conn = pika.BlockingConnection(params)

    def _close(self):
        if self.isOpen or self._isReady:
            try:
                self._channel.stop_consuming()
                self._channel.close()
            except Exception as e:
                print(e)
            try:
                self._conn.close()
            except Exception as e:
                print(e)
            self._conn = None
            return True
        return False

    def queue_declare(self, channel: Any, queueName: str):
        channel.queue_declare(
            queue=queueName, durable=True, exclusive=False, auto_delete=False
        )

    def publishMessage(
        self,
        route: str,
        message: Any,
        emitFunction: EmitFunction = None,
    ) -> bool:
        if not self.isReady:
            self._connect()
            return self.publishMessage(route, message, emitFunction)

        channel = self._conn.channel()
        exchangeName = (
            "amqp.fanout"
            if "type" in message
            and message["type"] in [InputType.EVENT.value, InputType.EVENT]
            else ""
        )

        self.queue_declare(channel, route)
        channel.basic_publish(
            exchange=exchangeName,
            routing_key=route,
            properties=None,
            body=simplejson.dumps(message, default=decimal_default, ignore_nan=True),
        )
        channel.close()

        if emitFunction:
            emitFunction(message, route, 0)
        return super().publishMessage(route, message, emitFunction)

    def subscribe(
        self,
        route: str,
        inputProcessor: InputProcessor,
        emitFunction: EmitFunction = None,
        onConsuming: OnConsuming = None,
    ) -> bool:
        channel = self._conn.channel()

        self.queue_declare(channel, route)
        channel.basic_qos(prefetch_count=1)
        msg_cb = functools.partial(
            consume_non_block, (self._conn, inputProcessor, emitFunction)
        )
        channel.basic_consume(queue=route, on_message_callback=msg_cb, auto_ack=False)
        self._channel = channel
        channel.start_consuming()

        if onConsuming:
            onConsuming(route)

        return super().subscribe(route, inputProcessor, emitFunction, onConsuming)
