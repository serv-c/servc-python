from __future__ import annotations

import json
from typing import Any, Callable, Tuple

import pika  # type: ignore
import pika.channel  # type: ignore
import pika.exceptions  # type: ignore
import simplejson
from pika.adapters.asyncio_connection import AsyncioConnection  # type: ignore
from pika.adapters.blocking_connection import BlockingConnection  # type: ignore

from servc.svc.com.bus import BusComponent, InputProcessor, OnConsuming
from servc.svc.com.cache.redis import decimal_default
from servc.svc.io.input import EventPayload, InputPayload, InputType
from servc.svc.io.output import StatusCode

EVENT_EXCHANGE = "amq.fanout"


def queue_declare(
    channel: pika.channel.Channel, queueName: str, bindEventExchange: bool
):
    channel.queue_declare(
        queue=queueName, durable=True, exclusive=False, auto_delete=False
    )
    if bindEventExchange:
        channel.queue_bind(exchange=EVENT_EXCHANGE, queue=queueName)


def on_channel_open(channel: pika.channel.Channel, method: Callable, args: Tuple):
    return method(*args, channel)


class BusRabbitMQ(BusComponent):
    _url: str

    _conn: AsyncioConnection | BlockingConnection | None = None

    @property
    def isReady(self) -> bool:
        return (
            self._conn is not None
            and self._conn.is_open
            and (self.isBlockingConnection() or not self._conn.is_closing)
        )

    @property
    def isOpen(self) -> bool:
        return self.isReady

    def isBlockingConnection(self) -> bool:
        return isinstance(self._conn, BlockingConnection)

    def _connect(
        self, method: None | Callable = None, args: None | Tuple = None, blocking=True
    ):
        if not self.isOpen:
            if blocking:
                self._conn = BlockingConnection(pika.URLParameters(self._url))
                return self.get_channel(method, args)
            else:
                self._conn = AsyncioConnection(
                    parameters=pika.URLParameters(self._url),
                    on_open_callback=lambda _c: self.get_channel(method, args),
                    on_close_callback=self.on_connection_closed,
                )
                self._conn.ioloop.run_forever()  # type: ignore

    def _close(self, expected=True, reason: Any = None):
        print("Close method called", flush=True)
        if not expected:
            print("Unexpected close: ", reason, flush=True)
            exit(1)
        if self.isOpen or self.isReady:
            if (
                self._conn
                and not self._conn.is_closed
                and (self.isBlockingConnection() or not self._conn.is_closing)
            ):
                self._conn.close()
                self._conn = None

            return True
        return False

    def on_connection_closed(self, _conn: AsyncioConnection, reason: pika.exceptions):
        if reason == pika.exceptions.StreamLostError:
            # Async connection always is impossible to reconstitute for some reason
            print(str(reason), flush=True)
            self._conn = None
            exit(1)

    def get_channel(self, method: Callable | None, args: Tuple | None):
        if not self.isReady:
            self._connect(method, args)
        elif method and args and self._conn:
            if self.isBlockingConnection():

                try:
                    channel = self._conn.channel()
                    return on_channel_open(channel, method, args)

                # if the connection is lost, we will retry
                # this often happens for connections that are
                # left idle for a long time
                except pika.exceptions.StreamLostError as e:
                    print(str(e), flush=True)
                    self._conn = None
                    return self.get_channel(method, args)

            else:
                self._conn.channel(
                    on_open_callback=lambda c: on_channel_open(c, method, args)
                )

    def create_queue(self, queue: str, bindEventExchange: bool, channel: pika.channel.Channel | None = None) -> bool:  # type: ignore
        if not self.isReady:
            return self._connect(self.create_queue, (queue, bindEventExchange))
        if not channel:
            return self.get_channel(self.create_queue, (queue, bindEventExchange))

        queue_declare(channel, self.getRoute(queue), bindEventExchange)
        channel.close()
        return True

    def delete_queue(self, queue: str, channel: pika.channel.Channel | None = None) -> bool:  # type: ignore
        if not self.isReady:
            return self._connect(self.delete_queue, (queue,))
        if not channel:
            return self.get_channel(self.delete_queue, (queue,))

        channel.queue_delete(queue=self.getRoute(queue))
        channel.close()
        return True

    def get_queue_length(self, route, channel: pika.channel.Channel | None = None) -> int:  # type: ignore
        if not self.isReady:
            return self._connect(self.get_queue_length, (route,))
        if not channel:
            return self.get_channel(self.get_queue_length, (route,))

        try:
            queue = channel.queue_declare(
                queue=self.getRoute(route),
                passive=True,
                durable=True,
                exclusive=False,
                auto_delete=False,
            )
            channel.close()
            return queue.method.message_count
        except pika.exceptions.ChannelClosedByBroker:
            return 0

    def publishMessage(  # type: ignore
        self,
        route: str,
        message: InputPayload | EventPayload,
        channel: pika.channel.Channel | None = None,
    ) -> bool:
        if not self.isReady:
            return self._connect(self.publishMessage, (route, message))
        if not channel:
            return self.get_channel(self.publishMessage, (route, message))

        exchangeName = (
            EVENT_EXCHANGE
            if "type" in message
            and message["type"] in [InputType.EVENT.value, InputType.EVENT]
            else ""
        )

        channel.basic_publish(
            exchange=exchangeName,
            routing_key=self.getRoute(route),
            properties=None,
            body=simplejson.dumps(message, default=decimal_default, ignore_nan=True),
        )
        channel.close()

        return super().publishMessage(route, message)

    def subscribe(  # type: ignore
        self,
        route: str,
        inputProcessor: InputProcessor,
        onConsuming: OnConsuming | None,
        bindEventExchange: bool,
        channel: pika.channel.Channel | None = None,
    ) -> bool:
        if not self.isReady:
            self._connect(
                self.subscribe,
                (route, inputProcessor, onConsuming, bindEventExchange),
                blocking=False,
            )
        elif self.isBlockingConnection():
            self.close()
            return self.subscribe(route, inputProcessor, onConsuming, bindEventExchange)
        if not channel:
            return self.get_channel(
                self.subscribe, (route, inputProcessor, onConsuming, bindEventExchange)
            )
        channel.add_on_close_callback(lambda _c, r: self._close(False, r))
        channel.add_on_cancel_callback(lambda _c: self._close(False))

        queue_declare(channel, self.getRoute(route), bindEventExchange)
        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            self.getRoute(route),
            on_message_callback=lambda c, m, p, b: self.on_message(
                c, m, p, b, inputProcessor
            ),
            auto_ack=False,
        )

        if onConsuming:
            onConsuming(self.getRoute(route))

    def on_message(
        self,
        channel: pika.channel.Channel,
        method,
        properties: Any,
        body: Any,
        inputProcessor: InputProcessor,
    ):
        if not body:
            channel.basic_ack(method.delivery_tag)
        payload = json.loads(body.decode("utf-8"))
        result = inputProcessor(payload)

        if result == StatusCode.NO_PROCESSING:
            channel.basic_nack(method.delivery_tag)
        else:
            channel.basic_ack(method.delivery_tag)
