from __future__ import annotations

import json
import time
from typing import Any, Callable, Union

import simplejson
from pika.exchange_type import ExchangeType

from servc.com.bus import BusComponent, EmitFunction, InputProcessor, OnConsuming
from servc.com.bus.rabbitmq.rpc import RabbitMQConsumer
from servc.com.cache.redis import decimal_default
from servc.io.input import InputType
from servc.io.output import StatusCode


def on_message(
    self,
    busComponent: BusComponent,
    _unused_channel,
    basic_deliver,
    properties,
    body: Any,
    input_processor: InputProcessor,
    emit: EmitFunction,
):
    payload = json.loads(body.decode("utf-8"))
    result = input_processor(payload)

    if result == StatusCode.NO_PROCESSING:
        _unused_channel.basic_nack(basic_deliver.delivery_tag)
    else:
        self.acknowledge_message(basic_deliver.delivery_tag)

    if emit:
        emit(
            payload,
            payload["route"] if "route" in payload else "",
            _unused_channel,
            self._consumer_tag,
        )


class BusRabbitMQ(BusComponent):
    _consumer: Union[RabbitMQConsumer, None] = None
    _reconnect_delay: int | None = None

    def __init__(self, url: str):
        super().__init__(url)
        self._init_conn()

    def _init_conn(
        self,
        queueName: str | None = None,
        exchangeName: str | None = "",
        exchangeType: str | None = ExchangeType.direct,
        onMessage: Callable | None = None,
    ):
        self._consumer = RabbitMQConsumer(self._url)
        self._consumer.EXCHANGE = exchangeName
        self._consumer.EXCHANGE_TYPE = exchangeType

        if queueName:
            self._consumer.QUEUE = queueName
            self._consumer.ROUTING_KEY = queueName
        if onMessage:
            self._consumer.on_message = onMessage

        self._reconnect_delay = 0

    @BusComponent.isReady.getter
    def isReady(self):
        return (
            self._consumer is not None
            and self._consumer._connection is not None
            and self._consumer._connection.is_open
        )

    @BusComponent.isOpen.getter
    def isOpen(self):
        return (
            self._consumer is not None
            and self._consumer._connection is not None
            and self._consumer._connection.is_open
        )

    def _connect(self):
        if self._consumer is not None:
            self._consumer.connect()

    def _close(self):
        if self.isOpen or self.isReady:
            self._consumer.stop()
            return True
        return False

    def publishMessage(
        self, route: str, message: Any, emitFunction: EmitFunction
    ) -> bool:
        if not self.isReady:
            self.connect()
        if not self.isReady:
            return False

        exchangeName = (
            ""
            if "type" in message
            and message["type"] in [InputType.INPUT, InputType.INPUT.value]
            else "amqp.fanout"
        )

        channel = self._consumer._connection.channel()
        channel.basic_publish(
            exchange=exchangeName,
            routing_key=route,
            properties=None,
            body=simplejson.dumps(message, default=decimal_default, ignore_nan=True),
        )
        channel.close()

        if emitFunction:
            emitFunction(message, route)

        return True

    def subscribe(
        self,
        route: str,
        inputProcessor: InputProcessor,
        emitFunction: EmitFunction,
        onConsuming: OnConsuming,
    ) -> bool:
        if not self.isReady:
            self.connect()
        if not self.isReady:
            return False

        channel = self._consumer._connection.channel()
        channel.queue_declare(
            queue=route, durable=True, exclusive=False, auto_delete=False
        )
        channel.close()
        self._init_conn(
            queueName=route,
            onMessage=lambda cls, channel, bd, prop, body: on_message(
                cls, self, channel, bd, prop, body, inputProcessor, emitFunction
            ),
        )
        self._consumer.run()

        if onConsuming:
            onConsuming(route)

        return True

    def _maybe_reconnect(self):
        if self._consumer is not None and self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            time.sleep(reconnect_delay)
            self._init_conn(
                queueName=self._consumer.QUEUE, onMessage=self._consumer.on_message
            )

    def _get_reconnect_delay(self):
        if self._consumer._was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
