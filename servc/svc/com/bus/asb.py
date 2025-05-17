from __future__ import annotations

import json
import threading
import time
from typing import Any

import simplejson
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiver
from azure.servicebus.management import ServiceBusAdministrationClient

from servc.svc.com.bus import BusComponent, InputProcessor, OnConsuming
from servc.svc.com.cache.redis import decimal_default
from servc.svc.io.input import EventPayload, InputPayload, InputType
from servc.svc.io.output import StatusCode


class AzureServiceBus(BusComponent):
    _url: str

    _conn: ServiceBusClient | None = None

    @property
    def isReady(self) -> bool:
        return self._conn is not None

    @property
    def isOpen(self) -> bool:
        return self.isReady

    def isBlockingConnection(self) -> bool:
        return isinstance(self._conn, ServiceBusClient)

    def _connect(self):
        if not self.isOpen:
            self._conn = ServiceBusClient.from_connection_string(self._url)

    def _close(self, expected=True, reason: Any = None):
        print("Close method called", flush=True)
        if not expected:
            print("Unexpected close: ", reason, flush=True)
            exit(1)
        if self.isOpen or self.isReady:
            if (
                self._conn
                # and not self._conn.is_closed
                # and (self.isBlockingConnection() or not self._conn.is_closing)
            ):
                self._conn.close()
                self._conn = None

            return True
        return False

    def publishMessage(self, route: str, message: InputPayload | EventPayload) -> bool:
        if not self.isReady or not self._conn:
            self._connect()
        if not self._conn:
            raise Exception("Service Bus connection is not established")

        isEvent = (
            True
            if "type" in message
            and message["type"] in [InputType.EVENT.value, InputType.EVENT]
            else False
        )
        asb_message = ServiceBusMessage(
            simplejson.dumps(message, default=decimal_default, ignore_nan=True)
        )

        # NOTE: azure service bus does not support event routing. thus, we must
        #       manually handle the event routing
        if isEvent:
            with ServiceBusAdministrationClient.from_connection_string(
                self._url
            ) as admin_client:
                for queue_properties in admin_client.list_queues():
                    sender = self._conn.get_queue_sender(
                        queue_name=self.getRoute(queue_properties.name)
                    )
                    with sender:
                        sender.send_messages(asb_message)

            return super().publishMessage(route, message)

        sender = self._conn.get_queue_sender(queue_name=self.getRoute(route))
        with sender:
            sender.send_messages(asb_message)

        return super().publishMessage(route, message)

    def subscribe(
        self,
        route: str,
        inputProcessor: InputProcessor,
        onConsuming: OnConsuming | None,
        bindEventExchange: bool,
    ) -> bool:
        if not self.isReady or not self._conn:
            self._connect()
        if not self._conn:
            raise Exception("Service Bus connection is not established")

        receiver = self._conn.get_queue_receiver(queue_name=self.getRoute(route))
        with receiver:
            received_msgs = receiver.receive_messages(max_message_count=1)
            for msg in received_msgs:
                thread = threading.Thread(
                    target=self.on_message,
                    args=(msg, receiver, inputProcessor),
                )
                thread.start()
                thread.join()

        time.sleep(1)
        self.subscribe(
            route,
            inputProcessor,
            onConsuming,
            bindEventExchange,
        )

        return True

    def on_message(
        self,
        body: Any,
        receiver: ServiceBusReceiver,
        inputProcessor: InputProcessor,
    ):
        payload = json.loads(str(body))
        result = inputProcessor(payload)

        if result == StatusCode.NO_PROCESSING:
            receiver.abandon_message(body)
        else:
            receiver.complete_message(body)
        print("Processed message", flush=True)
