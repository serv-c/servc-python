#!/usr/bin/env python

import os

from servc.server import start_server
from servc.svc.client.send import sendMessage
from servc.svc.idgen.simple import simple
from servc.svc.io.input import InputType


def test_resolver(id, bus, cache, payload: str | list[str], _c, emitEvent):
    if not isinstance(payload, list):
        sendMessage(
            {
                "type": InputType.INPUT.value,
                "id": "",
                "route": os.getenv("SEND_ROUTE", "my-response-queue"),
                "force": True,
                "argumentId": "",
                "argument": {
                    "method": "test",
                    "inputs": payload,
                },
            },
            bus,
            cache,
            simple,
        )
        return False
    for x in payload:
        if not isinstance(x, str):
            return False

    emitEvent(
        os.getenv("EVENT", "my-event"),
        payload,
    )
    return True


def fail(id, _b, _c, _p, _ch, _e):
    raise Exception("This is a test exception")


def main():
    return start_server(
        resolver={
            "test": test_resolver,
            "fail": fail,
        },
        # route="test",
    )


if __name__ == "__main__":
    main()
