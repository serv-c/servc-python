#!/usr/bin/env python

import os
from typing import Any

from servc.server import start_server
from servc.svc.client.send import sendMessage
from servc.svc.com.worker.types import RESOLVER_CONTEXT, RESOLVER_RETURN_TYPE
from servc.svc.idgen.simple import simple
from servc.svc.io.input import InputType


def test_resolver(
    id: str, payload: Any, context: RESOLVER_CONTEXT
) -> RESOLVER_RETURN_TYPE:
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
            context["bus"],
            context["cache"],
            simple,
        )
        return False
    for x in payload:
        if not isinstance(x, str):
            return False

    context["bus"].emitEvent(
        os.getenv("EVENT", "my-event"),
        payload,
    )
    return True


def test_hook(id: str, payload: Any, context: RESOLVER_CONTEXT) -> RESOLVER_RETURN_TYPE:
    return [x for x in payload]


def fail(id: str, payload: Any, context: RESOLVER_CONTEXT) -> RESOLVER_RETURN_TYPE:
    raise Exception("This is a test exception")


def main():
    return start_server(
        resolver={
            "test": test_resolver,
            "fail": fail,
            "hook": lambda id, p, _c: len(p),
            "hook_part": test_hook,
        },
    )


if __name__ == "__main__":
    main()
