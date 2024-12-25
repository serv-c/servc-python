#!/usr/bin/env python

import os
from typing import Any, List

from servc.server import start_server
from servc.svc import Middleware
from servc.svc.client.send import sendMessage
from servc.svc.com.bus import BusComponent
from servc.svc.com.cache import CacheComponent
from servc.svc.com.worker.types import EMIT_EVENT, RESOLVER_RETURN_TYPE
from servc.svc.idgen.simple import simple
from servc.svc.io.input import InputType


def test_resolver(
    id: str,
    bus: BusComponent,
    cache: CacheComponent,
    payload: str | list[str],
    _c: List[Middleware],
    emitEvent: EMIT_EVENT,
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


def test_hook(
    id: str,
    _b: BusComponent,
    _c: CacheComponent,
    p: List[Any],
    _ch: List[Middleware],
    _e: EMIT_EVENT,
) -> RESOLVER_RETURN_TYPE:
    return [x for x in p]


def fail(
    id: str,
    _b: BusComponent,
    _c: CacheComponent,
    _p: Any,
    _ch: List[Middleware],
    _e: EMIT_EVENT,
) -> RESOLVER_RETURN_TYPE:
    raise Exception("This is a test exception")


def main():
    return start_server(
        resolver={
            "test": test_resolver,
            "fail": fail,
            "hook": lambda id, _b, _c, p, _ch, _e: len(p),
            "hook_part": test_hook,
        },
    )


if __name__ == "__main__":
    main()
