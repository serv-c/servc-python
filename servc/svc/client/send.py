from typing import List

from servc.svc import Middleware
from servc.svc.com.bus import BusComponent
from servc.svc.com.cache import CacheComponent
from servc.svc.idgen import ID_GENERATOR
from servc.svc.io.input import InputPayload, InputType


def sendMessage(
    message: InputPayload,
    bus: BusComponent,
    cache: CacheComponent,
    idGenerator: ID_GENERATOR,
    force: bool = False,
    services: List[Middleware] = [],
) -> str:
    if "argument" not in message:
        raise Exception("InputPayload must have inputs")

    id = (
        idGenerator(
            "-".join(["svc", message["route"]]),
            [bus, cache, *services],
            message["argument"],
        )
        if "id" not in message or message["id"] in ["", None]
        else message["id"]
    )
    if force or message.get("force", False):
        cache.deleteKey(id)
    response = cache.getKey(id)

    if (
        response
        and response["progress"] > 0
        and (not response["isError"])
        and force is False
    ):
        return id

    inputObject: InputPayload = {
        "type": InputType.INPUT.value,
        "route": message["route"],
        "argumentId": message["argumentId"] if "argumentId" in message else "",
        "id": id,
        "argument": message["argument"],
    }

    if "instanceId" in message and message["instanceId"]:
        inputObject["instanceId"] = message["instanceId"]
    if inputObject["argumentId"] not in ["plain", "raw"] and inputObject[
        "argumentId"
    ] in ["", None]:
        argumentId = idGenerator(
            "-".join(["arg", message["route"]]),
            [bus, cache, *services],
            message["argument"],
        )
        cache.setKey(argumentId, message["argument"])
        inputObject["argumentId"] = argumentId
        del inputObject["argument"]

    bus.publishMessage(message["route"], inputObject)

    return id
