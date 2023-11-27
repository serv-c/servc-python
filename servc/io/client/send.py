from typing import Dict, List

from servc.com.bus import BusComponent
from servc.com.cache import CacheComponent
from servc.com.service import ServiceComponent
from servc.io.client import BULK_JOB_DELIMITER, BULK_JOB_SEPARATOR
from servc.io.idgenerator import ID_GENERATOR
from servc.io.input import InputPayload, InputType


def sendMessage(
    message: InputPayload,
    bus: BusComponent,
    cache: CacheComponent,
    idGenerator: ID_GENERATOR,
    force: bool = False,
    services: List[ServiceComponent] = [],
) -> str:
    if "inputs" not in message:
        raise Exception("InputPayload must have inputs")

    id = (
        idGenerator(
            "-".join(["svc", message["route"]]),
            [bus, cache, *services],
            message["inputs"],
        )
        if "id" not in message or message["id"] in ["", None]
        else message["id"]
    )
    response = cache.getKey(id)

    if force:
        cache.deleteKey(id)

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
        "inputs": message["inputs"],
    }

    if "instanceId" in message and message["instanceId"]:
        inputObject["instanceId"] = message["instanceId"]
    if inputObject["argumentId"] not in ["plain", "raw"] and inputObject[
        "argumentId"
    ] in ["", None]:
        argumentId = idGenerator(
            "-".join(["arg", message["route"]]),
            [bus, cache, *services],
            message["inputs"],
        )
        cache.setKey(argumentId, message["inputs"])
        inputObject["argumentId"] = argumentId
        del inputObject["inputs"]

    bus.publishMessage(message["route"], inputObject, lambda *x: True)

    return id


def sendBulkMessage(
    message: Dict[str, InputPayload],
    bus: BusComponent,
    cache: CacheComponent,
    idGenerator: ID_GENERATOR,
    force: bool = False,
    services: List[ServiceComponent] = [],
) -> str:
    ids: List[str] = []
    for key, payload in message.items():
        job_id = sendMessage(payload, bus, cache, idGenerator, force, services)
        ids.append(BULK_JOB_SEPARATOR.join([key, job_id]))
    return BULK_JOB_DELIMITER.join(ids)
