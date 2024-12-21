from servc.svc.client.send import sendMessage
from servc.svc.com.bus import BusComponent
from servc.svc.com.cache import CacheComponent
from servc.svc.idgen.simple import simple as idGenerator
from servc.svc.io.hooks import OnCompleteHook
from servc.svc.io.input import ArgumentArtifact, InputPayload, InputType


def process_complete_hook(
    bus: BusComponent,
    cache: CacheComponent,
    message: InputPayload,
    artifact: ArgumentArtifact,
    hook: OnCompleteHook,
) -> bool:
    inputs = (
        hook["inputs"]
        if "inputs" in hook
        else {
            "id": message["id"],
            "method": artifact["method"],
            "inputs": artifact["inputs"],
        }
    )

    payload: InputPayload = {
        "id": "",
        "type": InputType.INPUT.value,
        "route": hook["route"],
        "force": message["force"] if "force" in message else False,
        "argumentId": "",
        "argument": {
            "method": hook["method"],
            "inputs": inputs,
        },
    }
    sendMessage(payload, bus, cache, idGenerator)

    return True
