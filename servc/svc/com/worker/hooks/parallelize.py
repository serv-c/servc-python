from typing import List

from servc.svc.client.send import sendMessage
from servc.svc.com.bus import BusComponent
from servc.svc.com.cache import CacheComponent
from servc.svc.com.worker.types import RESOLVER_CONTEXT, RESOLVER_MAPPING
from servc.svc.idgen.simple import simple as idGenerator
from servc.svc.io.hooks import Hooks, OnCompleteHook, PartHook
from servc.svc.io.input import ArgumentArtifact, InputPayload, InputType


def process_post_part_hook(
    bus: BusComponent,
    cache: CacheComponent,
    message: InputPayload,
    artifact: ArgumentArtifact,
    partHook: PartHook,
) -> bool:
    # publish message to part queue
    payload: InputPayload = {
        "id": "",
        "type": InputType.INPUT.value,
        "route": partHook["part_queue"],
        "force": True,
        "argumentId": "",
        "argument": {
            "method": str(partHook["part_id"]),
            "inputs": {
                "id": message["id"],
                "method": artifact["method"],
                "inputs": artifact["inputs"],
            },
        },
    }
    sendMessage(payload, bus, cache, idGenerator)

    # check if all parts are complete
    if bus.get_queue_length(partHook["part_queue"]) == partHook["total_parts"]:
        bus.delete_queue(partHook["part_queue"])
        return True
    return False


def evaluate_part_pre_hook(
    resolvers: RESOLVER_MAPPING,
    message: InputPayload,
    artifact: ArgumentArtifact,
    context: RESOLVER_CONTEXT,
) -> bool:
    bus = context["bus"]
    cache = context["cache"]
    route = bus.route
    hooks: Hooks = artifact.get("hooks", {})
    method = artifact["method"]
    part_method = f"{method}_part"
    if part_method not in resolvers:
        return True

    jobs = resolvers[part_method](message["id"], artifact, context)
    if not isinstance(jobs, list):
        raise Exception(f"Resolver {part_method} did not return a list")

    # formulate on complete hook
    complete_hook: List[OnCompleteHook] = []
    if "on_complete" in hooks and isinstance(hooks["on_complete"], list):
        for hook in hooks["on_complete"]:
            if not all(x in hook for x in ("type", "route", "method")):
                continue
            inputs = (
                hook["inputs"]
                if "inputs" in hook
                else {
                    "id": message["id"],
                    "method": artifact["method"],
                    "inputs": artifact["inputs"],
                }
            )
            newHook: OnCompleteHook = {
                "type": hook["type"],
                "route": hook["route"],
                "method": hook["method"],
                "inputs": inputs,
            }
            complete_hook.append(newHook)

    # create task queue
    task_queue = f"part.{route}-{method}-{message['id']}"
    if len(jobs):
        bus.create_queue(task_queue, False)

    # publish messages to part queue
    payload_template: InputPayload = {
        "id": message["id"],
        "type": InputType.INPUT.value,
        "route": route,
        "force": True,
        "argumentId": "",
        "argument": {
            "method": method,
            "inputs": {},
            "hooks": {
                **hooks,
                "on_complete": complete_hook,
                "part": {
                    "part_id": 0,
                    "total_parts": len(jobs),
                    "part_queue": task_queue,
                },
            },
        },
    }
    for i, job in enumerate(jobs):
        payload: InputPayload = {**payload_template}
        payload["argument"]["inputs"] = job
        payload["argument"]["hooks"]["part"]["part_id"] = i
        sendMessage(payload, bus, cache, idGenerator)

    # do not continue execution
    return False
