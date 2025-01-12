from servc.svc.com.bus import BusComponent
from servc.svc.com.cache import CacheComponent
from servc.svc.com.worker.hooks.oncomplete import process_complete_hook
from servc.svc.com.worker.hooks.parallelize import (
    evaluate_part_pre_hook,
    process_post_part_hook,
)
from servc.svc.com.worker.types import RESOLVER_CONTEXT, RESOLVER_MAPPING
from servc.svc.io.hooks import Hooks, OnCompleteHook, PartHook
from servc.svc.io.input import ArgumentArtifact, InputPayload


def evaluate_post_hooks(
    bus: BusComponent,
    cache: CacheComponent,
    message: InputPayload,
    artifact: ArgumentArtifact,
) -> bool:
    if "hooks" not in artifact or not isinstance(artifact["hooks"], dict):
        return False
    hooks: Hooks = artifact["hooks"]

    if "part" in hooks and isinstance(hooks["part"], dict):
        if not all(
            x in hooks["part"] for x in ("part_id", "total_parts", "part_queue")
        ):
            return False
        partHook: PartHook = hooks["part"]

        completed = process_post_part_hook(bus, cache, message, artifact, partHook)
        if not completed:
            return True

    if "on_complete" in hooks and isinstance(hooks["on_complete"], list):
        for hook in hooks["on_complete"]:
            if not all(x in hook for x in ("type", "route", "method")):
                return False
            completeHook: OnCompleteHook = hook
            process_complete_hook(bus, cache, message, artifact, completeHook)

    return True


def evaluate_pre_hooks(
    resolvers: RESOLVER_MAPPING,
    message: InputPayload,
    artifact: ArgumentArtifact,
    context: RESOLVER_CONTEXT,
) -> bool:
    hooks: Hooks = {}
    if "hooks" in artifact and isinstance(artifact["hooks"], dict):
        hooks = artifact["hooks"]
    if "part" in hooks:
        return True

    for prehook in (evaluate_part_pre_hook,):
        continueExecution = prehook(resolvers, message, artifact, context)
        if not continueExecution:
            return False

    return True
