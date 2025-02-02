from typing import Any, Tuple

from servc.svc.com.cache import CacheComponent
from servc.svc.config import Config
from servc.svc.io.input import ArgumentArtifact, InputPayload
from servc.svc.io.output import ResponseArtifact, StatusCode
from servc.svc.io.response import getErrorArtifact


def evaluate_exit(
    message: InputPayload,
    response: ResponseArtifact | None,
    cache: CacheComponent,
    statusCode: StatusCode,
    config: Config,
    error: Any | None,
):
    if config.get("exiton5xx") and statusCode.value >= 500:
        print("Exiting due to 5xx error: ", error, flush=True)
        exit(1)
    if config.get("exiton4xx") and statusCode.value >= 400 and statusCode.value < 500:
        print("Exiting due to 4xx error: ", error, flush=True)
        exit(1)

    # allow specific exit to an error code
    error_str: str = str(statusCode.value)
    if config.get(f"exiton{error_str}"):
        print(f"Exiting due to {error_str} error: ", error, flush=True)
        exit(1)

    if response is not None and "id" in message and message["id"]:
        cache.setKey(message["id"], response)


def get_artifact(
    message: InputPayload, cache: CacheComponent
) -> ArgumentArtifact | Tuple[StatusCode, ResponseArtifact]:
    artifact = (
        cache.getKey(message["argumentId"])
        if message["argumentId"] not in ["raw", "plain"]
        else message["argument"]
    )

    if artifact is None or "method" not in artifact or "inputs" not in artifact:
        return (
            StatusCode.USER_ERROR,
            getErrorArtifact(
                message["id"],
                "Invalid argument. Need to specify method and inputs in payload",
                StatusCode.USER_ERROR,
            ),
        )

    return artifact
