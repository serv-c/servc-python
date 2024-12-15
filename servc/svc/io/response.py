from typing import Any

from servc.svc.io.output import ResponseArtifact, StatusCode


def generateResponseArtifact(
    id: str,
    progress: float,
    responseBody: Any,
    statusCode: StatusCode,
    isError: bool,
) -> ResponseArtifact:
    progress = abs(progress)
    if progress < 1 and progress > 0:
        progress *= 100
    if progress > 100:
        progress = 100

    return {
        "id": id,
        "progress": progress,
        "responseBody": responseBody,
        "statusCode": statusCode.value,
        "isError": isError,
    }


def getErrorArtifact(
    id: str, errorMessage: str, statusCode: StatusCode
) -> ResponseArtifact:
    return generateResponseArtifact(id, 100, errorMessage, statusCode, True)


def getProgressArtifact(
    id: str, progress: float, message: str = ""
) -> ResponseArtifact:
    return generateResponseArtifact(id, progress, message, StatusCode.OK, False)


def getAnswerArtifact(id: str, message: Any) -> ResponseArtifact:
    return generateResponseArtifact(id, 100, message, StatusCode.OK, False)
