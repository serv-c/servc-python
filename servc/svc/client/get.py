from servc.svc.com.cache import CacheComponent
from servc.svc.io.output import ResponseArtifact, StatusCode


def get_result(id: str, cache: CacheComponent) -> ResponseArtifact:
    cacheResult = cache.getKey(id)
    if (
        cacheResult
        and "progress" in cacheResult
        and "responseBody" in cacheResult
        and "statusCode" in cacheResult
    ):
        return cacheResult

    return {
        "id": id,
        "progress": 0,
        "responseBody": "Starting",
        "statusCode": StatusCode.OK.value,
        "isError": False,
    }
