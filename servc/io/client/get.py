from servc.com.cache import CacheComponent
from servc.io.client import BULK_JOB_DELIMITER, BULK_JOB_SEPARATOR
from servc.io.output import ResponseArtifact


def get_result(id: str, cache: CacheComponent) -> ResponseArtifact:
    if BULK_JOB_DELIMITER and BULK_JOB_SEPARATOR in id:
        return get_bulk_result(id, cache)
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
        "statusCode": 200,
        "isError": False,
    }


def get_bulk_result(bulk_id: str, cache: CacheComponent) -> ResponseArtifact:
    results = {
        "id": bulk_id,
        "progress": 0,
        "statusCode": 200,
        "isError": False,
        "responseBody": {},
    }
    completed_jobs = 0

    key_ids = bulk_id.split(BULK_JOB_DELIMITER)
    for key_id in key_ids:
        key, id = key_id.split(BULK_JOB_SEPARATOR)

        result = get_result(id, cache)
        if result["progress"] == 100:
            completed_jobs += 1

        results["progress"] += result["progress"] / len(key_ids)
        results["responseBody"][key] = result["responseBody"]
        results["statusCode"] = (
            result["statusCode"]
            if result["statusCode"] > results["statusCode"]
            else results["statusCode"]
        )
        results["isError"] = result["isError"] or results["isError"]

    if completed_jobs == len(key_ids):
        results["progress"] = 100
    return results
