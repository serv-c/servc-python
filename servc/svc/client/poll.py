import time

from servc.svc.client.get import get_result
from servc.svc.com.cache import CacheComponent
from servc.svc.io.output import ResponseArtifact


def pollMessage(id: str, cache: CacheComponent, timeout: int = 30) -> ResponseArtifact:
    result = get_result(id, cache)
    start = time.time()

    while True:
        if result["progress"] == 100:
            return result

        if timeout and time.time() - start > timeout:
            raise Exception("Timeout")
        time.sleep(1)
        result = get_result(id, cache)
