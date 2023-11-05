import time
from typing import Union

from servc.com.cache import CacheComponent
from servc.io.client.get import get_result
from servc.io.output import ResponseArtifact


def pollMessage(
    id: str, cache: CacheComponent, timeout: Union[int, None]
) -> ResponseArtifact:
    result = get_result(id, cache)
    start = time.time()

    while True:
        if result["progress"] == 100:
            return result

        if timeout and time.time() - start > timeout:
            raise Exception("Timeout")
        time.sleep(1)
        result = get_result(id, cache)
