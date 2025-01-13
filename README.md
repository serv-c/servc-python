# servc-python

Serv-C implmentation for Python. Documentation can be found [here][1]

[![PyPI version](https://badge.fury.io/py/servc.svg)](https://pypi.org/project/servc/)
[![Serv-C](https://github.com/serv-c/servc-python/actions/workflows/servc.yml/badge.svg)][1]
[![Serv-C Compliancy](https://byob.yarr.is/serv-c/servc-python/servc-version)][1]
[![PyPI - Downloads](https://img.shields.io/pypi/dm/servc)](https://pypi.org/project/servc/)
[![Docker Pulls](https://img.shields.io/docker/pulls/yusufali/servc)](https://registry.hub.docker.com/r/yusufali/servc)

## Example

Here is the most simple example of use, starting a server to handle requests at the route `my-route`;

```python
from typing import Any

from servc.server import start_server
from servc.svc.com.worker.types import RESOLVER_CONTEXT, RESOLVER_RETURN_TYPE

def inputProcessor(
  messageId: str,
  payload: Any,
  context: RESOLVER_CONTEXT,
) -> RESOLVER_RETURN_TYPE:
  return True

# the method 'methodA' will be resolved by inputProcessor
start_server(
  resolver={
    "methodA": inputProcessor
  }
)
```


[1]: https://github.com/serv-c/docs