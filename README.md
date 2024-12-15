# servc-python

Serv-C implmentation for Python. Documentation can be found https://docs.servc.io

[![PyPI version](https://badge.fury.io/py/servc.svg)](https://pypi.org/project/servc/)
[![Serv-C](https://github.com/serv-c/servc-python/actions/workflows/servc.yml/badge.svg)](https://docs.servc.io)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/servc)](https://pypi.org/project/servc/)
[![Docker Pulls](https://img.shields.io/docker/pulls/yusufali/servc)](https://registry.hub.docker.com/r/yusufali/servc)

## Example

Here is the most simple example of use, starting a server to handle requests at the route `my-route`;

```python
from servc.server import start_server

def inputProcessor(messageId, bus, cache, payload, components, emitEvent):
  pass

# the method 'methodA' will be resolved by inputProcessor
start_server(
  resolver={
    "methodA": inputProcessor
  }
)
```
