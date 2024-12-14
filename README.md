# servc-python

Serv-C implmentation for Python. Documentation can be found https://docs.servc.io

[![PyPI version](https://badge.fury.io/py/servc.svg)](https://badge.fury.io/py/servc)
![Serv-C](https://github.com/serv-c/servc-python/actions/workflows/servc.yml/badge.svg)

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
