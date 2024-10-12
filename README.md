# servc-python

Serv-C implmentation for Python. Documentation can be found https://docs.servc.io

![Serv-C](https://git.yusufali.ca/serv-c/servc-python/actions/workflows/servc.yml/badge.svg)

## Example

Here is the most simple example of use, starting a server to handle requests at the route `my-route`;

```python
from servc.com.server.server import start_server

def inputProcessor(messageId, bus, cache, components, message, emit):
  pass

# the method 'methodA' will be resolved by inputProcessor
start_server(
  {
    "methodA": inputProcessor
  }
)
```
