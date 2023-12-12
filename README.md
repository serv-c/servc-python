# svc-lib-server-python
Server implmentation of services for Python.

## Introduction

SERVC is an opionated implementation of microservices for scalable web architecture. It assumes :

1. all messages can be sent over a bus within the client SLA
2. responses can be tracked, stored, and retrieved from an intermediate storage layer
3. the state can be measured and hashed into some form of a string
4. the scope of worked can be cached using an id representative of the state

## Documentation

Servc's documentation can be found https://docs.servc.ca

## Example

Here is the most simple example of use, starting a server to handle requests at the route `my-route`;

```python
from servc.com.server.server import start_server

def inputProcessor(messageId, bus, cache, components, message, emit):
  pass

# the method 'methodA' will be resolved by inputProcessor
start_server(
  "my-route",
  {
    "methodA": inputProcessor
  }
)
```
