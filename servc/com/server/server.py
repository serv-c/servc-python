from typing import List

from servc.com.bus import BusComponent, EmitFunction, OnConsuming
from servc.com.cache import CacheComponent
from servc.com.consumer import RESOLVER_MAPPING, ConsumerComponent
from servc.com.server.http import HTTPInterface
from servc.com.service import ComponentType
from servc.config import bus_url, cache_url, instance_id, port

blankEmitFunction: EmitFunction = lambda route, message: None
blankOnConsuming: OnConsuming = lambda route: None


def start_server(
    route: str,
    resolver: RESOLVER_MAPPING,
    eventResolver: RESOLVER_MAPPING = {},
    port: int = port,
    instance_id: str = instance_id,
    cache_url: str = cache_url,
    bus_url: str = bus_url,
    emitFunction: EmitFunction = blankEmitFunction,
    onConsuming: OnConsuming = blankOnConsuming,
    components: List[ComponentType] = [],
):
    bus = BusComponent(bus_url)
    cache = CacheComponent(cache_url)
    consumer = ConsumerComponent(
        route,
        instance_id,
        resolver,
        eventResolver,
        emitFunction,
        onConsuming,
        bus,
        cache,
        components,
    )

    interface = HTTPInterface(port, consumer)

    return interface
