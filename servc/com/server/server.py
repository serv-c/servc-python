from multiprocessing import Process
from typing import List

from servc.com.bus import BusComponent, EmitFunction, OnConsuming
from servc.com.bus.rabbitmq import BusRabbitMQ
from servc.com.cache import CacheComponent
from servc.com.cache.redis import CacheRedis
from servc.com.consumer import RESOLVER_MAPPING, ConsumerComponent
from servc.com.server.http import HTTPInterface
from servc.com.service import ComponentType
from servc.config import bus_url as default_bus_url
from servc.config import cache_url as default_cache_url
from servc.config import instance_id as default_instance_id
from servc.config import port as default_port

blankEmitFunction: EmitFunction = lambda route, message, code: None
blankOnConsuming: OnConsuming = lambda route: None


def start_consumer(
    route: str,
    resolver: RESOLVER_MAPPING,
    eventResolver: RESOLVER_MAPPING,
    busClass: type[BusComponent],
    cacheClass: type[CacheComponent],
    consumerClass: type[ConsumerComponent],
    instance_id: str,
    cache_url: str,
    bus_url: str,
    emitFunction: EmitFunction,
    onConsuming: OnConsuming,
    components: List[ComponentType],
):
    bus = busClass(bus_url)
    cache = cacheClass(cache_url)
    consumer = consumerClass(
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
    consumer.connect()


def test_start_http(
    route: str,
    busClass: type[BusComponent],
    cacheClass: type[CacheComponent],
    consumerProcess: Process,
    httpClass: type[HTTPInterface],
    cache_url: str,
    bus_url: str,
    port: int,
    instance_id: str,
):
    bus = busClass(bus_url)
    cache = cacheClass(cache_url)
    http = httpClass(port, bus, cache, route, instance_id, consumerProcess)
    http.start()


def start_server(
    route: str,
    resolver: RESOLVER_MAPPING,
    eventResolver: RESOLVER_MAPPING = {},
    port: int = default_port,
    busClass=BusRabbitMQ,
    cacheClass=CacheRedis,
    consumerClass=ConsumerComponent,
    httpClass=HTTPInterface,
    instance_id: str = default_instance_id,
    cache_url: str = default_cache_url,
    bus_url: str = default_bus_url,
    emitFunction: EmitFunction = blankEmitFunction,
    onConsuming: OnConsuming = blankOnConsuming,
    components: List[ComponentType] = [],
    start=True,
    returnProcess=False,
):
    consumer = Process(
        target=start_consumer,
        args=(
            route,
            resolver,
            eventResolver,
            busClass,
            cacheClass,
            consumerClass,
            instance_id,
            cache_url,
            bus_url,
            emitFunction,
            onConsuming,
            components,
        ),
    )
    consumer.start()

    if returnProcess:
        http = Process(
            target=test_start_http,
            args=(
                route,
                busClass,
                cacheClass,
                consumer,
                httpClass,
                cache_url,
                bus_url,
                port,
                instance_id,
            ),
        )
        http.start()
        return [http, consumer]

    bus = busClass(bus_url)
    cache = cacheClass(cache_url)
    interface = httpClass(port, bus, cache, route, instance_id, consumer)

    if start:
        interface.start()

    return interface
