from multiprocessing import Process
from typing import Any, List, Tuple

from servc.svc import Middleware
from servc.svc.com.bus import BusComponent, OnConsuming
from servc.svc.com.bus.rabbitmq import BusRabbitMQ
from servc.svc.com.cache import CacheComponent
from servc.svc.com.cache.redis import CacheRedis
from servc.svc.com.http import HTTPInterface
from servc.svc.com.worker import RESOLVER_MAPPING, WorkerComponent
from servc.svc.config import Config


def blankOnConsuming(route: str):
    print("Consuming on route", route, flush=True)


COMPONENT_ARRAY = List[Tuple[type[Middleware], List[Any]]]


def compose_components(component_list: COMPONENT_ARRAY) -> List[Middleware]:
    components: List[Middleware] = []
    for [componentClass, args] in component_list:
        components.append(componentClass(*args))
    return components


def start_consumer(
    configDictionary: dict,
    resolver: RESOLVER_MAPPING,
    eventResolver: RESOLVER_MAPPING,
    configClass: type[Config],
    busClass: type[BusComponent],
    cacheClass: type[CacheComponent],
    workerClass: type[WorkerComponent],
    onConsuming: OnConsuming,
    components: COMPONENT_ARRAY,
):
    config = configClass()
    config.setAll(configDictionary)
    bus = busClass(
        config.get("conf.bus.url"),
        config.get("conf.bus.routemap"),
        config.get("conf.bus.prefix"),
    )
    cache = cacheClass(config.get("conf.cache.url"))
    consumer = workerClass(
        config.get("conf.bus.route"),
        config.get("conf.instanceid"),
        resolver,
        eventResolver,
        onConsuming,
        bus,
        busClass,
        cache,
        config,
        compose_components(components),
    )
    consumer.connect()


def start_server(
    resolver: RESOLVER_MAPPING,
    route: str | None = None,
    eventResolver: RESOLVER_MAPPING = {},
    configClass=Config,
    busClass=BusRabbitMQ,
    cacheClass=CacheRedis,
    workerClass=WorkerComponent,
    httpClass=HTTPInterface,
    onConsuming: OnConsuming = blankOnConsuming,
    components: COMPONENT_ARRAY = [],
    start=True,
):
    config = configClass()
    if route is not None:
        config.setValue("conf.bus.route", route)

    consumer = Process(
        target=start_consumer,
        args=(
            config.getAll(),
            resolver,
            eventResolver,
            configClass,
            busClass,
            cacheClass,
            workerClass,
            onConsuming,
            components,
        ),
        daemon=True,
    )
    consumer.start()

    bus = busClass(
        config.get("conf.bus.url"),
        config.get("conf.bus.routemap"),
        config.get("conf.bus.prefix"),
    )
    cache = cacheClass(config.get("conf.cache.url"))
    http = httpClass(
        int(config.get("conf.http.port")),
        bus,
        cache,
        config.get("conf.bus.route"),
        config.get("conf.instanceid"),
        consumer,
        resolver,
        eventResolver,
    )
    if start:
        http.start()

    return http
