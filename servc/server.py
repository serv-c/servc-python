from multiprocessing import Process
from typing import List

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


COMPONENT_ARRAY = List[type[Middleware]]


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
    bus = busClass(config.get(f"conf.{busClass.name}"))
    cache = cacheClass(config.get(f"conf.{cacheClass.name}"))

    consumer = workerClass(
        resolver,
        eventResolver,
        onConsuming,
        bus,
        busClass,
        cache,
        config,
        [X(config.get(f"conf.{X.name}")) for X in components],
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

    bus = busClass(config.get(f"conf.{busClass.name}"))
    cache = cacheClass(config.get(f"conf.{cacheClass.name}"))
    http = httpClass(
        config.get(f"conf.{httpClass.name}"),
        bus,
        cache,
        consumer,
        resolver,
        eventResolver,
    )
    if start:
        http.start()

    return http
