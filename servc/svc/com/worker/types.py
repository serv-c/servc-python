from typing import Any, Callable, Dict, List, TypedDict, Union

from servc.svc import Middleware
from servc.svc.com.bus import BusComponent
from servc.svc.com.cache import CacheComponent
from servc.svc.config import Config
from servc.svc.io.output import StatusCode

EMIT_EVENT = Callable[[str, Any], None]

RESOLVER_RETURN_TYPE = Union[StatusCode, Any, None]


class RESOLVER_CONTEXT(TypedDict):
    bus: BusComponent
    cache: CacheComponent
    middlewares: List[Middleware]
    config: Config


RESOLVER = Callable[
    [str, Any, RESOLVER_CONTEXT],
    RESOLVER_RETURN_TYPE,
]

RESOLVER_MAPPING = Dict[str, RESOLVER]
