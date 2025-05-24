from typing import List, TypeVar

from servc.svc import Middleware

T = TypeVar("T", bound=Middleware)


def findType(c: List[Middleware], type: type[T]) -> T:
    cf = [x for x in c if isinstance(x, type)]
    if len(cf) == 0:
        raise ValueError(f"Middleware {type} not found")
    return cf[0]
