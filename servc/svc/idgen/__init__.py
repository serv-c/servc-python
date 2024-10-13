from typing import Callable, List

from servc.svc import Middleware
from servc.svc.io.input import ArgumentArtifact

ID_GENERATOR = Callable[[str, List[Middleware], ArgumentArtifact], str]
