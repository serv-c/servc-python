from typing import Callable, List

from servc.com.service import ServiceComponent
from servc.io.input import ArgumentArtifact

ID_GENERATOR = Callable[[str, List[ServiceComponent], ArgumentArtifact], str]
