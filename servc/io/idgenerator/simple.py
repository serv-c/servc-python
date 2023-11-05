import json
from hashlib import sha256
from typing import List

from servc.com.service import ServiceComponent
from servc.io.input import ArgumentArtifact


def simpleIDGenerator(
    route: str, _c: List[ServiceComponent], message: ArgumentArtifact
) -> str:
    input_string: str = "".join([route, json.dumps(message)])
    input_sha: str = sha256(input_string.encode("utf-8")).hexdigest()
    return input_sha
