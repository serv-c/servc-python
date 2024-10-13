import json
from hashlib import sha256

from servc.svc.idgen import ID_GENERATOR

simple: ID_GENERATOR = lambda route, _c, message: sha256(
    "".join([route, json.dumps(message)]).encode("utf-8")
).hexdigest()
