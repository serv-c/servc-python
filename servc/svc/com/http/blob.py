import json
import os
from io import BytesIO
from multiprocessing import Process
from typing import Dict, List, Tuple

from flask import jsonify, request, send_file
from werkzeug.utils import secure_filename

from servc.svc import Middleware
from servc.svc.com.bus import BusComponent
from servc.svc.com.cache import CacheComponent
from servc.svc.com.http import HTTPInterface
from servc.svc.com.storage.blob import BlobStorage
from servc.svc.com.worker import RESOLVER_MAPPING
from servc.svc.config import Config
from servc.svc.io.output import ResponseArtifact, StatusCode
from servc.svc.io.response import getErrorArtifact
from servc.util import findType


def returnError(message: str, error: StatusCode = StatusCode.METHOD_NOT_FOUND):
    return jsonify(getErrorArtifact("", message, error))


class HTTPUpload(HTTPInterface):
    _blobStorage: BlobStorage

    _uploadcontainer: str

    def __init__(
        self,
        config: Config,
        bus: BusComponent,
        cache: CacheComponent,
        consumerthread: Process,
        resolvers: RESOLVER_MAPPING,
        eventResolvers: RESOLVER_MAPPING,
        components: List[Middleware],
    ):
        super().__init__(
            config, bus, cache, consumerthread, resolvers, eventResolvers, components
        )

        self._blobStorage = findType(components, BlobStorage)
        self._uploadcontainer = config.get("uploadcontainer") or "uploads"

    def get_upload_file_path(self, extra_params: Dict, fname: str) -> Tuple[str, str]:
        return self._uploadcontainer, secure_filename(fname)

    def _postMessage(self, extra_params: Dict | None = None):
        if request.method == "POST" and len(list(request.files)) > 0:
            if extra_params is None:
                extra_params = {}
            extra_params["files"] = []

            for filekey in list(request.files):
                file = request.files[filekey]
                container, remote_filename = self.get_upload_file_path(
                    extra_params, file.filename
                )

                if file.filename != "":
                    self._blobStorage.put_file(
                        container, remote_filename, file.stream.read()
                    )
                    extra_params["files"].append(remote_filename)

        return super()._postMessage(extra_params)

    def _getFile(self, id: str):
        try:
            response = self._cache.getKey(id)
        except json.JSONDecodeError:
            return returnError("Bad Response", StatusCode.INVALID_INPUTS)

        if isinstance(response, dict):
            art: ResponseArtifact = response  # type: ignore
            if "file" in art["responseBody"]:
                data = self._blobStorage.get_file(
                    art["responseBody"].get("container", self._uploadcontainer),
                    art["responseBody"]["file"],
                )
                if data is None:
                    return returnError("File not found", StatusCode.INVALID_INPUTS)
                if isinstance(data, bytes):
                    data = BytesIO(data)

                return send_file(
                    data,
                    as_attachment=True,
                    download_name=os.path.basename(art["responseBody"]["file"]),
                    mimetype="application/octet-stream",
                )
        return returnError("File not found", StatusCode.INVALID_INPUTS)

    def bindRoutes(self):
        super().bindRoutes()
        self._server.add_url_rule(
            "/fid/<id>", "_getFile", self._getFile, methods=["GET"]
        )
