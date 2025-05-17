from io import BytesIO
from typing import List

from servc.svc.com.storage import StorageComponent
from servc.svc.config import Config


class BlobStorage(StorageComponent):
    name: str = "blob"

    def __init__(self, config: Config):
        super().__init__(config)

    def exists(self, container: str, prefix: str) -> bool:
        return False

    def get_file(self, container: str, prefix: str) -> bytes | BytesIO:
        return b""

    def put_file(
        self, container: str, prefix: str, data: bytes | str | BytesIO
    ) -> None:
        pass

    def delete_file(self, container: str, prefix: str) -> None:
        pass

    def list_files(self, container: str, prefix: str = "") -> List[str]:
        return []
