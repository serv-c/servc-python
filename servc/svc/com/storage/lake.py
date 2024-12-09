from enum import Enum
from typing import Any, Dict, List, NotRequired, TypedDict

from pyarrow import Schema  # type: ignore

from servc.svc.com.storage import StorageComponent


class Medallion(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class LakeTable(TypedDict):
    name: str
    schema: Any
    partitions: List[str]
    medallion: Medallion
    options: NotRequired[Dict[str, Any]]


class Lake(StorageComponent):
    _table: Any

    def __init__(self, config: Dict[str, Any] | None, table: LakeTable | str):
        super().__init__(config)
        self._table = table
        if not isinstance(self._table, str) and "options" not in self._table:
            self._table["options"] = {}

    def _getDatabase(self) -> str:
        return self._config.get("database", "default")

    def _get_table_name(self) -> str:
        schema: str = self._getDatabase()

        name_w_medallion: str = ""
        if isinstance(self._table, str):
            name_w_medallion = self._table
        else:
            name_w_medallion = "".join(
                [self._table["medallion"].value, "-", self._table["name"]]
            )

        return ".".join([schema, name_w_medallion])

    @property
    def name(self) -> str:
        return self._get_table_name()

    def getPartitions(self) -> Dict[str, List[Any]] | None:
        return None

    def getSchema(self) -> Schema | None:
        return None

    def getCurrentVersion(self) -> str | None:
        return None

    def getVersions(self) -> List[str] | None:
        return None

    def insert(self, data: List[Any]) -> bool:
        return False

    def overwrite(
        self, data: List[Any], partitions: Dict[str, List[Any]] | None = None
    ) -> bool:
        return False

    def read(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
        raw: bool = False,
        batch: bool = False,
        data: Any | None = None,
    ) -> Any | None:
        if data:
            if raw:
                return data
            elif batch:
                return data.to_arrow_batch_reader()
            return data.to_arrow()
        return None
