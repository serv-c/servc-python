from enum import Enum
from typing import Any, Dict, Generic, List, NotRequired, TypedDict, TypeVar

from pyarrow import RecordBatchReader, Schema, Table

from servc.svc import ComponentType
from servc.svc.com.storage import StorageComponent
from servc.svc.config import Config


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


T = TypeVar("T")


class Lake(Generic[T], StorageComponent):
    name: str = "lake"

    _type: ComponentType = ComponentType.LAKE

    _table: LakeTable | str

    _database: str

    _conn: T | None = None

    def __init__(self, config: Config, table: LakeTable | str):
        super().__init__(config)

        self._table = table
        self._database = str(config.get("database"))

        if not isinstance(self._table, str) and "options" not in self._table:
            self._table["options"] = {}

    def getConn(self) -> T:
        if not self._isOpen:
            self._connect()
        if self._conn is None:
            raise Exception("Table not connected")
        return self._conn

    def _get_table_name(self) -> str:
        schema: str = self._database

        name_w_medallion: str = ""
        if isinstance(self._table, str):
            name_w_medallion = self._table
        else:
            name_w_medallion = "".join(
                [self._table["medallion"].value, "_", self._table["name"]]
            )

        return ".".join([schema, name_w_medallion])

    @property
    def table(self) -> LakeTable | str:
        return self._table

    @property
    def tablename(self) -> str:
        return self._get_table_name()

    def _connect(self):
        self._isReady = self._conn is not None
        self._isOpen = self._conn is not None
        return self._conn is not None

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

    def readRaw(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
    ) -> Any:
        return None

    def readBatch(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
    ) -> RecordBatchReader:
        return None  # type: ignore

    def read(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
    ) -> Table:
        return None  # type: ignore
