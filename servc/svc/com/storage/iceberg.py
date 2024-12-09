from typing import Any, Dict, List

import pyarrow as pa  # type: ignore
from pyarrow import Schema
from pyarrow import Table as paTable
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression, In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField

from servc.svc.com.storage.lake import Lake, LakeTable


class IceBerg(Lake):
    # _config
    # _table
    _catalog: Catalog
    _ice: Table | None

    def __init__(self, config: Dict[str, Any] | None, table: LakeTable | str):
        super().__init__(config, table)

        if not config:
            raise Exception("Config is required")

        self._catalog = load_catalog(
            config.get("catalog_name", None),
            **{**config.get("catalog_properties", {})},
        )
        self._ice = None

    def _connect(self):
        if self.isOpen:
            return None

        tableName = self._get_table_name()
        doesExist = self._catalog.table_exists(tableName)
        if doesExist:
            self._ice = self._catalog.load_table(tableName)
        elif not doesExist and isinstance(self._table, str):
            raise Exception(f"Table {tableName} does not exist")
        else:
            # convert partitions to PartitionSpec
            partitions = []
            for part in self._table["partitions"]:
                field: NestedField = self._table["schema"].find_field(part)

                partitions.append(
                    PartitionField(
                        name=f"{part}_partition",
                        source_id=field.field_id,
                        field_id=1000 + field.field_id,
                        transform=self._table["options"].get(
                            f"{part}_transform", IdentityTransform()
                        ),
                    )
                )
            partitionSpec: PartitionSpec = PartitionSpec(*partitions)

            self._catalog.create_namespace_if_not_exists(self._getDatabase())
            self._ice = self._catalog.create_table(
                tableName,
                self._table["schema"],
                partition_spec=partitionSpec,
                sort_order=self._table["options"].get(
                    "sort_order", UNSORTED_SORT_ORDER
                ),
                properties=self._table["options"].get("properties", {}),
            )

        self._isReady = self._table is not None
        self._isOpen = self._table is not None
        return None

    def _close(self):
        if self._isOpen:
            self._isReady = False
            self._isOpen = False
            return True
        return False

    def getPartitions(self) -> Dict[str, List[Any]] | None:
        if self._ice is None:
            raise Exception("Table not connected")

        partitions: Dict[str, List[Any]] = {}
        for obj in self._ice.inspect.partitions().to_pylist():
            for key, value in obj["partition"].items():
                field = key.replace("_partition", "")
                if field not in partitions:
                    partitions[field] = []
                partitions[field].append(value)
        return partitions

    def getSchema(self) -> Schema | None:
        if self._ice is None:
            raise Exception("Table not connected")

        return self._ice.schema().as_arrow()

    def getCurrentVersion(self) -> str | None:
        if self._ice is None:
            raise Exception("Table not connected")
        snapshot = self._ice.current_snapshot()
        if snapshot is None:
            return None
        return str(snapshot.snapshot_id)

    def getVersions(self) -> List[str] | None:
        if self._ice is None:
            raise Exception("Table not connected")

        snapshots: paTable = self._ice.inspect.snapshots()
        chunked = snapshots.column("snapshot_id")
        return [str(x) for x in chunked.to_pylist()]

    def insert(self, data: List[Any]) -> bool:
        if self._ice is None:
            raise Exception("Table not connected")

        self._ice.append(pa.Table.from_pylist(data, self.getSchema()))
        return True

    def overwrite(
        self, data: List[Any], partitions: Dict[str, List[Any]] | None = None
    ) -> bool:
        if self._ice is None:
            raise Exception("Table not connected")

        if partitions is None or len(partitions) == 0:
            self._ice.overwrite(pa.Table.from_pylist(data, self.getSchema()))
            return True
        for partition, values in partitions:  # type: ignore
            self._ice.delete(In(partition, values))  # type: ignore
        return self.insert(data)

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
        if self._ice is None:
            raise Exception("Table not connected")

        if options is None:
            options = {}
        if partitions is not None:
            boolPartition: List[BooleanExpression] = []
            for partition, values in partitions:  # type: ignore
                boolPartition.append(In(partition, values))  # type: ignore
            right_side = boolPartition[0]
            if len(boolPartition) > 1:
                for i in range(1, len(boolPartition)):
                    right_side = And(right_side, boolPartition[i])
            options["row_filter"] = And(
                options.get("row_filter", AlwaysTrue()), right_side
            )

        scan = self._ice.scan(
            row_filter=options.get("row_filter", AlwaysTrue()),
            selected_fields=tuple(columns),
            limit=options.get("limit", None),
            snapshot_id=int(version) if version is not None else None,
        )
        return super().read(columns, partitions, version, options, raw, batch, scan)
