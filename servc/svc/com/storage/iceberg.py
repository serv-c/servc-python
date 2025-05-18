from typing import Any, Dict, List

import pyarrow as pa
from pyarrow import RecordBatchReader, Schema
from pyarrow import Table as paTable
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression, In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import DataScan, Table
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField

from servc.svc.com.storage.lake import Lake, LakeTable
from servc.svc.com.storage.tenant import TenantTable
from servc.svc.config import Config


class IceBerg(Lake[Table]):
    name: str = "iceberg"

    # _table
    _catalog: Catalog

    def __init__(self, config: Config, table: LakeTable | str):
        super().__init__(config, table)

        catalog_name = str(config.get("catalog_name"))
        catalog_properties_raw = config.get("catalog_properties")
        if not isinstance(catalog_properties_raw, dict):
            catalog_properties_raw = {}
        catalog_properties: Dict = catalog_properties_raw

        self._catalog = load_catalog(
            catalog_name,
            **{**catalog_properties},
        )

    def _connect(self):
        if self.isOpen:
            return None

        tableName = self._get_table_name()
        try:
            # fix: hack error on rest api, unexplainable
            # requests.exceptions.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
            doesExist = self._catalog.table_exists(tableName)
        except:
            doesExist = False
        if doesExist:
            self._conn = self._catalog.load_table(tableName)

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

            self._catalog.create_namespace_if_not_exists(self._database)

            # TODO: undo this garbage when rest catalog works
            self._conn = self._catalog.create_table_if_not_exists(
                tableName,
                self._table["schema"],
                partition_spec=partitionSpec,
                sort_order=self._table["options"].get(
                    "sort_order", UNSORTED_SORT_ORDER
                ),
                properties=self._table["options"].get("properties", {}),
            )

        return super()._connect()

    def _close(self):
        if self._isOpen:
            self._isReady = False
            self._isOpen = False
            return True
        return False

    def getPartitions(self) -> Dict[str, List[Any]] | None:
        table = self.getConn()

        partitions: Dict[str, List[Any]] = {}
        for obj in table.inspect.partitions().to_pylist():
            for key, value in obj["partition"].items():
                field = key.replace("_partition", "")
                if field not in partitions:
                    partitions[field] = []
                partitions[field].append(value)
        return partitions

    def getSchema(self) -> Schema | None:
        table = self.getConn()

        return table.schema().as_arrow()

    def getCurrentVersion(self) -> str | None:
        table = self.getConn()

        snapshot = table.current_snapshot()
        if snapshot is None:
            return None
        return str(snapshot.snapshot_id)

    def getVersions(self) -> List[str] | None:
        table = self.getConn()

        snapshots: paTable = table.inspect.snapshots()
        chunked = snapshots.column("snapshot_id")
        return [str(x) for x in chunked.to_pylist()]

    def insert(self, data: List[Any]) -> bool:
        table = self.getConn()

        table.append(pa.Table.from_pylist(data, self.getSchema()))
        return True

    def overwrite(
        self, data: List[Any], partitions: Dict[str, List[Any]] | None = None
    ) -> bool:
        table = self.getConn()

        df = pa.Table.from_pylist(data, self.getSchema())
        if partitions is None or len(partitions) == 0:
            table.overwrite(df)
            return True

        # when partitions are provided, we need to filter the data
        boolPartition: List[BooleanExpression] = []
        for partition, values in partitions.items():
            boolPartition.append(In(partition, values))
        right_side = boolPartition[0]
        if len(boolPartition) > 1:
            for i in range(1, len(boolPartition)):
                right_side = And(right_side, boolPartition[i])

        table.overwrite(df, overwrite_filter=right_side)
        return True

    def readRaw(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
    ) -> DataScan:
        table = self.getConn()

        if options is None:
            options = {}
        if partitions is not None:
            boolPartition: List[BooleanExpression] = []
            for partition, values in partitions.items():
                boolPartition.append(In(partition, values))
            right_side = boolPartition[0]
            if len(boolPartition) > 1:
                for i in range(1, len(boolPartition)):
                    right_side = And(right_side, boolPartition[i])
            options["row_filter"] = And(
                options.get("row_filter", AlwaysTrue()), right_side
            )

        return table.scan(
            row_filter=options.get("row_filter", AlwaysTrue()),
            selected_fields=tuple(columns),
            limit=options.get("limit", None),
            snapshot_id=int(version) if version is not None else None,
        )

    def readBatch(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
    ) -> RecordBatchReader:
        data = self.readRaw(columns, partitions, version, options)
        return data.to_arrow_batch_reader()

    def read(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
    ) -> paTable:
        data = self.readRaw(columns, partitions, version, options)
        return data.to_arrow()


class IceBergTenant(TenantTable, IceBerg):
    pass
