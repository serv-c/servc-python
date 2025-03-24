import os
from typing import Any, Dict, List, Tuple

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from pyarrow import Schema, Table

from servc.svc.com.storage.lake import Lake, LakeTable
from servc.svc.config import Config


class Delta(Lake[DeltaTable]):
    _storageOptions: Dict[str, str] = {}

    _location_prefix: str

    _table: LakeTable

    def __init__(self, config: Config, table: LakeTable):
        super().__init__(config, table)

        self._table = table

        catalog_properties_raw = config.get("catalog_properties")
        if not isinstance(catalog_properties_raw, dict):
            catalog_properties_raw = {}

        # TODO: make generic for all storage types
        if catalog_properties_raw.get("type") == "local":
            self._location_prefix = str(
                catalog_properties_raw.get("location", "/tmp/delta")
            )
            self._storageOptions = {}
        else:
            self._location_prefix = os.path.join(
                str(catalog_properties_raw.get("warehouse")),
                str(catalog_properties_raw.get("s3.access-key-id")),
            )
            self._storageOptions = {
                "AWS_ACCESS_KEY_ID": str(
                    catalog_properties_raw.get("s3.access-key-id")
                ),
                "AWS_SECRET_ACCESS_KEY": str(
                    catalog_properties_raw.get("s3.secret-access-key")
                ),
                "AWS_ENDPOINT_URL": str(catalog_properties_raw.get("s3.endpoint")),
                "AWS_ALLOW_HTTP": "true",
                "aws_conditional_put": "etag",
            }

    def _connect(self):
        if self.isOpen:
            return None

        tablename = self._get_table_name()
        uri = os.path.join(self._location_prefix, tablename)
        self._conn = DeltaTable.create(
            table_uri=uri,
            name=tablename,
            schema=self._table["schema"],
            partition_by=self._table["partitions"],
            mode="ignore",
            storage_options=self._storageOptions,
        )

        return super()._connect()

    def optimize(self):
        table = self.getConn()

        print("Optimizing", self._get_table_name(), flush=True)
        table.optimize.compact()
        table.vacuum()
        table.cleanup_metadata()
        table.create_checkpoint()

    def getPartitions(self) -> Dict[str, List[Any]] | None:
        table = self.getConn()

        partitions: Dict[str, List[Any]] = {}
        for obj in table.partitions():
            for key, value in obj.items():
                if key not in partitions:
                    partitions[key] = []
                if value not in partitions[key]:
                    partitions[key].append(value)

        return partitions

    def getCurrentVersion(self) -> str | None:
        table = self.getConn()
        return str(table.version())

    def getVersions(self) -> List[str] | None:
        return [str(self.getCurrentVersion())]

    def insert(self, data: List[Any]) -> bool:
        table = self.getConn()
        write_deltalake(
            table,
            data=pa.Table.from_pylist(data, self.getSchema()),
            storage_options=self._storageOptions,
            mode="append",
        )
        return True

    def _filters(
        self,
        partitions: Dict[str, List[Any]] | None = None,
    ) -> List[Tuple[str, str, Any]] | None:
        filters: List[Tuple[str, str, Any]] = []
        if partitions is None:
            return None
        for key, value in partitions.items():
            if len(value) == 1:
                filters.append((key, "=", value[0]))
            else:
                filters.append((key, "in", value))
        return filters if len(filters) > 0 else None

    def overwrite(
        self, data: List[Any], partitions: Dict[str, List[Any]] | None = None
    ) -> bool:
        table = self.getConn()

        predicate: str | None = None
        filter = self._filters(partitions)
        if filter is not None:
            predicate = " & ".join([" ".join(x) for x in filter])

        write_deltalake(
            table,
            data=pa.Table.from_pylist(data, self.getSchema()),
            storage_options=self._storageOptions,
            mode="overwrite",
            predicate=predicate,
            engine="rust",
        )
        return True

    def readRaw(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
    ) -> Table:
        table = self.getConn()
        if version is not None:
            table.load_as_version(int(version))

        if options is None or not isinstance(options, dict):
            options = {}

        rcolumns = columns if columns[0] != "*" else None

        if options.get("filter", None) is not None:
            return table.to_pyarrow_dataset(
                partitions=self._filters(partitions),
            ).to_table(
                filter=options.get("filter"),
                columns=rcolumns,
            )
        return table.to_pyarrow_table(
            columns=rcolumns,
            partitions=self._filters(partitions),
        )

    def read(
        self,
        columns: List[str],
        partitions: Dict[str, List[Any]] | None = None,
        version: str | None = None,
        options: Any | None = None,
    ) -> Table:
        return self.readRaw(columns, partitions, version, options)

    def getSchema(self) -> Schema | None:
        table = self.getConn()

        return table.schema().to_pyarrow()

    def _close(self):
        if self._isOpen:
            self._isReady = False
            self._isOpen = False
            return True
        return False
