from servc.svc.com.storage.lake import Lake, LakeTable
from servc.svc.config import Config


class TenantTable(Lake):
    _tenant_name: str

    _table: LakeTable

    def __init__(self, config: Config, table: LakeTable, tenant_name: str):
        super().__init__(config, table)
        self._tenant_name = tenant_name
        self._table = table

    def _get_table_name(self) -> str:
        schema: str = self._database

        name_w_medallion = "".join(
            [
                self._tenant_name,
                self._table["medallion"].value,
                "_",
                self._table["name"],
            ]
        )

        return ".".join([schema, name_w_medallion])
