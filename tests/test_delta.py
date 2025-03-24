import unittest

import pyarrow as pa
import pyarrow.dataset as ds

from servc.svc.com.storage.delta import Delta
from servc.svc.com.storage.lake import LakeTable, Medallion

schema = pa.schema(
    [
        ("date", pa.string()),
        ("some_int", pa.int64()),
    ]
)

mytable: LakeTable = {
    "name": "test",
    "partitions": ["date"],
    "medallion": Medallion.BRONZE,
    "schema": pa.schema(
        [  # type: ignore
            pa.field("date", pa.string(), nullable=False),
            pa.field("some_int", pa.int64(), nullable=False),
        ]
    ),
}

config = {
    "database": "default",
    "catalog_name": "default",
    "catalog_properties": {
        "type": "local",
        "location": "/tmp/delta",
    },
}


class TestLakeDelta(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.iceberg = Delta(config, mytable)

    def test_connect(self):
        self.iceberg._connect()
        self.assertTrue(self.iceberg.isOpen)

    def test_name(self):
        self.assertEqual(self.iceberg.tablename, "default.bronze_test")

    def test_insert(self):
        self.iceberg.overwrite([])
        self.iceberg.insert([{"date": "2021-01-01", "some_int": 1}])
        data = self.iceberg.read(["date"]).to_pylist()
        self.assertEqual(len(data), 1)
        self.assertEqual(data, [{"date": "2021-01-01"}])

    def test_overwrite(self):
        self.iceberg.overwrite([])
        self.iceberg.insert([{"date": "2021-01-01", "some_int": 1}])
        self.iceberg.insert([{"date": "2021-01-02", "some_int": 1}])
        self.iceberg.insert([{"date": "2021-01-02", "some_int": 3}])

        data = self.iceberg.read(["date"]).to_pylist()
        self.assertEqual(len(data), 3)

        self.iceberg.overwrite([], {"date": ["'2021-01-02'"]})
        data = self.iceberg.read(["date"]).to_pylist()
        self.assertEqual(len(data), 1)

    def test_reading_partitions(self):
        self.iceberg.overwrite([])
        self.iceberg.insert([{"date": "2021-01-01", "some_int": 1}])
        self.iceberg.insert([{"date": "2021-01-02", "some_int": 1}])
        self.iceberg.insert([{"date": "2021-01-02", "some_int": 3}])

        data = self.iceberg.read(
            ["date"], partitions={"date": ["2021-01-01"]}
        ).to_pylist()
        self.assertEqual(len(data), 1)

        data = self.iceberg.read(
            ["date"], partitions={"date": ["2021-01-02"]}
        ).to_pylist()
        self.assertEqual(len(data), 2)

        data = self.iceberg.read(
            ["date"], partitions={"date": ["2021-01-02", "2021-01-01"]}
        ).to_pylist()
        self.assertEqual(len(data), 3)

        data = self.iceberg.read(
            ["date"],
            partitions={"date": ["2021-01-02"]},
            options={"filter": (ds.field("some_int") == 3)},
        ).to_pylist()
        self.assertEqual(len(data), 1)

        data = self.iceberg.read(
            ["date"],
            partitions={"date": ["2021-01-02"]},
            options={"filter": (ds.field("some_int") == 3)},
        ).to_pylist()
        self.assertEqual(len(data), 1)

    def test_version_travel(self):
        self.iceberg.insert([{"date": "2021-01-01", "some_int": 1}])
        orig_data = self.iceberg.read(["date"]).to_pylist()
        currentVersion = self.iceberg.getCurrentVersion()

        versions = self.iceberg.getVersions()
        self.assertGreater(len(versions), 0)
        self.assertIn(currentVersion, versions)

        self.iceberg.insert([{"date": "2021-01-02", "some_int": 1}])
        new_version = self.iceberg.getCurrentVersion()
        self.assertNotEqual(currentVersion, new_version)

        data = self.iceberg.read(["date"], version=currentVersion).to_pylist()
        self.assertEqual(len(data), len(orig_data))
        self.assertEqual(data, orig_data)

    def test_partitions(self):
        self.iceberg.overwrite([])
        self.iceberg.insert([{"date": "2021-01-01", "some_int": 1}])
        self.iceberg.insert([{"date": "2021-01-02", "some_int": 1}])
        self.iceberg.insert([{"date": "2021-01-02", "some_int": 3}])

        partitions = self.iceberg.getPartitions()
        self.assertEqual(list(partitions.keys()), ["date"])
        self.assertEqual(len(partitions["date"]), 2)
        self.assertIn("2021-01-01", partitions["date"])
        self.assertIn("2021-01-02", partitions["date"])

    def test_schema(self):
        schema = self.iceberg.getSchema()
        self.assertIsInstance(schema, pa.Schema)
        self.assertEqual(len(schema.names), 2)
        self.assertEqual(schema.names, ["date", "some_int"])

    def test_close(self):
        self.iceberg.close()
        self.iceberg.connect()
        self.iceberg.close()


if __name__ == "__main__":
    unittest.main()
