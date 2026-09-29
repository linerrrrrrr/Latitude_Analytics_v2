"""更新已退出的 metadata 整根迁移测试；保留外部日历测试。"""
from pathlib import Path

path = Path(__file__).resolve().parents[2]/'00_draft_collection_02/tests/test_b03_metadata_upgrade.py'
text = path.read_text(encoding='utf8')
start = text.index('    def test_cli_upgrades_multiple_partitions_without_api', text.index('class OverseasFactMetadataUpgradeTests'))
end = text.index('\n\nif __name__', start)
text = text[:start]+'''    def test_description_drift_is_read_without_api_or_any_rewrite(self) -> None:
        with tempfile.TemporaryDirectory(prefix="overseas-description-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root, fact_path = self.prepare_lake(lake_root)
            before = parquet_hashes(silver_root)
            with mock.patch.object(self.module, "authenticate_jqdata", side_effect=AssertionError("no API")) as authentication:
                result = CliRunner().invoke(self.module.main, ["--lake-root", str(lake_root), "--write"])
            self.assertEqual(result.exit_code, 0, result.output)
            authentication.assert_not_called()
            self.assertIn("outcome=up_to_date", result.output)
            self.assertEqual(before, parquet_hashes(silver_root))
            self.assertEqual(len(self.module.read_optional_fact(fact_path)), 2)
            assert_no_recovery_paths(self, silver_root, self.module.TABLE_NAME)

    def test_identity_metadata_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="overseas-identity-") as directory:
            lake_root = pathlib.Path(directory)
            _, fact_path = self.prepare_lake(lake_root)
            leaf_file = next(fact_path.glob("year=*/month=*/*.parquet"))
            table = pq.ParquetFile(leaf_file).read()
            metadata = dict(table.schema.metadata)
            metadata[b"table_name"] = b"wrong_table"
            pq.write_table(table.replace_schema_metadata(metadata), leaf_file)
            with self.assertRaises(ValueError):
                self.module.read_optional_fact(fact_path)

    def test_physical_nullability_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="overseas-nullability-") as directory:
            lake_root = pathlib.Path(directory)
            _, fact_path = self.prepare_lake(lake_root)
            leaf_file = next(fact_path.glob("year=*/month=*/*.parquet"))
            table = pq.ParquetFile(leaf_file).read()
            fields = [pa.field(field.name, field.type, nullable=True, metadata=field.metadata)
                      if field.name == "instrument_code" else field for field in table.schema]
            pq.write_table(table.cast(pa.schema(fields, metadata=table.schema.metadata)), leaf_file)
            with self.assertRaises(ValueError):
                self.module.read_optional_fact(fact_path)
'''+text[end:]
text = text.replace('隔离验证 b03 外部市场日历与境外期货事实的 metadata 原子升级。',
                    '隔离验证外部日历 metadata 兼容与境外期货描述差异不重写历史。')
path.write_text(text, encoding='utf8', newline='\n')
