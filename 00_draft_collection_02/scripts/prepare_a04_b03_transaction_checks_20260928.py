"""复用已有事务故障场景，替换为宏观报告及真实临时月叶。"""
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
source = (ROOT / '00_draft_collection_02/tests/test_a04_b02_shared_transaction.py').read_text(encoding='utf8')
for old, new in [('SHIBOR', '宏观发布'), ('shibor-txn-', 'macro-txn-'), ('test_b04_c02_interest_rate', 'test_b04_c03_macro_release'), ('fixtures.C02', 'fixtures.C03'), ('InterestRate', 'MacroRelease'), ('read_interest_calendar', 'read_macro_calendar'), ('INTEREST_RATE_DAILY_SCHEMA', 'MACRO_RELEASE_SCHEMA'), ("'rate'", "'value'"), ('dataset_name=interest_rate', 'dataset_name=macro_release'), ("'de9e1f3f'", "'14766fcd'"), ('b02_interest_rate', 'b03_macro_release')]:
    source = source.replace(old, new)
source = source.replace('self.first, self.second = date(2026, 7, 31), date(2026, 8, 3)\n        fixtures.build_calendar(self.lake, self.first, self.second)', '''self.first, self.second = date(2026, 7, 31), date(2026, 8, 31)
        built = CliRunner().invoke(fixtures.C01.main, [
            '--lake-root', str(self.lake), '--start-date', self.first.isoformat(),
            '--end-date', self.second.isoformat(), '--write',
        ])
        self.assertEqual(built.exit_code, 0, built.output)''')
start = source.index('        self.fact_df, self.counts = M.normalize_shibor_response(')
end = source.index('        M.commit_complete_fact_partition', start)
source = source[:start] + '''        frames = []
        self.counts = {}
        for report_name in M.SERIES_BY_REPORT:
            pending = self.calendar_df.loc[
                self.calendar_df['report_date'].eq(self.first)
                & self.calendar_df['series_code'].isin([s.series_code for s in M.SERIES_BY_REPORT[report_name]])
            ]
            if pending.empty:
                continue
            frame, counts = M.normalize_macro_release_response(
                [fixtures.source_row(report_name, self.first.replace(day=1))], report_name,
                pending, self.first.replace(day=1), self.first, M.datetime.now(M.timezone.utc),
            )
            frames.append(frame)
            self.counts.update(counts)
        self.fact_df = pd.concat(frames, ignore_index=True)
''' + source[end:]
start = source.index('    def test_later_calendar_failure_keeps_facts_then_repairs_without_api(')
end = source.index('    def test_missing_upstream_leaf_is_not_created(', start)
source = source[:start] + '''    def test_later_calendar_failure_keeps_facts_then_repairs_without_api(self):
        # July 无 API 修复；August 同月三个报告的最后一个日历安装失败。
        client = fixtures.FakeEastmoneySession(fixtures.all_report_rows(self.second.replace(day=1)))
        def replacing(source, target):
            if f'.{M.CALENDAR_TABLE_NAME}.staging-' in str(source) and str(target).endswith('month=8'):
                table = M.ds.dataset(source, format='parquet').to_table()
                if all(table['is_fetch_completed'].to_pylist()):
                    raise OSError('injected last August calendar install')
            return self.real_replace(source, target)
        with patch.object(TRANSACTION.os, 'replace', side_effect=replacing):
            failed = fixtures.run_c03(self.lake, None, client)
        self.assertIsInstance(failed.exception, OSError, failed.output)
        self.assertEqual(len(client.calls), 3)
        self.assertTrue(client.closed)
        calendar = M.read_macro_calendar(self.calendar_path)
        self.assertTrue(calendar.loc[calendar['month'].eq(7), 'is_fetch_completed'].all())
        self.assertEqual(int(calendar.loc[calendar['month'].eq(8), 'is_fetch_completed'].sum()), 11)
        facts, _ = M.read_optional_fact(self.fact_path)
        self.assertEqual(len(facts), 26)
        fact_files = hashes(self.fact_path)
        repaired = fixtures.run_c03(self.lake, None, None)
        self.assertEqual(repaired.exit_code, 0, repaired.output)
        self.assertEqual(fact_files, hashes(self.fact_path))
        self.assertTrue(M.read_macro_calendar(self.calendar_path)['is_fetch_completed'].all())

    def test_boolean_null_and_infinite_dirty_values_rejected_before_staging(self):
        for value in (True, None, float('inf')):
            with self.subTest(value=value):
                changed = self.fact_df.copy()
                changed['value'] = changed['value'].astype(object)
                changed.loc[changed.index[0], 'value'] = value
                with patch.object(M, 'write_fact_staging', side_effect=AssertionError('must not stage')) as writing:
                    with self.assertRaises(ValueError):
                        M.commit_complete_fact_partition(changed, self.lake, (2026, 7))
                writing.assert_not_called()
                self.assertEqual(self.before, hashes(self.lake))

    def test_description_metadata_does_not_rewrite_completed_facts(self):
        session = fixtures.FakeEastmoneySession(fixtures.all_report_rows(self.second.replace(day=1)))
        completed = fixtures.run_c03(self.lake, None, session)
        self.assertEqual(completed.exit_code, 0, completed.output)
        for path in self.fact_path.rglob('*.parquet'):
            table = M.pq.ParquetFile(path).read()
            metadata = dict(table.schema.metadata)
            metadata[b'description'] = b'older descriptive text'
            M.pq.write_table(table.replace_schema_metadata(metadata), path)
        before = hashes(self.lake)
        with patch.object(M, 'upgrade_fact_metadata', side_effect=AssertionError('no description migration')):
            result = fixtures.run_c03(self.lake, None, None)
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(before, hashes(self.lake))

''' + source[end:]
assert 'shibor' not in source and 'C02' not in source
(ROOT / '00_draft_collection_02/tests/test_a04_b03_shared_transaction.py').write_text(source, encoding='utf8', newline='\n')
print('macro transaction checks prepared')
