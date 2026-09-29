"""让既有 b01/b02 测试读取候选导出，正式文件保持不动。"""

import importlib.util
import io
import pathlib
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

DRAFT = pathlib.Path(__file__).resolve().parent
PROJECT = DRAFT.parents[1]
TESTS = PROJECT / '00_draft_collection_02/tests'
sys.path[:0] = [str(DRAFT), str(TESTS)]
import a00_04_staged_path_transaction as transaction_module

original_spec = importlib.util.spec_from_file_location
stems = ('b01_trade_calendar', 'b02_futures_variety_calendar')


def candidate_spec(name, location, *args, **kwargs):
    location = pathlib.Path(location)
    if location.stem in stems:
        location = DRAFT / location.name
    return original_spec(name, location, *args, **kwargs)


# 保留原有断言，只把文件移动故障注入点转到抽象后真正的 os.replace 边界。
regression_source = (TESTS / 'test_b01_b02_commit_semantics.py').read_text(encoding='utf-8')
regression_source = regression_source.replace('import shutil\n', 'import shutil\nimport a00_04_staged_path_transaction as transaction_module\n')
regression_source = regression_source.replace('shutil.move', 'transaction_module.os.replace')
regression_source = regression_source.replace("patch.object(c01.shutil, 'move'", "patch.object(transaction_module.os, 'replace'")
regression_source = regression_source.replace("patch.object(module.shutil, 'move'", "patch.object(transaction_module.os, 'replace'")
(DRAFT / 'test_b01_b02_commit_semantics.py').write_text(regression_source, encoding='utf-8', newline='\n')

with patch.object(importlib.util, 'spec_from_file_location', side_effect=candidate_spec):
    for stem in stems:
        spec = candidate_spec(stem, DRAFT / f'{stem}.py')
        module = importlib.util.module_from_spec(spec)
        sys.modules[stem] = module
        spec.loader.exec_module(module)
    import test_b01_c01_c02_daily_tail_modes as daily
    import test_futures_variety_calendar_complete_catalog as catalog
    import test_b01_fragment_physical_contracts as physical
    # 日历测试导入时会更新 sys.path，显式指定候选回归文件。
    spec = original_spec('candidate_regression', DRAFT / 'test_b01_b02_commit_semantics.py')
    regression = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(regression)
    import test_staged_path_transaction as transaction_tests

physical.CASES = [case for case in physical.CASES if case[0] in (physical.C01, physical.C02)]
suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(module)
                          for module in (daily, catalog, physical, regression, transaction_tests))
with redirect_stdout(io.StringIO()):
    result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(not result.wasSuccessful())
