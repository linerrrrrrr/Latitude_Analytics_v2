"""只重放共享模块的已登记目标重叠检查，不执行文件操作。"""

import json
import pathlib
import time

PROJECT = pathlib.Path(__file__).resolve().parents[2]
OUTPUT = PROJECT / '00_draft_collection_02/staged_path_transaction_refactor/runtime_check_20260927'
records = []
for count in (6, 1200):
    # 与 b02 的 6 交易所 x 200 月相同层数和互不重叠关系。
    targets = [PROJECT / '00_draft_collection_02/cost_probe/silver/dim_futures_variety_calendar'
               / f'exchange_code={exchange}' / f'year={2010 + month // 12}' / f'month={month % 12 + 1}'
               for exchange in ('CCFX', 'XSGE', 'XDCE', 'XZCE', 'XINE', 'GFEX')
               for month in range(200)][:count]
    durations = []
    for repetition in range(3):
        previous_targets = []
        started = time.perf_counter()
        for target_path in targets:
            for previous_path in previous_targets:
                if target_path.is_relative_to(previous_path) or previous_path.is_relative_to(target_path):
                    raise AssertionError('该重放只使用互不重叠的目标')
            previous_targets.append(target_path)
        durations.append(time.perf_counter() - started)
    record = {'targets': count, 'pairs': count * (count - 1) // 2,
              'is_relative_to_calls': count * (count - 1), 'elapsed_s': durations,
              'scope': 'only overlap loop; no resolve, exists, mkdir, move, Parquet or logging'}
    records.append(record)
    print(json.dumps(record), flush=True)
(OUTPUT / 'path_cost.json').write_text(json.dumps(records, indent=2), encoding='utf-8')
