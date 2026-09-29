"""各一次单日只读 CLI 真实来源检查；单次 90 秒，失败即停。"""

import hashlib
import json
import pathlib
import subprocess
import sys
import time

PROJECT = pathlib.Path(__file__).resolve().parents[2]
OUTPUT = PROJECT / '00_draft_collection_02/staged_path_transaction_refactor/runtime_check_20260927'
sys.path.insert(0, str(PROJECT))
from config.settings import settings

source_roots = [settings.futures_lake_root / 'silver' / name for name in
                ('dim_trade_calendar', 'dim_futures_variety_calendar')]
before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
          for root in source_roots for path in root.rglob('*.parquet')}
records = []
for stem in ('b01_trade_calendar', 'b02_futures_variety_calendar'):
    command = [sys.executable, '-B', '-X', 'utf8',
               str(PROJECT / '02_Futures_Lakehouse/a01_Futures_Market_Data' / f'{stem}.py'),
               '--start-date', '2026-08-28', '--end-date', '2026-08-28']
    started = time.perf_counter()
    print(f'live_readonly: {stem}; date=2026-08-28; write=false; timeout_s=90; status=started', flush=True)
    try:
        completed = subprocess.run(command, cwd=PROJECT, capture_output=True, text=True,
                                   encoding='utf-8', errors='replace', timeout=90)
        output = completed.stdout + completed.stderr
        return_code = completed.returncode
        timed_out = False
    except subprocess.TimeoutExpired as error:
        output = ''.join(value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value or ''
                         for value in (error.stdout, error.stderr))
        return_code = None
        timed_out = True
    # 日志仅保留业务进度和错误；不把配置身份或口令落到报告中。
    for value in (settings.jqdata_id, settings.jqdata_secret):
        if value:
            output = output.replace(str(value), '[redacted]')
    (OUTPUT / f'{stem}-live-readonly.log').write_text(output, encoding='utf-8')
    record = {'script': stem, 'start_date': '2026-08-28', 'end_date': '2026-08-28',
              'write': False, 'cli_attempts': 1, 'elapsed_s': time.perf_counter() - started,
              'return_code': return_code, 'timed_out': timed_out,
              'completion_lines': [line for line in output.splitlines()
                                   if 'phase=collect; status=completed' in line or 'phase=run; status=completed' in line]}
    records.append(record)
    print(json.dumps(record, ensure_ascii=False), flush=True)
    if return_code != 0:
        break
after = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
         for root in source_roots for path in root.rglob('*.parquet')}
assert before == after, '只读检查期间正式文件发生变化'
(OUTPUT / 'live_readonly.json').write_text(json.dumps({'runs': records, 'formal_files_unchanged': True}, ensure_ascii=False, indent=2), encoding='utf-8')
