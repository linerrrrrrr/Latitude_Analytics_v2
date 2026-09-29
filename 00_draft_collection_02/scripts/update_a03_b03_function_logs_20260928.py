"""b03 第 5—6 项：日志归位；不改变读取、业务校验、查询或事务。"""
import ast
import copy
import hashlib
import io
import json
import pathlib
import shutil
import tempfile
import textwrap
import tokenize

import nbformat

ROOT=pathlib.Path(__file__).resolve().parents[2]
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb')
PATH=ROOT/RELATIVE
original_bytes=PATH.read_bytes()
notebook=nbformat.read(PATH,as_version=4)
before=copy.deepcopy(notebook)
cells={c.id:c for c in notebook.cells}
editing_source=(ROOT/'00_draft_collection_02/scripts/update_a03_b01_function_logs_20260928.py').read_text(encoding='utf8')
exec(editing_source[editing_source.index('class FunctionEdit:'):editing_source.index('\ndef event(')])

def event(function,phase,status,fields='',prefix='planning_progress'):
    return f'''click.echo(
    f"{prefix}: dataset={{DATASET_NAME}}; function={function}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields else ''}elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
)'''

def boundary(edit,phase,fields='',setup='',failure_fields=''):
    edit.wrap(f'log_started_at = time.perf_counter()\nlog_phase = "{phase}"\n{setup}\n'+event(edit.function.name,phase,'started',fields),
        'except Exception as log_error:\n'+textwrap.indent(event(edit.function.name,phase,'failed',
            f'failed_phase={{log_phase}}; error={{type(log_error).__name__}}{failure_fields}')+'\nraise','    '))

def finish_return(edit,prefix,variable,phase,fields='',event_prefix='planning_progress'):
    expression=ast.unparse(edit.find(prefix).value)
    edit.replace(prefix,f'{variable} = {expression}\n'+event(edit.function.name,phase,'completed',fields,event_prefix)+f'\nreturn {variable}')

def progress(edit,prefix,fields,interval=10000):
    # 只统计原有循环访问；不增加迭代或额外扫描。
    edit.insert(prefix,'log_scanned_rows += 1\n'
        +f'if log_scanned_rows % {interval} == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
        +textwrap.indent(event(edit.function.name,'scan','running',fields)+'\nlog_last_progress_at = time.perf_counter()','    '))

scan_setup='log_scanned_rows = 0\nlog_last_progress_at = log_started_at'

edit=FunctionEdit('b03-c03-09','open_compatible_dataset')
edit.insert('dataset = ds.dataset','log_phase = "dataset_open"')
edit.insert('expected_file_schema =','log_phase = "fragment_schema"')
progress(edit,'if not physical_schema_matches', 'label={label}; scanned_fragments={log_scanned_rows}/{len(parquet_files)}; materialized=false',100)
finish_return(edit,'return (dataset, dataset_has_exact_schema_metadata','opened_dataset','dataset_open',
              'label={label}; scanned_fragments={log_scanned_rows}; metadata_exact={str(opened_dataset[1]).lower()}; materialized=false')
edit.insert('return (dataset, dataset_has_exact_schema_metadata','log_phase = "metadata"')
boundary(edit,'dataset_open','label={label}; path={table_path}; materialized=false',scan_setup,'; label={label}; scanned_fragments={log_scanned_rows}; materialized=false')

edit=FunctionEdit('b03-c03-09','open_exact_dataset')
edit.insert('if not is_exact:','log_phase = "metadata"')
edit.insert('return dataset',event('open_exact_dataset','dataset_open','completed','label={label}; metadata_exact=true; materialized=false'))
boundary(edit,'dataset_open','label={label}; path={table_path}; materialized=false',failure_fields='; label={label}; materialized=false')

for cell_id,name,variable in (
    ('a03-b03-calendar-validation','validate_calendar_frame','validated_calendar_df'),
    ('a03-b03-fact-validation','validate_overseas_futures_frame','validated_fact_df'),
):
    edit=FunctionEdit(cell_id,name)
    edit.insert('checked =','log_phase = "conversion"')
    edit.insert('if normalized.duplicated','log_phase = "primary_key"')
    edit.insert('for row in checked.to_pylist():','log_phase = "business_validation"')
    first='if row[\'fetch_result_status\'] not in' if name=='validate_calendar_frame' else "if row['snapshot_date']"
    progress(edit,first,'context={context}; scanned_rows={log_scanned_rows}/{len(frame)}')
    edit.insert('return normalized.sort_values','log_phase = "sort"')
    finish_return(edit,'return normalized.sort_values',variable,'validate',f'context={{context}}; rows={{len({variable})}}; scanned_rows={{log_scanned_rows}}')
    boundary(edit,'validate','context={context}; rows={len(frame)}',scan_setup,'; context={context}; scanned_rows={log_scanned_rows}')

edit=FunctionEdit('a03-b03-quality','ohlc_relation_warning_map')
edit.insert('return {}',event('ohlc_relation_warning_map','quality','completed','rows=0; warning_dates=0; persisted=false'))
edit.insert('checked_rows =','log_phase = "conversion"')
edit.insert('for row in checked_rows:','log_phase = "ohlc_relations"')
progress(edit,"high = row['high']",'scanned_rows={log_scanned_rows}/{len(frame)}; warning_dates={len(details_by_date)}; persisted=false')
returned=ast.unparse(edit.find('return {snapshot_date:').value)
edit.replace('return {snapshot_date:', 'quality_warning_by_date = '+returned+'\n'
    +event('ohlc_relation_warning_map','quality','completed','rows={len(frame)}; warning_dates={len(quality_warning_by_date)}; persisted=false')+'\n'
    +'for log_warning_date, log_warning_reason in quality_warning_by_date.items():\n'
    +textwrap.indent(event('ohlc_relation_warning_map','source_quality','completed','api_quality_warning: date={log_warning_date}; reason={log_warning_reason}; persisted=false'),'    ')
    +'\nreturn quality_warning_by_date')
boundary(edit,'quality','rows={len(frame)}; persisted=false',scan_setup,'; scanned_rows={log_scanned_rows}; persisted=false')

edit=FunctionEdit('a03-b03-read-fact','read_optional_fact')
finish_return(edit,'return empty_pandas','empty_fact_df','read','rows=0; outcome=no_fact_files; materialized=false')
edit.insert('table = dataset.to_table','log_phase = "materialize"\n'+event('read_optional_fact','materialize','started','path={table_path}'))
edit.insert('table = dataset.to_table',event('read_optional_fact','materialize','completed','rows={table.num_rows}; materialized=true'),after=True)
edit.insert('return validate_overseas_futures_frame','log_phase = "validate"')
finish_return(edit,'return validate_overseas_futures_frame','validated_fact_df','read','rows={len(validated_fact_df)}; materialized=true')
boundary(edit,'read','path={table_path}; materialized=false')

edit=FunctionEdit('b03-c03-11','query_overseas_futures_grid')
edit.insert('raw_df =','log_phase = "request"')
edit.insert('if raw_df is None:','log_phase = "response"')
edit.insert('return raw_df',event('query_overseas_futures_grid','fetch','completed','date={snapshot_date}; response_type={type(raw_df).__name__}; rows={len(raw_df) if isinstance(raw_df, pd.DataFrame) else \'unknown\'}; normalized=false; persisted=false',prefix='api_success'))
boundary(edit,'fetch','date={snapshot_date}; persisted=false',failure_fields='; date={snapshot_date}; persisted=false')

edit=FunctionEdit('a03-b03-normalize','normalize_overseas_futures_response')
finish_return(edit,'return empty_pandas','empty_fact_df','normalize','date={snapshot_date}; rows=0; outcome=empty_response; persisted=false')
edit.insert('missing_columns =','log_phase = "source_columns"')
edit.insert('response_df =','log_phase = "date_and_identity"')
edit.insert('numeric_values =','log_phase = "numeric_conversion"')
edit.insert('frame = pd.DataFrame','log_phase = "build_fact"')
edit.insert('return validate_overseas_futures_frame','log_phase = "output_validation"')
finish_return(edit,'return validate_overseas_futures_frame','normalized_fact_df','normalize','date={snapshot_date}; rows={len(normalized_fact_df)}; persisted=false',event_prefix='api_result')
boundary(edit,'normalize','date={snapshot_date}; persisted=false',failure_fields='; date={snapshot_date}; persisted=false')

edit=FunctionEdit('b03-c03-13','plan_overseas_futures_grids')
edit.insert('calendar_rows =','log_phase = "calendar_conversion"')
edit.insert('fact_counts =','log_phase = "fact_evidence"')
edit.insert('for row in calendar_rows:','log_phase = "classify_dates"')
progress(edit,"observation_date = row['observation_date']",'scanned_dates={log_scanned_rows}/{len(relevant_df)}; complete_grid_count={complete_count}; state_repair_count={len(state_repair_rows)}; api_pending_grid_count={len(pending_rows)}',100)
edit.insert('pending_df = pd.DataFrame','log_phase = "build_plan"')
edit.insert('return (pending_df, state_repair_df, complete_count)',event('plan_overseas_futures_grids','plan','completed',
    'complete_grid_count={complete_count}; state_repair_count={len(state_repair_df)}; api_pending_grid_count={len(pending_df)}; persisted=false',prefix='reconciliation_plan'))
boundary(edit,'plan','calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; start_date={start_date}; end_date={end_date}; persisted=false',scan_setup,'; scanned_dates={log_scanned_rows}; persisted=false')

edit=FunctionEdit('a03-b03-merge','full_fact_partition')
edit.insert('retained_df =','log_phase = "retain_untouched"')
edit.insert('complete_df = pd.concat','log_phase = "merge"')
edit.insert('return validate_overseas_futures_frame','log_phase = "output_validation"')
finish_return(edit,'return validate_overseas_futures_frame','merged_fact_df','generate','partition={partition_key}; retained_rows={len(retained_df)}; incoming_rows={len(incoming_df)}; complete_rows={len(merged_fact_df)}; persisted=false')
boundary(edit,'generate','partition={partition_key}; existing_rows={len(existing_df)}; incoming_rows={len(incoming_df)}; touched_dates={len(touched_dates)}; persisted=false',failure_fields='; partition={partition_key}; persisted=false')

for name in ('upgrade_fact_metadata','commit_complete_fact_partition','commit_calendar_partitions'):
    cell_id={'upgrade_fact_metadata':'b03-c03-15','commit_complete_fact_partition':'a03-b03-fact-commit','commit_calendar_partitions':'a03-b03-calendar-commit'}[name]
    edit=FunctionEdit(cell_id,name)
    calendar=name=='commit_calendar_partitions'
    metadata=name=='upgrade_fact_metadata'
    scope='calendar_leaves' if calendar else 'fact_root' if metadata else 'fact_leaf'
    fields='touched_dates={len(touched_dates)}' if calendar else 'rows={len(frame)}'+('' if metadata else '; partition={partition_key}')
    if calendar:
        edit.insert('return 0',event(name,'commit','skipped','reason=no_touched_dates; persisted=false; date_watermark=none'))
        edit.insert('partition_mask =','log_partition = partition_key\nlog_phase = "leaf_validation"\n'+event(name,'leaf_validation','started','partition={partition_key}; committed_partitions={log_committed_partitions}/{len(partition_keys)}'))
    edit.insert('staging_path.mkdir','log_phase = "staging_write"\n'+event(name,'staging_write','started','path={staging_path}; persisted=false'))
    edit.insert('staged_dataset =','log_phase = "staging_readback"\n'+event(name,'staging_readback','started','path={staging_path}; materialized=false'))
    edit.insert('if not pandas_to_arrow(staged_df',event(name,'staging_readback','completed','rows={len(staged_df)}; persisted=false'),after=True)
    edit.insert('committed_dataset =','log_phase = "formal_readback"\n'+event(name,'formal_readback','started','path={target_path}; transaction_state=pending'))
    edit.insert('if not pandas_to_arrow(committed_df',event(name,'formal_readback','completed','rows={len(committed_df)}; transaction_state=pending'),after=True)
    edit.insert('cleanup_recovery_paths = False',event(name,'rollback','failed','original_failed_phase={log_phase}; failed_paths={len(rollback_errors)}; backup_dir={backup_path}; quarantine_dir={quarantine_path}'),after=True)
    edit.insert('if rollback_errors:',event(name,'rollback','completed','recovery_complete=true; original_failed_phase={log_phase}'),after=True)
    # 安装日志放在旧的安装 try 内；其失败恢复与 finally 清理顺序保持原样。
    install_marker='if target_had_existing:' if metadata else 'if target_had_partition:' if calendar else 'target_path.mkdir'
    edit.insert(install_marker,'log_phase = "install"\n'+event(name,'install','started',f'scope={scope}; target={{target_path}}; transaction_state=pending'))
    # 成功日志放在现有 finally 结束之后，避免验收失败或清理异常时报成功。
    if calendar:
        install_try=next(n for n in ast.walk(edit.function) if isinstance(n,ast.Try) and any(h.name=='commit_error' for h in n.handlers))
        edit.insertions.setdefault(install_try.end_lineno,[]).append(textwrap.indent('log_committed_partitions += 1\n'+event(name,'commit_leaf','completed','partition={partition_key}; rows={len(committed_df)}; committed_partitions={log_committed_partitions}/{len(partition_keys)}; scope=calendar_leaf; persisted=true',prefix='partition_committed')+'\n',' '*install_try.col_offset))
        edit.insert('return len(touched_df)',event(name,'commit','completed','committed_partitions={log_committed_partitions}; touched_grids={len(touched_df)}')+'\nif log_committed_partitions:\n'+textwrap.indent(event(name,'calendar_state','completed','touched_grids={len(touched_df)}; completed_grids={int(touched_df[\'is_fetch_completed\'].sum())}; persisted=true; date_watermark=none'),'    '))
    else:
        detail='metadata_upgraded: rows={len(committed_df)}; full_root_swap=true; api_requests=0; ' if metadata else 'partition={partition_key}; rows={len(committed_df)}; '
        edit.insert('return committed_df',event(name,'commit','completed',detail+f'scope={scope}; persisted=true; calendar_state=not_updated',prefix='partition_committed'))
    boundary(edit,'commit',fields+f'; scope={scope}', 'log_partition = None\nlog_committed_partitions = 0' if calendar else '',
             '; partition={log_partition}; committed_partitions={log_committed_partitions}' if calendar else '' if metadata else '; partition={partition_key}')

for name in ('apply_calendar_completion','apply_calendar_failure'):
    edit=FunctionEdit('b03-c03-17',name)
    edit.insert('for index, row in updated_df.iterrows():','log_phase = "update_state"')
    first=next(n for n in ast.walk(edit.function) if isinstance(n,ast.For)).body[0]
    edit.insert(ast.unparse(first),'log_scanned_rows += 1\nif log_scanned_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'+textwrap.indent(event(name,'generate_state','running','scanned_rows={log_scanned_rows}/{len(calendar_df)}; updated_grids={log_updated_grids}; persisted=false')+'\nlog_last_progress_at = time.perf_counter()','    '))
    edit.insert("updated_df.at[index, 'updated_at'] =",'log_updated_grids += 1',after=True)
    edit.insert('return validate_calendar_frame','log_phase = "output_validation"')
    finish_return(edit,'return validate_calendar_frame','updated_calendar_df','generate_state','updated_grids={log_updated_grids}; rows={len(updated_calendar_df)}; persisted=false')
    boundary(edit,'generate_state','rows={len(calendar_df)}; '+('planned_grids={len(grid_results)}' if name=='apply_calendar_completion' else 'date={observation_date}; fetch_status={fetch_status}; fetch_completed=false')+'; persisted=false',scan_setup+'\nlog_updated_grids = 0','; updated_grids={log_updated_grids}; persisted=false')

edit=FunctionEdit('b03-c03-19','main')
for node in ast.walk(edit.function):
    if not isinstance(node,ast.Expr) or not isinstance(node.value,ast.Call) or ast.unparse(node.value.func)!='click.echo': continue
    printed=ast.unparse(node)
    if any(token in printed for token in ('metadata_upgraded:','reconciliation_plan:','api_success:')):
        edit.replace(printed,'')
    elif 'partition_committed:' in printed:
        edit.replace(printed,event('main','partition_batch','completed','partition={partition_key}; processed_grids={processed_grid_count}/{len(pending_df)}; calendar_rows={calendar_rows}'))
    elif 'state_repaired:' in printed:
        edit.replace(printed,event('main','repair_verification','completed','state_repaired: grids={len(repair_dates)}; calendar_rows={repaired_calendar_rows}; api_requests=0'))
edit.replace('if observation_date in api_quality_warning_by_date:','')
edit.insert('raw_df = query_overseas_futures_grid',event('main','fetch_batch','running','date={observation_date}; partition={partition_key}; completed_month_grids={processed_grid_count}/{len(pending_df)}'))
for node in ast.walk(edit.function):
    if not isinstance(node,ast.Assign) or not isinstance(node.value,ast.Call): continue
    if not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='to_table' for n in ast.walk(node.value)): continue
    variable=ast.unparse(node.targets[0])
    edit.insert(ast.unparse(node),event('main','materialize','started',f'frame={variable}; materialized=false'))
    edit.insert(ast.unparse(node),event('main','materialize','completed',f'frame={variable}; rows={{len({variable})}}; materialized=true'),after=True)
edit.apply()

# 局部说明与日志归属同步；原流程、单元格与入口不变。
cells['b03-c03-08'].source+='\n\n读取函数自行报告开始、fragment 扫描、metadata 判定和失败阶段；`materialized=false` 明确表示只打开并检查结构。实际 `to_table()` 由其所在函数或 main 报告物化起止和行数。fragment 每 100 个、业务扫描每 10000 行检查一次 2 秒日志间隔；只计数已有循环，不增加扫描。'
cells['b03-c03-10'].source=cells['b03-c03-10'].source.replace('日志中的 API 成功只在查询与归一化均成功后报告，仍不表示事实已提交。','`api_success` 由查询函数在收到非 None 响应后报告，并明确 `normalized=false; persisted=false`；归一化独立报告 `api_result`，随后仍需合并与正式提交。')
cells['b03-c03-12'].source+='\n\n规划函数自行报告三类日期数与累计分类进度；每 100 日期检查一次 2 秒输出间隔。OHLC 旁证由计算函数报告日期数、原值异常原因和扫描进度，结果仍为内存证据。短小的计数映射、状态判定和字段比较函数不单独刷日志。'
cells['a03-b03-merge-text'].source+='\n\n合并函数自行报告输入、保留和完整叶行数；归一化、合并与两个状态生成函数均以 `persisted=false` 报告内存结果，不能充当完成水位。'
cells['a03-b03-fact-commit-text'].source+='\n\n提交函数自行报告 staging、正式安装、复读与恢复阶段；只有原有 finally 完成后才报告当前事实叶提交。此时 `calendar_state=not_updated`，日历仍由后续提交形成持久完成凭证。'
cells['b03-c03-14'].source+='\n\n旧整根升级也由函数自行报告准备、staging、正式复读、恢复和最终提交；升级成功不等于日历状态已经修复。'
cells['a03-b03-calendar-commit-text'].source+='\n\n函数按叶报告累计成功数；单叶成功日志位于该叶 finally 之后。全部触达叶返回前才报告日历触达数与已完成数，并明确 `date_watermark=none`。错误状态可以成功落盘，但 `completed_grids=0` 不表示采集完成。'
cells['b03-c03-18'].source=cells['b03-c03-18'].source.replace('本轮统一现有入口日志；函数内部自行报告细粒度进度及日志归属在后续第 5—6 项落实。','读取、规划、查询、归一化、质量、合并和提交各自报告完整日志单元；`elapsed_s` 在函数内表示本次函数调用耗时，在 main 表示整批耗时。main 保留参数、批次与月份推进、由自身执行的物化、修复后复核和最终结果。生成成功、事实提交与日历完成状态分别由实际函数报告。')
cells['b03-c03-18'].source=cells['b03-c03-18'].source.replace('`elapsed_s` 为本次 main 的累计耗时，','')

for old_cell,new_cell in zip(before.cells,notebook.cells,strict=True):
    assert {k:v for k,v in old_cell.items() if k!='source'}=={k:v for k,v in new_cell.items() if k!='source'}
    if old_cell.cell_type=='code':
        comments=lambda text:[t.string for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type==tokenize.COMMENT]
        assert comments(old_cell.source)==comments(new_cell.source),old_cell.id
        ast.parse(new_cell.source)
assert before.metadata==notebook.metadata
nbformat.validate(notebook)
snapshot=pathlib.Path(tempfile.mkdtemp(prefix='a03-b03-function-logs-before-'))
paths=[ROOT/'AGENTS.md',ROOT/'.env.template']
for directory in ('02_Futures_Lakehouse','03_Futures_Database','config'):
    paths.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and p.suffix in ('.py','.ipynb','.md'))
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},ensure_ascii=False,indent=2),encoding='utf8')
for relative in (RELATIVE,RELATIVE.with_suffix('.py')):
    destination=snapshot/relative
    destination.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(ROOT/relative,destination)
assert PATH.read_bytes()==original_bytes
PATH.write_text(nbformat.writes(notebook)+'\n',encoding='utf8',newline='\n')
print(snapshot)
