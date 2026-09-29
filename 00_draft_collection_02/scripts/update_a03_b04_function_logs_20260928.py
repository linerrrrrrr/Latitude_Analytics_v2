"""第 5—6 项：函数内进度与日志归位，不改变业务和事务。"""
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
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b04_external_index.ipynb')
PATH=ROOT/RELATIVE
original_bytes=PATH.read_bytes()
notebook=nbformat.read(PATH,4)
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
    edit.insert(prefix,'log_scanned_rows += 1\n'
        +f'if log_scanned_rows % {interval} == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
        +textwrap.indent(event(edit.function.name,'scan','running',fields)+'\nlog_last_progress_at = time.perf_counter()','    '))

scan_setup='log_scanned_rows = 0\nlog_last_progress_at = log_started_at'
edit=FunctionEdit('b03-c04-09','open_exact_dataset')
edit.insert('parquet_files =','log_phase = "discovery"')
edit.insert('dataset = ds.dataset','log_phase = "dataset_open"\n'+event('open_exact_dataset','discovery','completed','label={label}; files={len(parquet_files)}; materialized=false'))
edit.insert('if not reconstructed_schema','log_phase = "schema"')
edit.insert('return dataset',event('open_exact_dataset','read_structure','completed','label={label}; files={len(parquet_files)}; metadata_exact=true; materialized=false'))
boundary(edit,'read_structure','label={label}; path={table_path}; materialized=false',failure_fields='; label={label}; materialized=false')

for cell_id,name,variable in (
    ('a03-b04-calendar-validation','validate_calendar_frame','validated_calendar_df'),
    ('a03-b04-fact-validation','validate_external_index_frame','validated_index_df'),
):
    edit=FunctionEdit(cell_id,name)
    edit.insert('checked =','log_phase = "conversion"')
    edit.insert('if normalized.duplicated','log_phase = "primary_key"')
    loop=next(n for n in ast.walk(edit.function) if isinstance(n,ast.For))
    edit.insert(ast.unparse(loop),'log_phase = "business_validation"')
    progress(edit,ast.unparse(loop.body[0]),'context={context}; scanned_rows={log_scanned_rows}/{len(frame)}')
    edit.insert('return normalized.sort_values','log_phase = "sort"')
    finish_return(edit,'return normalized.sort_values',variable,'validate',f'context={{context}}; rows={{len({variable})}}; scanned_rows={{log_scanned_rows}}')
    boundary(edit,'validate','context={context}; rows={len(frame)}',scan_setup,'; context={context}; scanned_rows={log_scanned_rows}')

edit=FunctionEdit('a03-b04-read-fact','read_optional_fact')
finish_return(edit,'return empty_pandas','empty_fact_df','read','rows=0; outcome=no_fact_files; materialized=false')
edit.insert('table = dataset.to_table','log_phase = "materialize"\n'+event('read_optional_fact','materialize','started','path={table_path}'))
edit.insert('table = dataset.to_table',event('read_optional_fact','materialize','completed','rows={table.num_rows}; materialized=true'),after=True)
edit.insert('return validate_external_index_frame','log_phase = "validate"')
finish_return(edit,'return validate_external_index_frame','validated_index_df','read','rows={len(validated_index_df)}; materialized=true')
boundary(edit,'read','path={table_path}; materialized=false')

edit=FunctionEdit('b03-c04-11','pending_request_ranges')
edit.insert('return []',event('pending_request_ranges','plan_ranges','completed','indicator={source_indicator_id}; pending_dates=0; ranges=0; persisted=false'))
edit.insert('required_mask =','log_phase = "select_required"')
edit.insert('for required_date in required_dates:','log_phase = "split_ranges"')
edit.insert('covered_dates =','log_phase = "coverage"')
edit.insert('return ranges',event('pending_request_ranges','plan_ranges','completed','indicator={source_indicator_id}; pending_dates={len(pending_dates)}; ranges={len(ranges)}; persisted=false'))
boundary(edit,'plan_ranges','indicator={source_indicator_id}; pending_dates={len(pending_dates)}; persisted=false',failure_fields='; indicator={source_indicator_id}; persisted=false')

edit=FunctionEdit('a03-b04-session','create_eastmoney_session')
edit.insert('return session',event('create_eastmoney_session','session','completed','business_requests=0; retry_total=3'))
boundary(edit,'session','business_requests=0; retry_total=3')

edit=FunctionEdit('a03-b04-query','query_eastmoney_indicator_range')
identity='indicator={source_indicator_id}; range={range_start}/{range_end}'
edit.insert('response = session.get','log_phase = "request"\n'+event('query_eastmoney_indicator_range','page','started',identity+'; page={page_number}; total_pages={expected_pages if expected_pages is not None else \'unknown\'}; received_rows={len(response_rows)}; persisted=false'))
edit.insert('payload = response.json','log_phase = "json"')
edit.insert('if not isinstance(payload, dict):','log_phase = "response_metadata"')
for node in ast.walk(edit.function):
    if isinstance(node,ast.Return) and isinstance(node.value,ast.List) and not node.value.elts:
        edit.insertions.setdefault(node.lineno-1,[]).append(textwrap.indent(event('query_eastmoney_indicator_range','fetch','completed',identity+'; pages_received={page_number}; rows=0; outcome=empty_response; normalized=false; persisted=false','api_success')+'\n',' '*node.col_offset))
edit.insert('response_rows.extend',event('query_eastmoney_indicator_range','page','completed',identity+'; page={page_number}/{expected_pages}; page_rows={len(data_rows)}; received_rows={len(response_rows)}/{expected_count}; normalized=false; persisted=false'),after=True)
edit.insert('if len(response_rows) != expected_count:','log_phase = "total_count"')
edit.insert('return response_rows',event('query_eastmoney_indicator_range','fetch','completed',identity+'; pages_received={page_number}; rows={len(response_rows)}; normalized=false; persisted=false','api_success'))
boundary(edit,'fetch',identity+'; normalized=false; persisted=false','log_request_page = 0',failure_fields='; indicator={source_indicator_id}; range={range_start}/{range_end}; page={log_request_page}; persisted=false')
# page_number 在原函数中初始化；额外日志变量使异常上下文不依赖初始化是否完成。
edit=FunctionEdit('a03-b04-query','query_eastmoney_indicator_range')
edit.insert('date_filter =','log_request_page = page_number')
edit.apply()

edit=FunctionEdit('a03-b04-normalize','normalize_external_index_response')
edit.insert('for item in response_rows:','log_phase = "source_rows"')
progress(edit,'if not isinstance(item, dict):','indicator={entity.source_indicator_id}; scanned_rows={log_scanned_rows}/{len(response_rows)}; accepted_rows={len(rows)}; persisted=false')
edit.insert('frame =','log_phase = "build_fact"')
edit.insert('return validate_external_index_frame','log_phase = "output_validation"')
finish_return(edit,'return validate_external_index_frame','normalized_index_df','normalize','indicator={entity.source_indicator_id}; range={range_start}/{range_end}; source_rows={len(response_rows)}; pending_grids={len(pending_dates)}; rows={len(normalized_index_df)}; normalized=true; persisted=false','api_result')
boundary(edit,'normalize','indicator={entity.source_indicator_id}; range={range_start}/{range_end}; source_rows={len(response_rows)}; pending_grids={len(pending_dates)}; persisted=false',scan_setup,'; indicator={entity.source_indicator_id}; scanned_rows={log_scanned_rows}; persisted=false')

edit=FunctionEdit('b03-c04-13','external_index_reconciliation')
edit.insert('calendar_rows =','log_phase = "calendar_conversion"')
edit.insert('fact_counts =','log_phase = "fact_counts"')
edit.insert('for row in calendar_rows:','log_phase = "classify_required"')
progress(edit,"source_indicator_id = row['entity_code']",'scanned_grids={log_scanned_rows}/{len(calendar_rows)}; complete_grids={complete_count}; pending_grids={len(pending_rows)}; persisted=false',1000)
edit.insert('fact_scope_mask =','log_phase = "obsolete_facts"')
edit.insert('return (pending_df, obsolete_df, complete_count)',event('external_index_reconciliation','plan','completed','complete_grid_count={complete_count}; pending_grid_count={len(pending_df)}; obsolete_fact_grid_count={len(obsolete_df)}; persisted=false','reconciliation_plan'))
boundary(edit,'plan','calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; start_date={start_date}; end_date={end_date}; persisted=false',scan_setup,'; scanned_grids={log_scanned_rows}; persisted=false')

edit=FunctionEdit('a03-b04-merge','full_fact_partition')
edit.insert('retained_df =','log_phase = "retain_untouched"')
edit.insert('complete_df = pd.concat','log_phase = "merge"')
edit.insert('return validate_external_index_frame','log_phase = "output_validation"')
finish_return(edit,'return validate_external_index_frame','merged_index_df','generate','partition={partition_key}; retained_rows={len(retained_df)}; incoming_rows={len(incoming_df)}; complete_rows={len(merged_index_df)}; persisted=false')
boundary(edit,'generate','partition={partition_key}; existing_rows={len(existing_df)}; incoming_rows={len(incoming_df)}; touched_grids={len(touched_grids)}; persisted=false',failure_fields='; partition={partition_key}; persisted=false')

for name in ('apply_calendar_completion','apply_calendar_failure'):
    edit=FunctionEdit('b03-c04-17',name)
    edit.insert('for index, row in updated_df.iterrows():','log_phase = "update_state"')
    loop=next(n for n in ast.walk(edit.function) if isinstance(n,ast.For))
    progress(edit,ast.unparse(loop.body[0]),'scanned_rows={log_scanned_rows}/{len(calendar_df)}; updated_grids={log_updated_grids}; persisted=false')
    edit.insert("updated_df.at[index, 'updated_at'] =",'log_updated_grids += 1',after=True)
    edit.insert('return validate_calendar_frame','log_phase = "output_validation"')
    finish_return(edit,'return validate_calendar_frame','updated_calendar_df','generate_state','updated_grids={log_updated_grids}; rows={len(updated_calendar_df)}; persisted=false')
    boundary(edit,'generate_state','rows={len(calendar_df)}; '+('planned_grids={len(grid_results)}' if name=='apply_calendar_completion' else 'planned_grids={len(failed_grids)}; fetch_status={fetch_status}; fetch_completed=false')+'; persisted=false',scan_setup+'\nlog_updated_grids = 0','; updated_grids={log_updated_grids}; persisted=false')

for cell_id,name in (('a03-b04-fact-commit','commit_complete_fact_partition'),('a03-b04-calendar-commit','commit_calendar_partitions')):
    edit=FunctionEdit(cell_id,name)
    calendar=name=='commit_calendar_partitions'
    scope='calendar_leaf' if calendar else 'fact_leaf'
    if calendar:
        edit.insert('return 0',event(name,'commit','completed','touched_grids=0; committed_partitions=0; outcome=no_work; persisted=false; date_watermark=none'))
        edit.insert('calendar_grid_keys =','log_phase = "locate_touched"')
        edit.insert('partition_mask =','log_partition = partition_key\nlog_phase = "select_leaf"')
    edit.insert('complete_df = validate','log_phase = "validate_leaf"')
    edit.insert('complete_table =','log_phase = "conversion"')
    edit.insert('staging_path.mkdir','log_phase = "staging_write"\n'+event(name,'staging','started',f'partition={{partition_key}}; rows={{len(complete_df)}}; scope={scope}; persisted=false'))
    edit.insert('staged_dataset =','log_phase = "staging_readback"')
    edit.insert('with StagedPathTransaction','log_phase = "install"\n'+event(name,'install','started',f'partition={{partition_key}}; scope={scope}; transaction_state=pending'))
    edit.insert('committed_dataset =','log_phase = "formal_readback"\n'+event(name,'formal_readback','started',f'partition={{partition_key}}; scope={scope}; transaction_state=pending'))
    # 原安装 try/finally 整体结束后才报告成功；回滚仍由共享模块自行报告。
    install_try=next(n for n in ast.walk(edit.function) if isinstance(n,ast.Try) and any(isinstance(c,ast.With) for c in n.body))
    success=('log_committed_partitions += 1\n' if calendar else '')+event(name,'commit_leaf','completed',
        'partition={partition_key}; rows={len(committed_df)}; '+('committed_partitions={log_committed_partitions}/{len(partition_keys)}; ' if calendar else 'calendar_state=not_updated; ')+f'scope={scope}; persisted=true','partition_committed')
    edit.insertions.setdefault(install_try.end_lineno,[]).append(textwrap.indent(success+'\n',' '*install_try.col_offset))
    if calendar:
        edit.insert('return len(touched_df)',event(name,'commit','completed','touched_grids={len(touched_df)}; committed_partitions={log_committed_partitions}; persisted=true')+'\n'+event(name,'calendar_state','completed','touched_grids={len(touched_df)}; completed_grids={int(touched_df[\'is_fetch_completed\'].sum())}; persisted=true; date_watermark=none'))
    boundary(edit,'commit',('touched_grids={len(touched_grids)}; scope=calendar_leaves' if calendar else 'partition={partition_key}; rows={len(frame)}; scope=fact_leaf')+'; persisted=false',
        'log_partition = None\nlog_committed_partitions = 0' if calendar else '',
        '; partition={log_partition}; committed_partitions={log_committed_partitions}' if calendar else '; partition={partition_key}')

edit=FunctionEdit('b03-c04-19','main')
for node in ast.walk(edit.function):
    if not isinstance(node,ast.Expr) or not isinstance(node.value,ast.Call) or ast.unparse(node.value.func)!='click.echo': continue
    printed=ast.unparse(node)
    if 'api_success:' in printed or 'reconciliation_plan:' in printed:
        edit.replace(printed,'')
    elif 'partition_committed:' in printed:
        edit.replace(printed,event('main','partition_batch','completed','partition={partition_key}; calendar_rows={calendar_rows}; grids={len(group_df)}; obsolete_removed={len(obsolete_grids)}; processed_grids={processed_grid_count}/{len(work_df)}'))
for node in ast.walk(edit.function):
    if not isinstance(node,ast.Assign) or not isinstance(node.value,ast.Call): continue
    if not any(isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='to_table' for n in ast.walk(node.value)): continue
    variable=ast.unparse(node.targets[0])
    edit.insert(ast.unparse(node),event('main','materialize','started',f'frame={variable}; materialized=false'))
    edit.insert(ast.unparse(node),event('main','materialize','completed',f'frame={variable}; rows={{len({variable})}}; materialized=true'),after=True)
edit.apply()

cells['b03-c04-08'].source+='\n\n读取函数自行报告目录发现、Dataset 打开、物理契约检查与失败阶段；`materialized=false` 表示未读取数据行。实际 `to_table()` 的物化起止及行数由其所在函数或 main 报告。'
for cell_id in ('a03-b04-calendar-validation-text','a03-b04-fact-validation-text'):
    cells[cell_id].source+='\n\n函数报告转换、主键、逐行业务校验及排序阶段。利用原有循环计数，每 10000 行检查一次 2 秒输出间隔，不为日志另做全表扫描。'
cells['a03-b04-read-fact-text'].source+='\n\n函数独立报告空目录、物化、验收结果与行数；空目录正常完成与读取异常明确区分。'
cells['b03-c04-10'].source+='\n\n函数自行报告精确待办日期数、生成段数和失败阶段；结果只是一份内存请求计划。'
cells['a03-b04-session-text'].source+='\n\n会话创建自行报告起止；`business_requests=0` 表示此处尚未发起请求。'
cells['a03-b04-query-text'].source+='\n\n分页函数自行报告范围和每页起止、冻结页数、页行数与累计条数。全部分页验收通过后才报告一次 `api_success`；两种合法空响应也各自报告完成，标记 `normalized=false; persisted=false`。页数仅统计逻辑分页，不声称等于适配器内部的 HTTP 尝试次数。'
cells['a03-b04-normalize-text'].source+='\n\n归一化独立报告来源条数、精确待办数、接受条数与失败位置；完成使用 `api_result`、`normalized=true; persisted=false`，不能当作事实已落盘。原有逐行循环每 10000 行检查一次 2 秒进度间隔。'
cells['b03-c04-12'].source+='\n\n对账函数报告已完成、API 待办和清退数量；利用原分类循环每 1000 个格点检查一次 2 秒进度间隔。短小的计数映射和单格点完整性判断不逐项刷日志。'
cells['a03-b04-merge-text'].source+='\n\n合并函数报告输入、保留及完整叶行数，成功仅表示 `persisted=false` 的内存结果。'
cells['b03-c04-16'].source+='\n\n两个生成函数自行报告计划数、原循环扫描进度和实际更新格点数；成功仍标记 `persisted=false`。不增加扫描，按每 10000 行及 2 秒节流。'
cells['a03-b04-fact-commit-text'].source+='\n\n提交函数自行报告 staging、安装和正式复读阶段。成功退出共享事务并完成当前清理后才发出 `partition_committed`；`calendar_state=not_updated` 表明此时日历尚未回写。失败恢复日志由共享模块负责，函数补充失败阶段并继续抛错。'
cells['a03-b04-calendar-commit-text'].source+='\n\n每个叶成功退出事务并完成当前清理后报告 `partition_committed` 和累计成功叶数；全部触达叶成功后才报告 `calendar_state`、触达数、已完成格点数与 `date_watermark=none`。失败状态写入成功可以是 `persisted=true; completed_grids=0`，不表示采集完成。'
cells['b03-c04-18'].source=cells['b03-c04-18'].source.replace('耗时为本次 main 累计值','函数日志耗时为本次调用耗时，main 日志为整批累计值').replace('API 成功只表示查询及归一化完成，标记 `persisted=false`。','查询函数的 `api_success` 与归一化函数的 `api_result` 分别报告，均标记 `persisted=false`。')
cells['b03-c04-18'].source+='\n\nmain 保留参数门禁、批次与分区推进、由自身执行的物化、计数核对和批末复验；规划、API 结果、生成、叶提交和日历状态日志由实际函数报告。main 的分区汇总用 `partition_batch`，不再重复充当提交日志。'
# 日志说明补在原图后，业务节点、边和布局保持原样。
for suffix, explanation in {
    '09':'函数内报告结构读取；数据物化由真正调用 to_table 的环节报告。',
    'query':'每页报告起止与累计条数；完整分页成功不代表归一化或持久化。',
    'normalize':'api_result 只表示归一化完成，persisted=false。',
    'fact-commit':'partition_committed 只在事务及当前清理结束后发出；日历尚未更新。',
    'calendar-commit':'逐叶提交日志与全部触达叶的 calendar_state 分开；失败状态落盘不等于采集完成。',
}.items():
    cells['a03-b04-flow-'+suffix].source+='\n\n'+explanation

for old_cell,new_cell in zip(before.cells,notebook.cells,strict=True):
    assert {k:v for k,v in old_cell.items() if k!='source'}=={k:v for k,v in new_cell.items() if k!='source'}
    if old_cell.cell_type=='code':
        comments=lambda source:[t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type==tokenize.COMMENT]
        assert comments(old_cell.source)==comments(new_cell.source),old_cell.id
        ast.parse(new_cell.source)
assert before.metadata==notebook.metadata
nbformat.validate(notebook)
snapshot=pathlib.Path(tempfile.mkdtemp(prefix='a03-b04-function-logs-before-'))
paths=[ROOT/'AGENTS.md',ROOT/'.env.template']
for directory in ('02_Futures_Lakehouse','03_Futures_Database','config'):
    paths.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and p.suffix in ('.py','.ipynb','.md'))
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},indent=2),encoding='utf8')
for relative in (RELATIVE,RELATIVE.with_suffix('.py')):
    destination=snapshot/relative
    destination.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(ROOT/relative,destination)
assert PATH.read_bytes()==original_bytes
PATH.write_text(nbformat.writes(notebook),encoding='utf8',newline='\n')
print(snapshot)
