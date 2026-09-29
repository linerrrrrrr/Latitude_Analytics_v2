"""a04/b03 第 5—6 项：函数自行报告进度；不改变业务、I/O 和恢复。"""
import ast
import copy
import io
import json
import pathlib
import shutil
import tempfile
import textwrap
import tokenize

import nbformat

ROOT=pathlib.Path(__file__).resolve().parents[2]
REL=pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')
path=ROOT/REL
original_bytes=path.read_bytes()
notebook=nbformat.read(path,4)
before=copy.deepcopy(notebook)
cells={c.id:c for c in notebook.cells}
definitions={n.name:c.id for c in notebook.cells if c.cell_type=='code' for n in ast.parse(c.source).body if isinstance(n,ast.FunctionDef)}
editor_tree=ast.parse((ROOT/'00_draft_collection_02/scripts/update_a03_b01_function_logs_20260928.py').read_text(encoding='utf8'))
editor=next(n for n in editor_tree.body if isinstance(n,ast.ClassDef) and n.name=='FunctionEdit')
exec(compile(ast.Module(body=[editor],type_ignores=[]),'<draft FunctionEdit>','exec'))


def edit_function(name):
    return FunctionEdit(definitions[name],name)


def event(name,phase,status,fields='',prefix='planning_progress',table='TABLE_NAME'):
    return f'''click.echo(
    f"{prefix}: table={{{table}}}; function={name}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields else ''}elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
)'''


def boundary(edit,phase,fields='',setup='',failure_fields='',table='TABLE_NAME'):
    edit.wrap('log_started_at = time.perf_counter()\n'+f'log_phase = "{phase}"\n'+setup+'\n'+event(edit.function.name,phase,'started',fields,table=table),
        'except Exception as log_error:\n'+textwrap.indent(event(edit.function.name,'{log_phase}','failed','error={type(log_error).__name__}'+failure_fields,table=table)+'\nraise','    '))


NAMED_RETURNS={'validated_calendar_df','validated_fact_df','empty_fact_df','merged_fact_df','updated_calendar_df'}


def result_return(edit,prefix,name,phase,fields,table='TABLE_NAME'):
    node=edit.find(prefix)
    edit.replace(prefix,name+' = '+ast.unparse(node.value)+'\n'+event(edit.function.name,phase,'completed',fields,table=table)+'\nreturn '+name)


def row_progress(edit,prefix,total,counter='log_processed_rows',phase='rows',table='TABLE_NAME',after=False):
    edit.insert(prefix,counter+' += 1\n'+f'if {counter} % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'+textwrap.indent(event(edit.function.name,phase,'running',f'processed_rows={{{counter}}}/{total}; persisted=false',table=table)+'\nlog_last_progress_at = time.perf_counter()','    '),after=after)


name='open_compatible_dataset'; edit=edit_function(name)
edit.insert('parquet_files =','log_phase = "discovery"')
edit.insert('dataset = ds.dataset',event(name,'discovery','completed','label={label}; files={len(parquet_files)}; materialized=false',table='log_table_name')+'\nlog_phase = "dataset_open"')
edit.insert('if not physical_schema_matches(reconstructed_schema', 'log_phase = "schema"')
edit.insert('for fragment in dataset.get_fragments():','log_phase = "fragment_schema"')
edit.insert('if not physical_schema_matches(pa.schema',
    'log_checked_fragments += 1\nif log_checked_fragments % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'+textwrap.indent(event(name,'fragment_schema','running','label={label}; checked_fragments={log_checked_fragments}/{len(parquet_files)}; materialized=false',table='log_table_name')+'\nlog_last_progress_at = time.perf_counter()','    '),after=True)
edit.insert('is_exact =','log_phase = "metadata"\n'+event(name,'metadata','started','label={label}; materialized=false',table='log_table_name'))
edit.insert('return (dataset, is_exact)',event(name,'dataset_open','completed','label={label}; checked_fragments={log_checked_fragments}; metadata_exact={str(is_exact).lower()}; materialized=false',table='log_table_name'))
boundary(edit,'dataset_open','label={label}; path={table_path}; materialized=false','log_table_name = CALENDAR_TABLE_NAME if schema is MACRO_RELEASE_CALENDAR_SCHEMA else TABLE_NAME\nlog_checked_fragments = 0\nlog_last_progress_at = log_started_at','; label={label}; checked_fragments={log_checked_fragments}',table='log_table_name')

name='open_exact_dataset';edit=edit_function(name)
edit.insert('if not is_exact:','log_phase = "metadata"')
edit.insert('return dataset',event(name,'dataset_open','completed','label={label}; materialized=false',table='log_table_name'))
boundary(edit,'dataset_open','label={label}; path={table_path}; materialized=false','log_table_name = CALENDAR_TABLE_NAME if schema is MACRO_RELEASE_CALENDAR_SCHEMA else TABLE_NAME','; label={label}',table='log_table_name')

for name,table,frame,primary_key in (
    ('validate_macro_calendar_table','CALENDAR_TABLE_NAME','frame','CALENDAR_PRIMARY_KEY'),
    ('validate_macro_release_frame','TABLE_NAME','checked_df','PRIMARY_KEY'),
):
    edit=edit_function(name)
    edit.insert('if '+frame+'.duplicated','log_phase = "primary_key"')
    edit.insert('for row in checked.to_pylist():','log_phase = "business_validation"')
    # 行通过当前所有规则后计数，报告实际完成的行数。
    last_statement='if status not in ' if table=='CALENDAR_TABLE_NAME' else 'if row[\'updated_at\'] > now_utc:'
    row_progress(edit,last_statement,'{checked.num_rows}',phase='validate',table=table,after=True)
    result_name='validated_calendar_df' if table=='CALENDAR_TABLE_NAME' else 'validated_fact_df'
    result_return(edit,'return '+frame+'.sort_values',result_name,'validate','context={context}; rows={len('+result_name+')}; checked_rows={log_processed_rows}; persisted=false',table)
    boundary(edit,'validate','context={context}; rows={table.num_rows}; persisted=false' if table=='CALENDAR_TABLE_NAME' else 'context={context}; rows={len(frame)}; persisted=false','log_processed_rows = 0\nlog_last_progress_at = log_started_at','; context={context}; checked_rows={log_processed_rows}',table)

name='read_macro_calendar';edit=edit_function(name)
edit.insert('table = dataset.to_table','log_phase = "materialize"\n'+event(name,'materialize','started','dataset_name={DATASET_NAME}; persisted=false',table='CALENDAR_TABLE_NAME'))
edit.insert('return validate_macro_calendar_table',event(name,'materialize','completed','rows={table.num_rows}; persisted=false',table='CALENDAR_TABLE_NAME')+'\nlog_phase = "validate"')
result_return(edit,'return validate_macro_calendar_table','validated_calendar_df','read','rows={len(validated_calendar_df)}; materialized=true; persisted=false','CALENDAR_TABLE_NAME')
boundary(edit,'read','path={table_path}; dataset_name={DATASET_NAME}; persisted=false',table='CALENDAR_TABLE_NAME')

name='read_optional_fact';edit=edit_function(name)
edit.replace('return (empty_pandas','empty_fact_df = empty_pandas(MACRO_RELEASE_SCHEMA)\n'+event(name,'read','completed','rows=0; reason=no_parquet; metadata_exact=true; persisted=false')+'\nreturn empty_fact_df, True')
edit.insert('dataset, metadata_is_exact =','log_phase = "dataset_open"')
edit.insert('source_table = dataset.to_table','log_phase = "materialize"\n'+event(name,'materialize','started','persisted=false'))
edit.insert('current_table =',event(name,'materialize','completed','rows={source_table.num_rows}; persisted=false')+'\nlog_phase = "current_contract"')
edit.insert('current_df =','log_phase = "validate"')
edit.insert('return (current_df, metadata_is_exact)',event(name,'read','completed','rows={len(current_df)}; metadata_exact={str(metadata_is_exact).lower()}; materialized=true; persisted=false'))
boundary(edit,'read','path={table_path}; persisted=false')

name='plan_macro_release_grids';edit=edit_function(name)
edit.insert('required_by_key =','log_phase = "watermark"')
edit.insert('for fact_row in fact_df.itertuples','log_phase = "available_date"')
edit.insert('fact_counts =','log_phase = "fact_counts"')
edit.insert('for _, row in required_df.sort_values','log_phase = "compare"')
row_progress(edit,'if calendar_grid_is_complete','{len(required_df)}',phase='compare',after=True)
edit.insert('return (pending_df, repair_df, complete_count)',event(name,'plan','completed','required_grids={len(required_df)}; complete={complete_count}; state_repair={len(repair_df)}; api_pending={len(pending_df)}; persisted=false','reconciliation_plan'))
boundary(edit,'plan','calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; start_date={start_date}; end_date={end_date}; persisted=false','log_processed_rows = 0\nlog_last_progress_at = log_started_at','; processed_rows={log_processed_rows}')

name='create_eastmoney_session';edit=edit_function(name)
edit.insert('return session',event(name,'session','completed','session_ready=true; source_request_sent=false; http_retry_total=3; persisted=false'))
boundary(edit,'session','source_request_sent=false; persisted=false')

name='query_eastmoney_report_range';edit=edit_function(name)
edit.insert('fields =','log_phase = "report_fields"')
edit.insert('date_filter =','log_page_number = page_number\nlog_phase = "request_page"\n'+event(name,'request_page','started','report={report_name}; page={page_number}; expected_pages={expected_pages}; received_rows={len(response_rows)}; start_date={range_start}; end_date={range_end}; persisted=false','request_batch'))
edit.insert('payload = response.json()','log_phase = "response_json"')
edit.insert('if not isinstance(payload, dict):','log_phase = "response_structure"')
edit.insert('integer_metadata =','log_phase = "pagination_metadata"')
edit.insert('if expected_pages is None:','log_phase = "pagination_consistency"')
edit.insert('response_rows.extend(data_rows)','log_received_rows = len(response_rows)\n'+event(name,'request_page','completed','report={report_name}; page={page_number}/{expected_pages}; page_rows={len(data_rows)}; received_rows={len(response_rows)}/{expected_count}; normalized=false; persisted=false','api_result'),after=True)
for node in ast.walk(edit.function):
    if isinstance(node,ast.Return) and isinstance(node.value,ast.List) and not node.value.elts:
        edit.insertions.setdefault(node.lineno-1,[]).append(textwrap.indent(event(name,'request','completed','report={report_name}; page={page_number}; response_rows=0; empty_response=true; normalized=false; persisted=false','api_result')+'\n',' '*node.col_offset))
edit.insert('if len(response_rows) != expected_count:','log_phase = "total_count"')
edit.insert('return response_rows',event(name,'request','completed','report={report_name}; pages={expected_pages}; response_rows={len(response_rows)}; normalized=false; persisted=false','api_result'))
boundary(edit,'request','report={report_name}; start_date={range_start}; end_date={range_end}; http_retry_total=3; business_window_retry=false; persisted=false','log_page_number = 0\nlog_received_rows = 0','; report={report_name}; page={log_page_number}; accepted_rows={log_received_rows}; partial_result_returned=false')

name='normalize_macro_release_response';edit=edit_function(name)
edit.insert('configured_series =','log_phase = "report_mapping"')
edit.insert('for item in response_rows:','log_phase = "source_rows"')
row_progress(edit,'values_by_date[canonical_report_date] =','{len(response_rows)}',counter='log_source_rows',phase='source_rows',after=True)
edit.insert('fact_rows =',event(name,'source_rows','completed','report={report_name}; checked_rows={log_source_rows}; normalized=false; persisted=false'))
edit.insert('for pending_row in pending_df.to_dict','log_phase = "exact_grids"')
row_progress(edit,"series_code = pending_row['series_code']",'{len(pending_df)}',counter='log_grid_rows',phase='exact_grids')
edit.insert('fact_df = pd.DataFrame','log_phase = "output_validation"')
edit.insert('expected_keys =','log_phase = "coverage"')
edit.insert('return (fact_df, outcome_counts)',event(name,'normalize','completed','report={report_name}; source_rows={len(response_rows)}; requested_grids={len(outcome_counts)}; fact_rows={len(fact_df)}; empty_grids={len(outcome_counts) - len(fact_df)}; normalized=true; persisted=false','api_result'))
boundary(edit,'normalize','report={report_name}; source_rows={len(response_rows)}; pending_grids={len(pending_df)}; start_date={request_start_date}; end_date={request_end_date}; persisted=false','log_source_rows = 0\nlog_grid_rows = 0\nlog_last_progress_at = log_started_at','; report={report_name}; checked_source_rows={log_source_rows}; visited_grids={log_grid_rows}')

name='full_fact_partition';edit=edit_function(name)
edit.insert('partition_df = existing_fact_df.loc','log_phase = "select_existing"')
edit.insert('for series_code, report_date in touched_grids:','log_phase = "partition_scope"')
edit.insert('if not partition_df.empty:','log_phase = "replace_touched"')
edit.insert('merged_df =','log_phase = "merge"')
result_return(edit,'return empty_pandas','empty_fact_df','generate','partition={partition_key}; rows=0; touched_grids={len(touched_grids)}; persisted=false')
edit.insert('return validate_macro_release_frame','log_phase = "validate_output"')
result_return(edit,'return validate_macro_release_frame','merged_fact_df','generate','partition={partition_key}; rows={len(merged_fact_df)}; touched_grids={len(touched_grids)}; persisted=false')
boundary(edit,'generate','partition={partition_key}; existing_rows={len(existing_fact_df)}; incoming_rows={len(incoming_fact_df)}; touched_grids={len(touched_grids)}; persisted=false',failure_fields='; partition={partition_key}')

name='write_fact_staging';edit=edit_function(name)
edit.insert('pq.write_table','log_phase = "schema_marker"')
edit.insert('table = pandas_to_arrow','log_phase = "convert"')
edit.insert('if len(table):','log_phase = "write_parquet"')
edit.insert('if len(table):',event(name,'staging_write','completed','path={staging_path}; rows={table.num_rows}; persisted=false'),after=True)
boundary(edit,'staging_write','path={staging_path}; rows={len(frame)}; persisted=false')

for name in ('apply_calendar_completion','apply_calendar_failure'):
    edit=edit_function(name)
    if name.endswith('completion'):
        fields='calendar_rows={len(calendar_df)}; input_grids={len(grid_counts)}; fetch_run_id={fetch_run_id}; persisted=false'
        completion_fields='rows={len(updated_calendar_df)}; changed_grids={log_updated_rows}; fetch_run_id={fetch_run_id}; is_fetch_completed=true; persisted=false'
        return_prefix='return validate_macro_calendar_table'
    else:
        fields='calendar_rows={len(calendar_df)}; input_grids={len(failed_grids)}; fetch_run_id={fetch_run_id}; result_status={failure_status}; persisted=false'
        completion_fields='rows={len(updated_calendar_df)}; changed_grids={log_updated_rows}; fetch_run_id={fetch_run_id}; result_status={failure_status}; is_fetch_completed=false; persisted=false'
        return_prefix='return validate_macro_calendar_table'
    edit.insert('updated_df =','log_phase = "copy_calendar"')
    edit.insert('for index, row in updated_df.iterrows():','log_phase = "update_rows"')
    row_progress(edit,"key = (row['series_code'], row['report_date'])",'{len(updated_df)}',phase='update_rows',table='CALENDAR_TABLE_NAME')
    edit.insert("updated_df.at[index, 'updated_at'] =",'log_updated_rows += 1',after=True)
    edit.insert(return_prefix,'log_phase = "validate_output"')
    result_return(edit,return_prefix,'updated_calendar_df','generate_state',completion_fields,'CALENDAR_TABLE_NAME')
    boundary(edit,'generate_state',fields,'log_updated_rows = 0\nlog_processed_rows = 0\nlog_last_progress_at = log_started_at','; processed_rows={log_processed_rows}; changed_grids={log_updated_rows}',table='CALENDAR_TABLE_NAME')

# 三条提交路径保持各自原有恢复代码。成功事件位于现有 finally 清理之后。
for name in ('upgrade_fact_metadata','commit_complete_fact_partition','commit_calendar_partition'):
    edit=edit_function(name)
    is_calendar=name=='commit_calendar_partition'
    is_root=name=='upgrade_fact_metadata'
    table='CALENDAR_TABLE_NAME' if is_calendar else 'TABLE_NAME'
    input_name='calendar_df' if is_calendar else 'existing_fact_df' if is_root else 'frame'
    partition_fields='' if is_root else '; partition={partition_key}'
    common='run_id={run_id}'+partition_fields
    edit.insert('silver_root =','log_phase = "paths"')
    if is_calendar:
        edit.insert('partition_mask =','log_phase = "select_partition"')
        edit.insert('open_exact_dataset(','log_phase = "upstream_contract"')
        edit.insert('staging_path.mkdir','log_phase = "staging_write"\n'+event(name,'staging_write','started',common+'; rows={len(partition_df)}; persisted=false',table=table))
    else:
        edit.insert('write_fact_staging(','log_phase = "staging_write"')
    edit.insert('staged_dataset =', 'log_phase = "staging_readback"\n'+event(name,'staging_readback','started',common+'; persisted=false',table=table))
    edit.insert('if table_digest(staged_df,',event(name,'staging_readback','completed',common+'; rows={len(staged_df)}; persisted=false',table=table),after=True)
    edit.insert('cleanup_recovery_paths = True','log_phase = "install"\n'+event(name,'install','started',common+'; pending_acceptance=true',table=table),after=True)
    edit.insert('committed_dataset =',event(name,'install','completed',common+'; pending_acceptance=true',table=table)+'\nlog_phase = "formal_readback"\n'+event(name,'formal_readback','started',common+'; pending_acceptance=true',table=table))
    edit.insert('if table_digest(committed_df,',event(name,'formal_readback','completed',common+'; rows={len(committed_df)}; pending_cleanup=true',table=table),after=True)
    # 恢复不完整的现场保护标记先设置，再报告；日志不能阻断尚未执行的恢复。
    edit.insert('cleanup_recovery_paths = False',event(name,'rollback','failed',common+'; failed_phase={log_phase}; error={type(commit_error).__name__}; rollback_errors={len(rollback_errors)}; backup_dir={backup_path}; quarantine_dir={quarantine_path}',table=table),after=True)
    edit.insert('if rollback_errors:',event(name,'rollback','completed',common+'; failed_phase={log_phase}; error={type(commit_error).__name__}; rollback_errors=0; cleanup_pending=true',table=table),after=True)
    final_fields=common+'; rows={len(committed_df)}; persisted=true; date_watermark=none'
    if is_calendar:
        final_fields+='; calendar_state=committed; collection_completion=per_grid'
    elif is_root:
        final_fields+='; message=metadata_upgraded'
    else:
        final_fields+='; calendar_state=not_committed_by_this_function'
    edit.insert('return committed_df',event(name,'commit','completed',final_fields,'committed',table))
    boundary(edit,'commit','rows={len('+input_name+')}'+partition_fields+'; persisted=false',failure_fields=partition_fields,table=table)

# main 保留参数、模式选择、失败分类、修复汇总、自己执行的累计表处理及批次结果。
edit=edit_function('main')
remove_phases={'read_calendar','read_fact','metadata_upgrade','plan','create_session','request','normalize','normalize_merge','merge_fact','fact_commit','calendar_commit','completion_state','failure_state','final_readback','final_reconcile','repair_readback_plan'}
for node in ast.walk(edit.function):
    if not isinstance(node,ast.Expr) or not isinstance(node.value,ast.Call) or ast.unparse(node.value.func)!='click.echo':continue
    source=ast.unparse(node)
    if any('phase='+phase+';' in source for phase in remove_phases):
        edit.replacements[node.lineno-1]=(node.end_lineno,'')
edit.apply()
cells[definitions['main']].source=cells[definitions['main']].source.replace('remaining_api_pending={len(pending_df)}; persisted=true; message=state_repaired:', 'remaining_api_pending={len(pending_df)}; message=state_repaired:')

# 同步职责说明；业务流程与流程图保持原样。
notes={
'open_compatible_dataset':'函数报告文件发现、已有 fragment 循环及 metadata 阶段；每 100 个片段检查一次 2 秒进度间隔，不新增遍历。打开成功仍 materialized=false。',
'open_exact_dataset':'自行报告打开与契约结果，不把打开句柄称为读取记录完成。',
'validate_macro_calendar_table':'利用已有循环报告已通过行数，10000 行与 2 秒双阈值限制中途日志；完成只表示内存校验通过。',
'validate_macro_release_frame':'报告输入、已通过行数和失败阶段；复用现有校验，不增加规则或转换。',
'read_macro_calendar':'自行区分打开、物化、校验与读取完成，报告实际行数；函数独立计时。',
'read_optional_fact':'空事实也报告 rows=0 与 no_parquet；非空报告物化、契约处理和读取完成，不声称磁盘 metadata 已迁移。',
'plan_macro_release_grids':'报告水位检查、可用日核对、格点比较及完整/修复/API 待办数量。只生成计划，persisted=false。',
'create_eastmoney_session':'报告会话准备及现有 HTTP 重试策略；构造会话不表示请求成功。',
'query_eastmoney_report_range':'每页请求前报告页号及窗口；分页检查通过后报告 page、page_rows 和 received_rows。全部核对后才报告 request 完成；部分失败报告 accepted_rows，不返回部分结果。HTTP 重试策略不变。',
'normalize_macro_release_response':'报告来源验证、精确待办、输出验收和预期空格点数量；转换及覆盖通过才 normalized=true，始终 persisted=false。',
'full_fact_partition':'取旧叶、触达范围、合并与输出验收由本函数报告；完整叶生成不表示提交。',
'write_fact_staging':'报告零行标记、Arrow 转换和 Parquet 暂存；完成仍 persisted=false。',
'apply_calendar_completion':'利用已有循环记录扫描及实际修改数，状态生成仍 persisted=false，不报告水位落盘。',
'apply_calendar_failure':'生成日志明确 is_fetch_completed=false、persisted=false；保存失败状态不表示采集完成。',
'upgrade_fact_metadata':'报告 staging、安装、正式复读和恢复；原有 finally 清理结束后才报告 metadata_upgraded 与 persisted=true。恢复和清理策略不变。',
'commit_complete_fact_partition':'安装成功仍待验收；正式复读及原有清理结束后才报告 committed，并明确本函数没有提交日历。恢复范围不变。',
'commit_calendar_partition':'正式复读和清理结束后报告 calendar_state=committed、collection_completion=per_grid、date_watermark=none，只证明叶状态已保存；采集完成与否由格点字段决定。没有独立水位文件，恢复结果在实际恢复后报告。',
}
for name,note in notes.items():
    cells['a04-b03-doc-'+name].source=cells['a04-b03-doc-'+name].source.rstrip()+'\n\n'+note+'\n'
old_paragraph='本轮 main 日志统一为事件前缀及 `table/function/phase/status/elapsed_s`，报告年月、报告名、格点和行数，批次首尾为 80 个 `=`。请求完成、内存生成和正式落盘分别表达；日历失败状态落盘明确标为未完成。日志复用现有结果，不增加读取或校验。函数内逐页进度与日志归位属于后续第 5—6 项。'
new_paragraph='日志沿用事件前缀与 `table/function/phase/status/elapsed_s`，批次首尾为 80 个 `=`。读取、分页、校验、转换、合并、状态生成和提交函数自行报告进度及失败；main 保留参数和模式、失败分类、修复汇总、累计事实处理和批次结果，移除重复的调用起止及提交成功事件。函数各自计时；请求/生成不声明落盘，提交在正式验收与原有清理后报告持久化。日志不增加读表、摘要或业务检查。'
assert old_paragraph in cells['755be747'].source
cells['755be747'].source=cells['755be747'].source.replace(old_paragraph,new_paragraph)

class RemoveLogging(ast.NodeTransformer):
    def visit_Expr(self,node):
        if isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='click.echo':return None
        return self.generic_visit(node)
    def visit_Assign(self,node):
        if all(isinstance(n,ast.Name) and n.id.startswith('log_') for n in node.targets):return None
        return self.generic_visit(node)
    def visit_AugAssign(self,node):
        if isinstance(node.target,ast.Name) and node.target.id.startswith('log_'):return None
        return self.generic_visit(node)
    def visit_If(self,node):
        node=self.generic_visit(node)
        return node if node.body else None
    def visit_Try(self,node):
        if len(node.handlers)==1 and node.handlers[0].name=='log_error':
            assert isinstance(node.handlers[0].body[-1],ast.Raise) and node.handlers[0].body[-1].exc is None
            return self.generic_visit(node).body
        return self.generic_visit(node)


def canonical(source):
    tree=RemoveLogging().visit(ast.parse(source))
    for node in ast.walk(tree):
        if not hasattr(node,'body') or not isinstance(node.body,list):continue
        for index in range(len(node.body)-2,-1,-1):
            assignment,following=node.body[index:index+2]
            if not(isinstance(assignment,ast.Assign) and len(assignment.targets)==1 and isinstance(assignment.targets[0],ast.Name)
                and assignment.targets[0].id in NAMED_RETURNS and isinstance(following,ast.Return)):continue
            target_name=assignment.targets[0].id
            references=[n for n in ast.walk(following) if isinstance(n,ast.Name) and n.id==target_name]
            assert len(references)==1
            class InlineResult(ast.NodeTransformer):
                def visit_Name(self,value):
                    return assignment.value if value.id==target_name else value
            following.value=InlineResult().visit(following.value)
            del node.body[index]
    return ast.dump(tree)


code=lambda n:'\n\n'.join(c.source for c in n.cells if c.cell_type=='code')
assert canonical(code(before))==canonical(code(notebook)), '业务 AST 发生变化'
comments=lambda s:[t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type==tokenize.COMMENT]
assert comments(code(before))==comments(code(notebook))
assert before.metadata==notebook.metadata
for old,new in zip(before.cells,notebook.cells,strict=True):
    assert {k:v for k,v in old.items() if k!='source'}=={k:v for k,v in new.items() if k!='source'}
    if '```mermaid' in old.source or old.id=='14766fcd':assert old.source==new.source
nbformat.validate(notebook)
snapshot=pathlib.Path(tempfile.mkdtemp(prefix='a04-b03-function-logs-before-'))
for relative in (REL,REL.with_suffix('.py')):
    saved=snapshot/relative;saved.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/relative,saved)
assert path.read_bytes()==original_bytes
path.write_text(nbformat.writes(notebook),encoding='utf8',newline='\n')
print(json.dumps({'snapshot':str(snapshot),'business_ast_unchanged':True},ensure_ascii=False))
