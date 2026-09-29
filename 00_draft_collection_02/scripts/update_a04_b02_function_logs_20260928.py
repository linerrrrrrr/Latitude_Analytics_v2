"""a04/b02 第 5—6 项：函数自行报告进度；不改变业务、I/O 和恢复。"""
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
REL=pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb')
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


NAMED_RETURNS={'validated_calendar_df','validated_fact_df','empty_fact_df','shibor_client','merged_fact_df','updated_calendar_df'}


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
    ('validate_interest_calendar_table','CALENDAR_TABLE_NAME','frame','CALENDAR_PRIMARY_KEY'),
    ('validate_interest_rate_frame','TABLE_NAME','checked_df','PRIMARY_KEY'),
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

name='read_interest_calendar';edit=edit_function(name)
edit.insert('table = dataset.to_table','log_phase = "materialize"\n'+event(name,'materialize','started','dataset_name={DATASET_NAME}; persisted=false',table='CALENDAR_TABLE_NAME'))
edit.insert('return validate_interest_calendar_table',event(name,'materialize','completed','rows={table.num_rows}; persisted=false',table='CALENDAR_TABLE_NAME')+'\nlog_phase = "validate"')
result_return(edit,'return validate_interest_calendar_table','validated_calendar_df','read','rows={len(validated_calendar_df)}; materialized=true; persisted=false','CALENDAR_TABLE_NAME')
boundary(edit,'read','path={table_path}; dataset_name={DATASET_NAME}; persisted=false',table='CALENDAR_TABLE_NAME')

name='read_optional_fact';edit=edit_function(name)
edit.replace('return (empty_pandas','empty_fact_df = empty_pandas(INTEREST_RATE_DAILY_SCHEMA)\n'+event(name,'read','completed','rows=0; reason=no_parquet; metadata_exact=true; persisted=false')+'\nreturn empty_fact_df, True')
edit.insert('dataset, metadata_is_exact =','log_phase = "dataset_open"')
edit.insert('source_table = dataset.to_table','log_phase = "materialize"\n'+event(name,'materialize','started','persisted=false'))
edit.insert('current_table =',event(name,'materialize','completed','rows={source_table.num_rows}; persisted=false')+'\nlog_phase = "current_contract"')
edit.insert('current_df =','log_phase = "validate"')
edit.insert('return (current_df, metadata_is_exact)',event(name,'read','completed','rows={len(current_df)}; metadata_exact={str(metadata_is_exact).lower()}; materialized=true; persisted=false'))
boundary(edit,'read','path={table_path}; persisted=false')

name='plan_interest_rate_grids';edit=edit_function(name)
edit.insert('required_keys =','log_phase = "watermark"')
edit.insert('fact_counts =','log_phase = "fact_counts"')
edit.insert('for _, row in required_df.sort_values','log_phase = "compare"')
row_progress(edit,"key = (row['series_code'], row['report_date'])",'{len(required_df)}',phase='compare')
edit.insert('return (pending_df, repair_df, complete_count)',event(name,'plan','completed','required_grids={len(required_df)}; complete={complete_count}; state_repair={len(repair_df)}; api_pending={len(pending_df)}; persisted=false','reconciliation_plan'))
boundary(edit,'plan','calendar_rows={len(calendar_df)}; fact_rows={len(fact_df)}; start_date={start_date}; end_date={end_date}; persisted=false','log_processed_rows = 0\nlog_last_progress_at = log_started_at','; processed_rows={log_processed_rows}')

name='create_tushare_client';edit=edit_function(name)
result_return(edit,'return ts.pro_api','shibor_client','client','client_ready=true; source_request_sent=false; persisted=false')
boundary(edit,'client','source_request_sent=false; persisted=false')

name='query_shibor_window';edit=edit_function(name)
edit.insert('if raw_df is None:','log_phase = "response_type"')
edit.insert('return raw_df',event(name,'request','completed','start_date={start_date}; end_date={end_date}; response_rows={len(raw_df)}; normalized=false; persisted=false','api_result'))
boundary(edit,'request','start_date={start_date}; end_date={end_date}; automatic_retry=false; persisted=false',failure_fields='; start_date={start_date}; end_date={end_date}; automatic_retry=false')

name='normalize_shibor_response';edit=edit_function(name)
edit.insert('missing_columns =','log_phase = "columns"')
edit.insert('selected_df =','log_phase = "dates"')
edit.insert('for source_column in SOURCE_COLUMN_TO_SERIES:','log_phase = "source_values"')
edit.insert('if non_missing_mask.any():','log_processed_columns += 1\n'+event(name,'source_values','running','column={source_column}; checked_columns={log_processed_columns}/{len(SOURCE_COLUMN_TO_SERIES)}; source_rows={len(selected_df)}; persisted=false'),after=True)
edit.insert('rows_by_date =','log_phase = "source_index"')
edit.insert('for pending_row in pending_df.to_dict','log_phase = "exact_grids"')
row_progress(edit,"series_code = pending_row['series_code']",'{len(pending_df)}',phase='exact_grids')
edit.insert('fact_df = pd.DataFrame','log_phase = "output_validation"')
edit.insert('expected_keys =','log_phase = "coverage"')
edit.insert('return (fact_df, outcome_counts)',event(name,'normalize','completed','source_rows={len(raw_df)}; requested_grids={len(outcome_counts)}; fact_rows={len(fact_df)}; empty_grids={len(outcome_counts) - len(fact_df)}; normalized=true; persisted=false','api_result'))
boundary(edit,'normalize','source_rows={len(raw_df)}; pending_grids={len(pending_df)}; start_date={request_start_date}; end_date={request_end_date}; persisted=false','log_processed_columns = 0\nlog_processed_rows = 0\nlog_last_progress_at = log_started_at','; checked_columns={log_processed_columns}; processed_grids={log_processed_rows}')

name='full_fact_partition';edit=edit_function(name)
edit.insert('partition_df = existing_fact_df.loc','log_phase = "select_existing"')
edit.insert('for series_code, observation_date in touched_grids:','log_phase = "partition_scope"')
edit.insert('if not partition_df.empty:','log_phase = "replace_touched"')
edit.insert('merged_df =','log_phase = "merge"')
result_return(edit,'return empty_pandas','empty_fact_df','generate','partition={partition_key}; rows=0; touched_grids={len(touched_grids)}; persisted=false')
edit.insert('return validate_interest_rate_frame','log_phase = "validate_output"')
result_return(edit,'return validate_interest_rate_frame','merged_fact_df','generate','partition={partition_key}; rows={len(merged_fact_df)}; touched_grids={len(touched_grids)}; persisted=false')
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
        return_prefix='return validate_interest_calendar_table'
    else:
        fields='calendar_rows={len(calendar_df)}; input_grids={len(failed_grids)}; fetch_run_id={fetch_run_id}; result_status={failure_status}; persisted=false'
        completion_fields='rows={len(updated_calendar_df)}; changed_grids={log_updated_rows}; fetch_run_id={fetch_run_id}; result_status={failure_status}; is_fetch_completed=false; persisted=false'
        return_prefix='return validate_interest_calendar_table'
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
remove_phases={'read_calendar','read_fact','metadata_upgrade','plan','create_client','request','normalize','normalize_merge','merge_fact','fact_commit','calendar_commit','failure_state','final_readback','final_reconcile','repair_readback_plan'}
for node in ast.walk(edit.function):
    if not isinstance(node,ast.Expr) or not isinstance(node.value,ast.Call) or ast.unparse(node.value.func)!='click.echo':continue
    source=ast.unparse(node)
    if any('phase='+phase+';' in source for phase in remove_phases):
        edit.replacements[node.lineno-1]=(node.end_lineno,'')
edit.apply()
cells[definitions['main']].source=cells[definitions['main']].source.replace('remaining_api_pending={len(pending_df)}; persisted=true; message=state_repaired:', 'remaining_api_pending={len(pending_df)}; message=state_repaired:')

# 说明同步；已有流程图描述的业务次序和入口保持原样。
notes={
'a04-b02-doc-open-compatible-dataset':'函数自行报告文件发现、物理 fragment 检查及 metadata 检查，打开成功标记 materialized=false。已有片段循环每 100 个检查一次 2 秒进度间隔；不为日志新增片段遍历。',
'ab4baaf6':'校验函数报告输入、已通过行数和失败阶段；已有行循环每 10000 行检查一次 2 秒间隔。完成只表示内存日历通过校验，不代表日历已提交。',
'a04-b02-doc-validate-interest-rate-frame':'函数自行报告校验上下文、行数与失败阶段，利用已有循环计数；不新增业务检查或数据转换。',
'a04-b02-doc-read-interest-calendar':'读取函数区分 Dataset 打开、实际物化、校验和读取完成；本次调用耗时从函数进入时起算。',
'a04-b02-doc-read-optional-fact':'空事实也报告读取完成及无文件原因；非空路径报告物化行数、当前契约处理、校验和 metadata 匹配标志。',
'a04-b02-doc-plan-interest-rate-grids':'规划函数自行报告日期范围、输入量、已有循环的比较进度及完整/修复/API 待办数量；返回计划均为 persisted=false。',
'a04-b02-doc-create-tushare-client':'日志只表示客户端构造完成，不声称网络认证或来源请求成功；不输出 token。',
'a04-b02-doc-query-shibor-window':'函数自行报告请求日期、响应行数与失败阶段；请求成功标记 normalized=false、persisted=false，不提前声明转换或提交完成。',
'a04-b02-doc-normalize-shibor-response':'转换函数报告来源值检查进度、精确待办扫描、输出验收和 0/1 格点结果；只有转换与覆盖检查通过后才报告 normalized=true，仍为 persisted=false。',
'dd29e6da':'合并函数自行报告取旧叶、触达格点检查、合并与输出验收；返回的完整叶尚未落盘。',
'a04-b02-doc-write-fact-staging':'暂存函数自行报告零行标记、Arrow 转换和 Parquet 写入；staging 完成不等于正式提交。',
'a04-b02-doc-upgrade-fact-metadata':'迁移函数自行报告 staging 复读、安装、正式验收及恢复结果；现有清理完成后才报告 metadata_upgraded 与 persisted=true。',
'a04-b02-doc-commit-complete-fact-partition':'提交函数报告 staging、安装和正式复读。安装仍待验收；现有清理完成后才报告事实已提交，并明确本函数尚未提交日历。',
'49ae48fd':'状态生成函数报告输入和实际修改的格点数量，利用已有日历遍历表达进度；完成日志为 persisted=false，状态是否落盘由日历提交函数报告。',
'a04-b02-doc-apply-calendar-failure':'失败状态生成仍为 persisted=false、is_fetch_completed=false；后续提交成功只表示失败状态已保存，不表示采集完成。',
'a04-b02-doc-commit-calendar-partition':'日历提交成功事件只在正式复读和现有清理结束后发出：calendar_state=committed、persisted=true、date_watermark=none。它证明当前叶的状态已持久化，具体采集完成与否仍以逐格点字段为准；没有独立日期水位文件。恢复算法、范围和清理策略本轮不变。',
}
for cell_id,note in notes.items():
    cells[cell_id].source=cells[cell_id].source.rstrip()+'\n\n'+note+'\n'
paragraph='本轮统一 main 日志为事件前缀及 table/function/phase/status/elapsed_s，包含月份、格点与行数，首尾使用 80 个 `=`。阶段日志暂留在 main；第 5—6 项再将函数内细分进度与提交完成日志归位。生成和计划不报告持久化；正式提交函数成功返回后才报告对应表落盘，失败状态落盘明确标为未完成。日志不新增读表、摘要或业务校验。'
replacement='日志沿用事件前缀及 table/function/phase/status/elapsed_s，首尾使用 80 个 `=`。读取、请求、校验、转换、合并、状态生成及提交函数报告自己的进度与失败；main 保留参数、模式选择、失败分类、修复汇总、累计事实处理和批次结果，移除重复的调用起止及提交成功事件。函数耗时从各自进入时起算；生成与计划始终 persisted=false，持久化只由提交函数在正式验收和现有清理后报告。日志不新增读表、摘要或业务校验。'
assert paragraph in cells['48b4a6ce'].source
cells['48b4a6ce'].source=cells['48b4a6ce'].source.replace(paragraph,replacement)


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
    if '```mermaid' in old.source or old.id=='de9e1f3f':assert old.source==new.source
nbformat.validate(notebook)
snapshot=pathlib.Path(tempfile.mkdtemp(prefix='a04-b02-function-logs-before-'))
for relative in (REL,REL.with_suffix('.py')):
    saved=snapshot/relative;saved.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/relative,saved)
assert path.read_bytes()==original_bytes
path.write_text(nbformat.writes(notebook),encoding='utf8',newline='\n')
print(json.dumps({'snapshot':str(snapshot),'business_ast_unchanged':True},ensure_ascii=False))
