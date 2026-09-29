"""b05 第 5—6 项：日志归属现有函数，不改变采集、校验和事务顺序。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b05_futures_daily.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


class FunctionEdit:
    def __init__(self, cell_id, name):
        self.cell = cells[cell_id]
        self.function = next(n for n in ast.parse(self.cell.source).body if isinstance(n, ast.FunctionDef) and n.name == name)
        self.lines = self.cell.source.splitlines(keepends=True)
        self.insertions = {}
        self.replacements = {}

    def find(self, prefix):
        matches = [n for n in ast.walk(self.function) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(prefix)]
        assert len(matches) == 1, (self.function.name, prefix, len(matches))
        return matches[0]

    def insert(self, prefix, source, *, after=False, indent=None):
        node = self.find(prefix)
        index = node.end_lineno if after else node.lineno - 1
        indentation = node.col_offset if indent is None else indent
        self.insertions.setdefault(index, []).append(textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * indentation))

    def replace(self, prefix, source):
        node = self.find(prefix)
        self.replacements[node.lineno - 1] = (node.end_lineno, textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * node.col_offset))

    def wrap(self, setup, failure):
        # 保留函数 docstring 及原有注释位置，仅在业务正文外增加记录后原样抛错的边界。
        first = self.function.body[0]
        start = first.end_lineno if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str) else self.function.lineno
        # 多行函数签名的正文起点不能用 def 所在行。
        if start == self.function.lineno:
            start = first.lineno - 1
            while start > self.function.lineno and (not self.lines[start - 1].strip() or self.lines[start - 1].lstrip().startswith('#')):
                start -= 1
        end = self.function.end_lineno
        body = []
        index = start
        while index < end:
            body.extend(self.insertions.get(index, []))
            if index in self.replacements:
                index, replacement = self.replacements[index]
                body.append(replacement)
            else:
                body.append(self.lines[index])
                index += 1
        body.extend(self.insertions.get(end, []))
        self.cell.source = (
            ''.join(self.lines[:start])
            + textwrap.indent(textwrap.dedent(setup).strip() + '\n', '    ')
            + '    try:\n'
            + textwrap.indent(''.join(body).rstrip() + '\n', '    ')
            + textwrap.indent(textwrap.dedent(failure).strip() + '\n', '    ')
            + ''.join(self.lines[end:])
        )
        ast.parse(self.cell.source)


def boundary(edit, phase, *, table='TABLE_NAME', fields='', setup='', initial_phase=None):
    """仅供本次编辑使用；生成直接写在各业务函数里的日志，不形成生产依赖。"""
    edit.wrap(f'''
        log_started_at = time.perf_counter()
        log_phase = "{initial_phase or phase}"
        {setup}
        click.echo(
            f"planning_progress: table={{{table}}}; function={edit.function.name}; phase={phase}; status=started; "
            f"{fields}"
        )
    ''', f'''
        except Exception as log_error:
            click.echo(
                f"planning_progress: table={{{table}}}; function={edit.function.name}; phase={phase}; status=failed; "
                f"failed_phase={{log_phase}}; error={{type(log_error).__name__}}; "
                f"elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
            )
            raise
    ''')


edit = FunctionEdit('f1d01294', 'open_contract_dataset')
edit.insert('return None', '''
    click.echo(
        f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=dataset_open; status=completed; "
        f"outcome=absent_optional; checked_fragments=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.insert('dataset = ds.dataset', 'log_phase = "dataset_open"')
edit.insert('actual_schema = reconstructed_schema', 'log_phase = "logical_schema"')
edit.insert('for fragment in dataset.get_fragments():', '''
    log_phase = "fragment_schema"
    log_fragment_count = 0
    click.echo(
        f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=fragment_schema; status=started"
    )
''')
edit.insert('if not physically_and_identity_compatible(fragment.physical_schema', '''
    log_fragment_count += 1
    if log_fragment_count == 1 or log_fragment_count % 250 == 0:
        click.echo(
            f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=fragment_schema; status=running; "
            f"checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
''', after=True)
edit.insert('return dataset', '''
    click.echo(
        f"planning_progress: table={table_path.name}; function=open_contract_dataset; phase=dataset_open; status=completed; "
        f"outcome=ready; checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
boundary(edit, 'dataset_open', table='table_path.name', fields='path={table_path}; required={str(required).lower()}', initial_phase='discovery')

edit = FunctionEdit('c05-daily-policy', 'daily_policy_plan')
edit.insert('desired_required = pa.array', 'log_phase = "whitelist"')
edit.insert('confirmed_closed = pc.equal', 'log_phase = "completion_evidence"')
edit.insert('return (planned_table, dirty_mask, pending_mask, completed_mask)', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=daily_policy_plan; phase=policy_plan; status=completed; "
        f"rows={len(planned_table)}; completion_source=is_fetch_completed; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
boundary(edit, 'policy_plan', fields='rows={len(planning_table)}; completion_source=is_fetch_completed', initial_phase='input')

edit = FunctionEdit('bc59b121', 'request_batches')
edit.insert('contract_codes = sorted', '''
    log_phase = "group_batches"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=running; "
        f"exchange={exchange_code}; year={year}; pending={len(group_df)}; batches={len(batches)}"
    )
''')
edit.insert('current_codes: list[str] = []', 'log_contract_count = 0', after=True)
edit.insert('current_codes.append(contract_code)', '''
    log_contract_count += 1
    if log_contract_count == 1 or log_contract_count % 100 == 0 or log_contract_count == len(contract_codes):
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=running; "
            f"exchange={exchange_code}; year={year}; contracts={log_contract_count}/{len(contract_codes)}; "
            f"finalized_batches={len(batches)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
''', after=True)
edit.insert('return batches', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=request_batches; phase=request_plan; status=completed; "
        f"pending={len(pending_df)}; batches={len(batches)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
boundary(edit, 'request_plan', fields='pending={len(pending_df)}')

edit = FunctionEdit('b205c6aa', 'read_partition_leaf')
edit.insert('return empty_pandas', '''
    click.echo(
        f"planning_progress: table={table_path.name}; function=read_partition_leaf; phase=read_leaf; status=completed; "
        f"partition={partition_key}; outcome=absent_leaf; rows=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.insert('leaf_dataset = ds.dataset', 'log_phase = "leaf_schema"')
edit.insert('leaf_table = leaf_dataset.to_table', '''
    log_phase = "leaf_scan"
    click.echo(
        f"planning_progress: table={table_path.name}; function=read_partition_leaf; phase=leaf_scan; status=started; "
        f"partition={partition_key}; columns={len(schema.names)}"
    )
''')
edit.replace('return arrow_to_pandas', '''
    log_phase = "schema_conversion"
    leaf_df = arrow_to_pandas(leaf_table, schema)
    click.echo(
        f"planning_progress: table={table_path.name}; function=read_partition_leaf; phase=read_leaf; status=completed; "
        f"partition={partition_key}; outcome=read; rows={len(leaf_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    return leaf_df
''')
boundary(edit, 'read_leaf', table='table_path.name', fields='partition={partition_key}; path={table_path}', initial_phase='leaf_discovery')

edit = FunctionEdit('d180c167', 'collect_batch')
for assignment, api, number, response in [
    ('raw_price_df = jqdata.get_price', 'get_price', 1, 'raw_price_df'),
    ('raw_settlement_df = jqdata.get_extras', 'futures_sett_price', 2, 'raw_settlement_df'),
    ('raw_position_df = jqdata.get_extras', 'futures_positions', 3, 'raw_position_df'),
]:
    edit.insert(assignment, f'''
        log_phase = "{api}"
        click.echo(
            f"fetch_progress: table={{TABLE_NAME}}; function=collect_batch; phase=api_request; status=started; "
            f"api={api}; completed={number - 1}; total=3; contracts={{len(contract_codes)}}; "
            f"elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
        )
    ''')
    edit.insert(f'if {response} is None:', f'''
        click.echo(
            f"fetch_progress: table={{TABLE_NAME}}; function=collect_batch; phase=api_request; status=completed; "
            f"api={api}; completed={number}; total=3; elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
        )
    ''', after=True)
for prefix, phase in [
    ('price_df = normalize_price_response', 'normalize_responses'),
    ('for response_name, response_df in', 'response_dates'),
    ('extras_df = settlement_df.merge', 'derive_values'),
    ('daily_df = pending_df.loc', 'align_pending'),
    ('checked_df = validate_daily_frame', 'validate_output'),
]:
    edit.insert(prefix, f'''
        log_phase = "{phase}"
        click.echo(
            f"planning_progress: table={{TABLE_NAME}}; function=collect_batch; phase={phase}; status=started; "
            f"pending={{len(pending_df)}}; elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
        )
    ''')
edit.insert('return (checked_df, returned_value_count)', '''
    click.echo(
        f"api_success: table={TABLE_NAME}; function=collect_batch; phase=collect; status=completed; "
        f"requests=3; fact_rows={len(checked_df)}; returned_values={returned_value_count}; "
        f"contracts={len(contract_codes)}; pending={len(pending_df)}; start={request_start}; end={request_end}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
boundary(edit, 'collect', fields="contracts={len(batch['contract_codes'])}; pending={len(batch['pending_df'])}; start={batch['request_start']}; end={batch['request_end']}", initial_phase='batch_scope')

edit = FunctionEdit('b5abb5b2', 'prepare_fact_leaf_specs')
edit.insert('return []', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=build_fact_leaves; status=completed; "
        f"outcome=no_new_facts; leaves=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.insert('existing_df = read_partition_leaf', 'log_phase = "read_existing_leaf"')
edit.insert('incoming_keys = pd.MultiIndex.from_frame', '''
    log_phase = "merge_leaf"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=merge_leaf; status=started; "
        f"partition={partition_key}; existing_rows={len(existing_df)}; new_rows={len(incoming_df)}"
    )
''')
edit.insert('desired_df = validate_daily_frame', 'log_phase = "validate_complete_leaf"')
edit.insert('specs.append(', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=build_fact_leaves; status=running; "
        f"partition={partition_key}; prepared_leaves={len(specs)}; rows={len(desired_df)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''', after=True)
edit.insert('return specs', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=prepare_fact_leaf_specs; phase=build_fact_leaves; status=completed; "
        f"leaves={len(specs)}; new_rows={len(collected_df)}; persisted=false; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
boundary(edit, 'build_fact_leaves', fields='new_rows={len(collected_df)}; persisted=false')

edit = FunctionEdit('f35dba7f', 'apply_completion_to_calendar_leaf')
edit.insert('return calendar_df.copy()', '''
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_completion_to_calendar_leaf; phase=completion_state; "
        f"status=completed; outcome=no_new_facts; updated_rows=0; persisted=false; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.insert('calendar_index = pd.MultiIndex.from_frame', 'log_phase = "match_calendar_keys"')
edit.insert('status_values = fact_status_df.loc', 'log_phase = "apply_completion"')
edit.insert('return desired_df', '''
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=apply_completion_to_calendar_leaf; phase=completion_state; "
        f"status=completed; outcome=prepared; updated_rows={len(matched_rows)}; persisted=false; "
        f"fetch_run_id={fetch_run_id}; actual_bars={int(has_market_data.sum())}; "
        f"confirmed_empty={int((~has_market_data).sum())}; invalid_ohlc_rows={int(invalid_mask.sum())}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
boundary(edit, 'completion_state', table='CALENDAR_TABLE_NAME', fields='fact_rows={len(fact_df)}; calendar_rows={len(calendar_df)}; persisted=false', initial_phase='derive_completion')

edit = FunctionEdit('f35dba7f', 'prepare_calendar_leaf_spec')
for prefix, phase in [
    ('desired_df = apply_policy_to_calendar_leaf', 'apply_policy'),
    ('desired_df = apply_completion_to_calendar_leaf', 'apply_completion'),
    ('validated_df = validate_calendar_state_frame', 'validate_complete_leaf'),
]:
    edit.insert(prefix, f'''
        log_phase = "{phase}"
        click.echo(
            f"planning_progress: table={{CALENDAR_TABLE_NAME}}; function=prepare_calendar_leaf_spec; phase={phase}; status=started; "
            f"partition={{partition_key}}"
        )
    ''')
edit.insert('return {', '''
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=prepare_calendar_leaf_spec; phase=build_calendar_leaf; status=completed; "
        f"partition={partition_key}; rows={len(validated_df)}; policy_rows={len(policy_df)}; fact_rows={len(fact_df)}; "
        f"persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
boundary(edit, 'build_calendar_leaf', table='CALENDAR_TABLE_NAME', fields='partition={partition_key}; policy_rows={len(policy_df)}; fact_rows={len(fact_df)}; persisted=false', initial_phase='read_existing_leaf')

edit = FunctionEdit('b43616a1', 'commit_validated_leaf_group')
edit.insert('return 0', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=commit_group; status=completed; "
        f"outcome=no_specs; leaves=0; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.insert('silver_root.mkdir', '''
    log_phase = "schema_marker"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=schema_marker; status=started; "
        f"run_id={run_id}; leaves={len(prepared_specs)}"
    )
''')
edit.insert('staging_root.mkdir', '''
    log_phase = "staging_write"
    log_written_leaves = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=staging_write; status=started; "
        f"run_id={run_id}; leaves={len(prepared_specs)}"
    )
''')
edit.insert('ds.write_dataset(', '''
    log_written_leaves += 1
    click.echo(
        f"planning_progress: table={spec['table_name']}; function=commit_validated_leaf_group; phase=staging_write; status=running; "
        f"partition={spec['partition_key']}; completed={log_written_leaves}; total={len(prepared_specs)}; "
        f"run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''', after=True)
edit.insert('staging_leaf_path = staging_table_path', '''
    log_phase = "staging_readback"
    click.echo(
        f"planning_progress: table={spec['table_name']}; function=commit_validated_leaf_group; phase=staging_readback; status=started; "
        f"partition={spec['partition_key']}; run_id={run_id}"
    )
''')
edit.insert('backup_root.mkdir', '''
    log_phase = "prepare_install"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=install; status=started; "
        f"run_id={run_id}; leaves={len(prepared_specs)}"
    )
''')
edit.insert('destination_path = target_table_path', '''
    log_phase = "install"
    click.echo(
        f"planning_progress: table={table_name}; function=commit_validated_leaf_group; phase=install; status=running; "
        f"partition={spec['partition_key']}; installed={log_installed_leaves}; total={len(prepared_specs)}; run_id={run_id}"
    )
''')
edit.insert('if not marker_path.exists():', 'log_installed_leaves += 1', after=True)
edit.insert('for spec, destination_path, _ in moved_specs:', '''
    log_phase = "formal_readback"
    log_checked_leaves = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=formal_readback; status=started; "
        f"run_id={run_id}; leaves={len(moved_specs)}"
    )
''')
edit.insert('if formal_key_table.to_pandas().duplicated', '''
    log_checked_leaves += 1
    click.echo(
        f"planning_progress: table={spec['table_name']}; function=commit_validated_leaf_group; phase=formal_readback; status=running; "
        f"partition={spec['partition_key']}; checked={log_checked_leaves}; total={len(moved_specs)}; run_id={run_id}"
    )
''', after=True)
edit.insert('rollback_errors = []', '''
    log_rollback_status = "started"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=rollback; status=started; "
        f"failed_phase={log_phase}; run_id={run_id}; moved_leaves={len(moved_specs)}"
    )
''')
edit.insert('if rollback_errors:', '''
    log_rollback_status = "failed" if rollback_errors else "completed"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=rollback; status={log_rollback_status}; "
        f"run_id={run_id}; errors={len(rollback_errors)}; backup={backup_root}; quarantine={quarantine_root}"
    )
''')
edit.insert('return sum(', '''
    for log_spec in prepared_specs:
        if log_spec["table_name"] == CALENDAR_TABLE_NAME:
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_validated_leaf_group; phase=completion_state; "
                f"status=completed; outcome=committed; persisted=true; partition={log_spec['partition_key']}; "
                f"calendar_leaf_rows={len(log_spec['complete_table'])}; source=is_fetch_completed; run_id={run_id}"
            )
    click.echo(
        f"partition_committed: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=commit_group; status=completed; "
        f"leaves={len(prepared_specs)}; run_id={run_id}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.wrap('''
    log_started_at = time.perf_counter()
    log_phase = "prepare_specs"
    log_rollback_status = "not_required"
    log_installed_leaves = 0
    click.echo(
        f"partition_start: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=commit_group; status=started; "
        f"leaves={len(partition_specs)}; lake_root={lake_root}"
    )
''', '''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_validated_leaf_group; phase=commit_group; status=failed; "
            f"failed_phase={log_phase}; rollback={log_rollback_status}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
''')

# main 保留真正由入口掌握的模式、配额和批次累计；移除替下层函数记录的单次起止。
source = cells['71d11aec'].source


def replace_once(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new, 1)


replace_once('            log_batch_started_at = time.perf_counter()\n', '')
replace_once('f"table={TABLE_NAME}; function=main; phase=request_batch; status=started"',
             'f"table={TABLE_NAME}; function=main; phase=collect_batch; status=running; event=dispatch"')
replace_once('''                f"api_success: table={TABLE_NAME}; function=main; phase=request_batch; status=completed; "''',
             '''                f"planning_progress: table={TABLE_NAME}; function=main; phase=collect_batch; status=running; "''')
replace_once('f"pending_total={len(pending_df)}; elapsed_s={time.perf_counter() - log_batch_started_at:.3f}"',
             'f"pending_total={len(pending_df)}; elapsed_s={time.perf_counter() - log_collect_started_at:.3f}"')
replace_once('''        log_leaf_started_at = time.perf_counter()
        click.echo(
            f"partition_start: table={TABLE_NAME}; function=main; phase=commit_group; status=started; "
            f"base_partition={base_key}; group={committed_leaf_groups + 1}/{len(dirty_calendar_keys)}"
        )
''', '')
replace_once('''        click.echo(
            "leaf_group_committed: "
            f"calendar_partition={calendar_partition_key}; "
            f"fact_leaves={len(fact_specs)}; "
            f"fact_rows={len(leaf_fact_df)}; "
            f"policy_rows={len(leaf_policy_df)}; "
            f"table={TABLE_NAME}; function=main; phase=commit_group; status=completed; "
            f"group={committed_leaf_groups}/{len(dirty_calendar_keys)}; "
            f"elapsed_s={time.perf_counter() - log_leaf_started_at:.3f}"
        )

''', '')
replace_once('''f"completed_groups={committed_leaf_groups}; total_groups={len(dirty_calendar_keys)}; "''',
             '''f"completed_groups={committed_leaf_groups}; total_groups={len(dirty_calendar_keys)}; "
            f"calendar_partition={calendar_partition_key}; fact_leaves={len(fact_specs)}; "
            f"new_fact_rows={len(leaf_fact_df)}; policy_rows={len(leaf_policy_df)}; "''')
# collect 在入口代表整批调度；单批采集的开始、阶段、完成、失败由 collect_batch 负责。
source = source.replace('function=main; phase=collect; ', 'function=main; phase=collect_batch; ')
cells['71d11aec'].source = source

cells['b86cefb4'].source += '\n\n读取日志由函数自行记录：根 Dataset 沿原有 fragment 遍历报告首个及每 250 个文件的检查进度，最后报告总数；不为日志额外列目录或扫描数据。异常记录失败阶段后原样抛出，可选下游不存在与必需上游缺失分别表达。'
cells['c05-daily-policy-markdown'].source += '\n\n`daily_policy_plan()` 自行报告政策规划起止及失败阶段，并明确完成依据为 `is_fetch_completed`；`request_batches()` 报告交易所—年份分组及每 100 个合约的组批进度。入口保留 dirty、pending、completed 和批次数量汇总，复用已有计数，不重复求差。'
cells['fbc92c70'].source = cells['fbc92c70'].source.replace('进度日志目前由入口在调用前后报告。',
    '`collect_batch()` 自行报告三次 API 的 0/3—3/3 进度、归一化、日期门禁、派生、待办对齐和输出校验，最后报告事实行数及耗时。`api_request` 完成仅表示接口已返回非 None，整批 `api_success` 只在输出校验通过后记录；异常记录实际阶段后原样抛出。入口只记录批次调度及累计完成量。')
cells['2a22008f'].source += '\n\n`read_partition_leaf()` 自行报告目标分区、完整叶扫描、读取行数和耗时。缺叶、成功读到零行与读取异常分别表达；日志不引入额外叶读取。'
cells['d7734812'].source += '\n\n生成日志属于 `prepare_fact_leaf_specs()`：逐叶报告读取、合并、业务校验后的准备进度及总耗时，空输入明确报告零叶。生成完成只表示内存规格已准备，`persisted=false`。'
cells['43d2aa31'].source += '\n\n完成状态日志分为两层：`apply_completion_to_calendar_leaf()` 只报告内存状态已生成，带 `persisted=false`；`prepare_calendar_leaf_spec()` 报告完整叶准备及校验。只有 `commit_validated_leaf_group()` 完成本组所有正式复读和原有清理后，才报告该日历叶 `persisted=true`。这里的完成凭证不是最大日期水位，也不把零行 Schema 标记称为水位。'
cells['720c2940'].source += '\n\n提交日志由 `commit_validated_leaf_group()` 自行记录：规格与标记门禁、staging 写入及复读、逐叶安装、正式复读、失败恢复和最终结果。恢复日志表达原有恢复路径的实际结果，异常仍保持原类型及异常链；正式完成凭证日志位于整个组成功之后，不在某个叶刚安装时提前发布。入口仅保留整批累计提交量。'
cells['c2d11e3b'].source = cells['c2d11e3b'].source.replace(
    '本轮日志仍在入口，函数内部进度归位留待后续。',
    'Dataset 与叶读取、政策及请求计划生成、单批采集、完整叶生成和协调提交日志均由对应函数负责；`main()` 保留整批规划、请求调度、配额停止、累计进度和运行结果，不重复报告单组提交起止。')

replacements = {
    'b05-flow-dataset': [('打开 Hive Dataset', '记录打开开始；打开 Hive Dataset'), ('检查逻辑与 fragment 物理契约', '检查契约；沿原遍历报告文件进度'), ('返回 Dataset', '记录完成并返回 Dataset')],
    'b05-flow-policy': [('日线规划窄表', '记录规划开始；日线规划窄表'), ('required 且已完成 → completed', 'required 且已完成 → completed；记录规划完成')],
    'b05-flow-batches': [('依次尝试加入合约；估算含回看期的返回量', '逐合约估算含回看期返回量；报告组批进度'), ('返回批次列表', '记录完成与批次数；返回计划')],
    'b05-flow-collect': [('一次日线、两次扩展字段 API', '记录采集开始；三次 API 各报告进度'), ('响应归一化并检查请求日期范围', '报告归一化及请求日期门禁'), ('返回事实与返回量计数', '校验通过后记录 api_success；返回结果')],
    'b05-flow-leaf-read': [('分区列与键 → Hive 叶路径', '记录读取开始；构造 Hive 叶路径'), ('返回契约化空表', '记录缺叶；返回契约化空表'), ('读取完整叶并转换', '读取并转换；报告行数及耗时')],
    'b05-flow-fact-leaves': [('按事实分区拆分本批结果', '记录生成开始；按事实分区处理'), ('返回事实叶规格', '报告已准备叶数；返回内存规格')],
    'b05-flow-calendar-leaf': [('按事实生成完成、缺失与质量状态', '生成完成状态；记录 persisted=false'), ('返回日历叶规格；等待与事实共同提交', '报告准备完成；等待与事实共同提交')],
    'b05-flow-commit': [('已校验完整叶规格', '记录提交开始；已校验叶规格'), ('清理 staging 与备份；返回行数', '清理后报告组完成及状态已落盘'), ('保留隔离证据及未恢复备份；抛错', '记录恢复结果；保留现场并抛错')],
    'b05-flow-main': [('逐批配额检查与采集；记录批次进度', '逐批配额检查；调度采集并报告累计量'), ('逐组准备、提交；报告组数与耗时', '调用生成与提交函数；报告整批累计量')],
}
for cell_id, pairs in replacements.items():
    for old, new in pairs:
        assert old in cells[cell_id].source, (cell_id, old)
        cells[cell_id].source = cells[cell_id].source.replace(old, new)

assert notebook.metadata == before.metadata
assert [c.id for c in notebook.cells] == [c.id for c in before.cells]
for old in before.cells:
    new = cells[old.id]
    assert old.metadata == new.metadata
    if old.cell_type == 'code':
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        assert comments(old.source) == comments(new.source), old.id
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
        compile(new.source, str(PATH), 'exec')
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print('b05 function-owned read/plan/collect/build/commit/completion logs updated; original comments and execution state retained.')
