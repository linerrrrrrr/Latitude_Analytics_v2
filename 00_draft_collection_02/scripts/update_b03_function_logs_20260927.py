"""本轮 b03 函数日志归位；只编辑 Notebook，不运行采集。"""

import ast
import copy
import hashlib
import io
import json
import pathlib
import tempfile
import textwrap
import tokenize


ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / "02_Futures_Lakehouse/a01_Futures_Market_Data/b03_futures_contract_calendar.ipynb"
before_bytes = PATH.read_bytes()
notebook = json.loads(before_bytes)
before = copy.deepcopy(notebook)
by_id = {cell["id"]: cell for cell in notebook["cells"]}


class FunctionEdit:
    def __init__(self, cell_id, function_name):
        self.cell = by_id[cell_id]
        self.source = "".join(self.cell["source"])
        self.function = next(n for n in ast.parse(self.source).body if isinstance(n, ast.FunctionDef) and n.name == function_name)
        self.lines = self.source.splitlines(keepends=True)
        self.insertions = {}

    def find(self, expression):
        matches = [n for n in ast.walk(self.function) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(expression)]
        assert len(matches) == 1, (expression, len(matches))
        return matches[0]

    def insert(self, node, text, *, after=False, indent=None):
        line = node.end_lineno if after else node.lineno - 1
        indent = node.col_offset if indent is None else indent
        self.insertions.setdefault(line, []).append(textwrap.indent(textwrap.dedent(text).strip() + "\n", " " * indent))

    def wrap(self, setup, failure):
        body_start = self.function.body[0].lineno - 1
        body_end = self.function.end_lineno
        body = "".join(
            "".join(self.insertions.get(index, [])) + self.lines[index]
            for index in range(body_start, body_end)
        ) + "".join(self.insertions.get(body_end, []))
        rewritten = (
            "".join(self.lines[:body_start])
            + textwrap.indent(textwrap.dedent(setup).strip() + "\n", "    ")
            + "    try:\n"
            + textwrap.indent(body.rstrip("\n") + "\n", "    ")
            + textwrap.indent(textwrap.dedent(failure).strip() + "\n", "    ")
            + "".join(self.lines[body_end:])
        )
        ast.parse(rewritten)
        self.cell["source"] = rewritten.splitlines(keepends=True)


e = FunctionEdit("c03-source", "collect_source_data")
e.insert(e.find("return (empty_contract_catalog_df"), '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=collect; status=completed; "
        f"outcome=empty_request; contracts=0; api_calls=0; "
        f"elapsed_s={perf_counter() - collection_started_at:.3f}"
    )
''')
e.insert(e.find("from config.jqdata_connection import"), '''
    collection_phase = "authenticate"
    phase_started_at = perf_counter()
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=authenticate; status=started")
''')
e.insert(e.find("jqdata = authenticate_jqdata"), '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=authenticate; status=completed; "
        f"elapsed_s={perf_counter() - phase_started_at:.3f}"
    )
    collection_phase = "contract_catalog"
    phase_started_at = perf_counter()
    click.echo(f"request_batch: table={TABLE_NAME}; phase=contract_catalog; status=started; date=None")
''', after=True)
e.insert(e.find("contract_catalog_df = contract_catalog_df.rename_axis"), '''
    click.echo(
        f"api_result: table={TABLE_NAME}; phase=contract_catalog; status=completed; "
        f"rows={len(contract_catalog_df)}; elapsed_s={perf_counter() - phase_started_at:.3f}"
    )
    collection_phase = "catalog_validation"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=catalog_validation; status=started")
''')
e.insert(e.find("candidate_contract_mask ="), '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=catalog_validation; status=completed; "
        f"fixed_contracts={len(contract_catalog_df)}"
    )
    collection_phase = "candidate_selection"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=candidate_selection; status=started")
''')
e.insert(e.find("contract_info_by_code ="), '''
    info_batch_total = (len(requested_contract_codes) + INFO_BATCH_SIZE - 1) // INFO_BATCH_SIZE
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=candidate_selection; status=completed; "
        f"contracts={len(requested_contract_codes)}; info_batches={info_batch_total}"
    )
''')
e.insert(e.find("batch_contract_info ="), '''
    collection_phase = "contract_info_request"
    phase_started_at = perf_counter()
    click.echo(
        f"request_batch: table={TABLE_NAME}; phase=contract_info; status=started; "
        f"completed={info_batch_count}; total={info_batch_total}; "
        f"batch={info_batch_count + 1}; contracts={len(batch_contract_codes)}"
    )
''')
e.insert(e.find("info_batch_count += 1"), 'collection_phase = "contract_info_validation"', after=True)
e.insert(e.find("for contract_code, contract_record in batch_contract_info.items()"), '''
    click.echo(
        f"api_result: table={TABLE_NAME}; phase=contract_info; status=completed; "
        f"completed={info_batch_count}; total={info_batch_total}; "
        f"requested_contracts={len(batch_contract_codes)}; returned_contracts={len(batch_contract_info)}; "
        f"elapsed_s={perf_counter() - phase_started_at:.3f}"
    )
''', after=True)
e.insert(e.find("missing_contract_codes ="), '''
    collection_phase = "info_coverage"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=info_coverage; status=started")
''')
e.insert(e.find("contract_info_df = pd.DataFrame"), '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=info_coverage; status=completed; "
        f"contracts={len(contract_info_by_code)}"
    )
    collection_phase = "catalog_merge"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=catalog_merge; status=started")
''')
e.insert(e.find("contract_catalog_df = contract_catalog_df.merge"), '''
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=catalog_merge; status=completed")
    collection_phase = "previous_trading_dates"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=previous_trading_dates; status=started")
''', after=True)
e.insert(e.find("boundary_window ="), '''
    phase_started_at = perf_counter()
    click.echo(
        f"request_batch: table={TABLE_NAME}; phase=boundary_trade_day; status=started; "
        f"end_date={earliest_trading_date}; count=2"
    )
''')
e.insert(e.find("boundary_trade_day_call_count = 1"), '''
    click.echo(
        f"api_result: table={TABLE_NAME}; phase=boundary_trade_day; status=completed; "
        f"previous_trading_date={boundary_previous_trading_date}; "
        f"elapsed_s={perf_counter() - phase_started_at:.3f}"
    )
''', after=True)
e.insert(e.find("return (contract_catalog_df"), '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=previous_trading_dates; status=completed; "
        f"trading_dates={len(previous_trading_date_by_trading_date)}"
    )
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=collect; status=completed; outcome=source_ready; "
        f"api_calls={source_summary['api_call_count']}; contracts={source_summary['contract_count']}; "
        f"info_batches={source_summary['info_batch_count']}; "
        f"boundary_trade_day_calls={source_summary['boundary_trade_day_call_count']}; "
        f"elapsed_s={perf_counter() - collection_started_at:.3f}"
    )
''')
e.wrap('''
    collection_started_at = perf_counter()
    collection_phase = "input"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=collect; status=started; "
        f"requested_variety_dates={len(requested_variety_calendar_df)}"
    )
''', '''
    except Exception as collection_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=collect; status=failed; "
            f"failed_phase={collection_phase}; error={type(collection_error).__name__}; "
            f"elapsed_s={perf_counter() - collection_started_at:.3f}"
        )
        raise
''')

e = FunctionEdit("c03-build", "build_contract_calendar_partition")
e.insert(e.find("contract_session_rows ="), '''
    build_partition_label = (
        "/".join(str(variety_partition_df.iloc[0][name]) for name in PARTITION_COLUMNS)
        if not variety_partition_df.empty else "empty"
    )
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=build; status=started; "
        f"partition={build_partition_label}; completed=0; total={len(variety_partition_df)}"
    )
    build_phase = "expand"
    processed_variety_day_count = 0
''')
e.insert(e.find("for contract in active_contract_catalog_df.itertuples"), '''
    processed_variety_day_count += 1
    if (
        processed_variety_day_count == 1
        or processed_variety_day_count % 25 == 0
        or processed_variety_day_count == len(variety_partition_df)
    ):
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=expand; status=running; "
            f"partition={build_partition_label}; completed={processed_variety_day_count}; "
            f"total={len(variety_partition_df)}; trading_date={variety_day.trading_date}; "
            f"candidate_contract_days={candidate_contract_day_count}; rows={len(contract_session_rows)}; "
            f"elapsed_s={perf_counter() - build_started_at:.3f}"
        )
''', after=True)
e.insert(e.find("new_contract_calendar_df = validate_contract_calendar_frame"), '''
    build_phase = "output_validation"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=output_validation; status=started; "
        f"partition={build_partition_label}; rows={len(new_contract_calendar_df)}"
    )
''')
e.insert(e.find("return (new_contract_calendar_df, audit)"), '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=build; status=completed; "
        f"partition={build_partition_label}; completed={processed_variety_day_count}; total={len(variety_partition_df)}; "
        f"rows={len(new_contract_calendar_df)}; candidate_contract_days={candidate_contract_day_count}; "
        f"no_valid_rule_contract_day_count={no_valid_rule_contract_day_count}; "
        f"elapsed_s={perf_counter() - build_started_at:.3f}"
    )
''')
e.wrap('''
    build_started_at = perf_counter()
    build_phase = "input"
    build_partition_label = "unknown"
''', '''
    except Exception as build_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=build; status=failed; "
            f"partition={build_partition_label}; failed_phase={build_phase}; "
            f"error={type(build_error).__name__}; elapsed_s={perf_counter() - build_started_at:.3f}"
        )
        raise
''')

e = FunctionEdit("c03-commit", "commit_partition")
e.insert(e.find("existing_contract_calendar_dataset = None"), '''
    commit_phase = "existing_dataset"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=existing_dataset; status=started; "
        f"partition={partition_key}; run_id={run_id}"
    )
''')
# The merge branch has a nested guard with the same prefix; select the top-level branch.
merge_node = next(n for n in e.function.body if isinstance(n, ast.If) and ast.unparse(n.test) == 'replacement_variety_date_keys is not None')
e.insert(merge_node, '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=existing_dataset; status=completed; "
        f"partition={partition_key}; rows={len(existing_contract_calendar_df)}; run_id={run_id}"
    )
    commit_phase = "merge_validate"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=merge_validate; status=started; "
        f"partition={partition_key}; run_id={run_id}"
    )
''')
e.insert(e.find("relative_path = pathlib.Path"), '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=merge_validate; status=completed; "
        f"partition={partition_key}; rows={len(replacement_contract_calendar_df)}; "
        f"replacement_rows={len(complete_partition_table)}; run_id={run_id}"
    )
    commit_phase = "staging_write"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=staging_write; status=started; "
        f"partition={partition_key}; run_id={run_id}"
    )
''')
e.insert(e.find("staged_contract_calendar_dataset = ds.dataset"), '''
    commit_phase = "staging_readback"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=staging_readback; status=started; "
        f"partition={partition_key}; run_id={run_id}"
    )
''')
e.insert(e.find("source_path = staging_path"), '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=staging; status=completed; "
        f"partition={partition_key}; rows={len(complete_partition_table)}; run_id={run_id}"
    )
    commit_phase = "prepare_install"
''')
install_try = next(n for n in e.function.body if isinstance(n, ast.Try) and any(h.name == 'commit_error' for h in n.handlers))
e.insert(install_try, '''
    commit_phase = "install"
    click.echo(
        f"partition_start: table={TABLE_NAME}; phase=install; status=started; "
        f"partition={partition_key}; completed=0; total=1; run_id={run_id}"
    )
''')
e.insert(e.find("data_files = []"), '''
    commit_phase = "formal_readback"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=formal_readback; status=started; "
        f"partition={partition_key}; run_id={run_id}"
    )
''')
handler = next(h for h in install_try.handlers if h.name == 'commit_error')
e.insert(handler.body[0], 'rollback_status = "started"')
rollback_check = next(n for n in handler.body if isinstance(n, ast.If) and ast.unparse(n.test) == 'rollback_errors')
e.insert(rollback_check, 'rollback_status = "failed" if rollback_errors else "completed"')
e.insert(e.find("return len(replacement_contract_calendar_df)"), '''
    click.echo(
        f"partition_committed: table={TABLE_NAME}; phase=commit; status=completed; "
        f"partition={partition_key}; completed=1; total=1; rows={len(replacement_contract_calendar_df)}; "
        f"replacement_rows={len(complete_partition_table)}; run_id={run_id}; "
        f"elapsed_s={perf_counter() - commit_started_at:.3f}"
    )
''')
e.wrap('''
    commit_started_at = perf_counter()
    commit_phase = "input_validation"
    rollback_status = "not_required"
    click.echo(
        f"partition_plan: table={TABLE_NAME}; phase=commit; status=started; "
        f"partition={partition_key}; rows={len(replacement_contract_calendar_df)}; lake_root={lake_root}"
    )
''', '''
    except Exception as partition_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=commit; status=failed; "
            f"partition={partition_key}; failed_phase={commit_phase}; rollback={rollback_status}; "
            f"error={type(partition_error).__name__}; elapsed_s={perf_counter() - commit_started_at:.3f}"
        )
        raise
''')

e = FunctionEdit("d36703b192451764", "write_automatic_tail_processed_through")
marker_try = next(n for n in e.function.body if isinstance(n, ast.Try))
e.insert(marker_try.body[0], '''
    watermark_phase = "staging_write"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=watermark_staging_write; status=started")
''')
e.insert(marker_try.body[1], '''
    watermark_phase = "staging_readback"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=watermark_staging_readback; status=started")
''')
e.insert(marker_try.body[2], '''
    watermark_phase = "install"
    click.echo(f"planning_progress: table={TABLE_NAME}; phase=watermark_install; status=started")
''')
e.insert(marker_try, '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=watermark; status=completed; "
        f"outcome=watermark_committed; automatic_tail_processed_through={processed_through}; "
        f"elapsed_s={perf_counter() - watermark_started_at:.3f}"
    )
''', after=True)
e.wrap('''
    watermark_started_at = perf_counter()
    watermark_phase = "prepare"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; phase=watermark; status=started; "
        f"automatic_tail_processed_through={processed_through}; lake_root={lake_root}"
    )
''', '''
    except Exception as watermark_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=failed; "
            f"failed_phase={watermark_phase}; automatic_tail_processed_through={processed_through}; "
            f"error={type(watermark_error).__name__}; elapsed_s={perf_counter() - watermark_started_at:.3f}"
        )
        raise
''')

# Remove only the two operation summaries now owned by their functions.
cell = by_id["c03-cli"]
cli_source = "".join(cell["source"])
lines = cli_source.splitlines(keepends=True)
remove_lines = set()
for n in ast.walk(ast.parse(cli_source)):
    if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and ast.unparse(n.value.func) == 'click.echo':
        text = ast.get_source_segment(cli_source, n)
        if 'outcome=source_ready' in text or 'outcome=watermark_committed' in text:
            remove_lines.update(range(n.lineno - 1, n.end_lineno))
assert len(remove_lines) > 0
cli_source = ''.join(line for index, line in enumerate(lines) if index not in remove_lines)
assert cli_source.count('        source_summary,') == 1
cli_source = cli_source.replace('        source_summary,', '        _,', 1)
cli_source = cli_source.replace(
    'f"committed: table={TABLE_NAME}; phase=commit; status=completed; mode={mode}; "',
    'f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=completed; mode={mode}; "',
)
cli_source = cli_source.replace(
    '    committed_row_count = 0\n',
    '    committed_row_count = 0\n'
    '    committed_partition_count = 0\n'
    '    click.echo(\n'
    '        f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=started; "\n'
    '        f"completed=0; total={len(dirty_partition_plans)}"\n'
    '    )\n',
    1,
)
cli_lines = cli_source.splitlines(keepends=True)
commit_loop = next(
    n for n in ast.walk(ast.parse(cli_source))
    if isinstance(n, ast.For) and ast.unparse(n.target) == 'partition_plan'
    and any(isinstance(s, ast.AugAssign) for s in ast.walk(n))
)
cli_lines.insert(commit_loop.end_lineno, '''
        committed_partition_count += 1
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=running; "
            f"completed={committed_partition_count}; total={len(dirty_partition_plans)}; "
            f"rows={committed_row_count}; elapsed_s={perf_counter() - run_started_at:.3f}"
        )
''')
cli_source = ''.join(cli_lines)
cell['source'] = cli_source.splitlines(keepends=True)

markdown_replacements = {
    "c03-source-heading": [
        ('当前来源汇总由 `main()` 在函数返回后输出；函数内部尚无逐批进度日志。', '函数自身报告认证、目录检查、候选筛选、合约信息批次及边界交易日查询的进度，完成时输出 API 汇总与耗时；空请求输出零 API 完成记录。异常记录失败阶段后原样抛出。'),
    ],
    "b03-flow-source": [
        ('同步 API 的内部进度未在当前函数中打印。', '每次同步 API 请求前报告开始，返回并通过相应检查后报告结果；请求执行中不虚构百分比。'),
        ('H["每批至多 200 个合约请求信息"]', 'H["每批至多 200 个合约请求信息；报告已完成批数"]'),
        ('M["构建日期映射；返回目录与 API 汇总"]', 'M["构建日期映射；记录完成日志并返回目录与 API 汇总"]'),
    ],
    "c03-build-heading": [
        ('生成后完成本表业务校验，并返回无有效规则的统计。', '函数在开始、生成进度和完成时记录日志：首个、每 25 个及最后一个品种日报告已处理数、候选合约日数和已生成行数。生成后完成本表业务校验，记录耗时并返回无有效规则统计；样例 warning 仍由入口作本批汇总。'),
    ],
    "b03-flow-build": [
        ('J["继续其余 Session、合约及品种日"]', 'J["继续其余 Session、合约及品种日；定期报告生成进度"]'),
        ('L["完整业务校验；返回结果与审计统计"]', 'L["完整业务校验；记录完成日志并返回结果与审计统计"]'),
    ],
    "c03-commit-heading": [
        ('根水位标记由自动入口在分区工作成功后单独写入。', '入口在本批分区工作成功后调用水位写入函数。提交函数自行报告输入、旧叶读取、合并校验、暂存复读、正式安装及完成耗时；失败日志在原有恢复与清理路径结束后输出，包含失败阶段和恢复状态。'),
    ],
    "b03-flow-commit": [
        ('自动批次水位由 main 另行推进。', '提交进度由本函数报告；失败日志在原有恢复和清理处理之后输出。自动批次水位由 main 另行调用写入函数推进。'),
        ('T["清理临时目录；返回本次替换输入行数"]', 'T["清理临时目录；记录提交完成并返回替换输入行数"]'),
    ],
    "b03-config-heading": [
        ('函数定义本身不执行这些 I/O。', '写入函数自行报告开始、暂存写入、复读、安装和完成耗时；失败时记录阶段并继续抛出异常。函数定义本身不执行这些 I/O。'),
    ],
    "b03-flow-config": [
        ('H["写水位：构造含已处理日期的空表 Schema"]', 'H["写水位：记录开始；构造含已处理日期的空表 Schema"]'),
        ('K["清理临时标记；写入异常也执行清理"]', 'K["清理临时标记后记录完成；异常清理后记录失败"]'),
    ],
    "c03-cli-heading": [
        ('来源汇总和批次提交汇总仍由入口负责。', '采集、生成、单分区提交和水位写入日志由对应函数负责；入口保留模式、差异计划、聚合 warning 与批次汇总，不重复报告来源或水位完成。批次提交汇总使用 `phase=commit_batch`，与单分区完成事件区分。'),
    ],
}
for cell_id, replacements in markdown_replacements.items():
    c = by_id[cell_id]
    source = ''.join(c['source'])
    for old, new in replacements:
        assert source.count(old) == 1, (cell_id, old)
        source = source.replace(old, new, 1)
    c['source'] = source.splitlines(keepends=True)


LOG_VARIABLES = {
    'collection_started_at', 'collection_phase', 'phase_started_at', 'info_batch_total',
    'build_started_at', 'build_phase', 'build_partition_label', 'processed_variety_day_count',
    'commit_started_at', 'commit_phase', 'rollback_status',
    'committed_partition_count',
    'watermark_started_at', 'watermark_phase',
}
LOG_ERRORS = {'collection_error', 'build_error', 'partition_error', 'watermark_error'}


class WithoutFunctionLogs(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and t.id in LOG_VARIABLES for t in node.targets):
            return None
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'collect_source_data':
            node.targets[0].elts[2].id = '_'
        return self.generic_visit(node)

    def visit_AugAssign(self, node):
        if isinstance(node.target, ast.Name) and node.target.id in LOG_VARIABLES:
            return None
        return self.generic_visit(node)

    def visit_If(self, node):
        node = self.generic_visit(node)
        return node if node.body else None

    def visit_Try(self, node):
        if len(node.handlers) == 1 and node.handlers[0].name in LOG_ERRORS:
            node = self.generic_visit(node)
            return node.body
        return self.generic_visit(node)


code = lambda nb: '\n\n'.join(''.join(c['source']) for c in nb['cells'] if c['cell_type'] == 'code')
old_code, new_code = code(before), code(notebook)
old_tree = WithoutFunctionLogs().visit(ast.parse(old_code))
new_tree = WithoutFunctionLogs().visit(ast.parse(new_code))
assert ast.dump(old_tree) == ast.dump(new_tree), '业务 AST 发生了非日志变化'
comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
assert comments(old_code) == comments(new_code)
for old_cell in before['cells']:
    current = by_id[old_cell['id']]
    assert {k: v for k, v in old_cell.items() if k != 'source'} == {k: v for k, v in current.items() if k != 'source'}

snapshot = pathlib.Path(tempfile.mkdtemp(prefix='b03-function-logs-before-'))
(snapshot / PATH.name).write_bytes(before_bytes)
(snapshot / PATH.with_suffix('.py').name).write_bytes(PATH.with_suffix('.py').read_bytes())
hashes = {
    str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
    for directory in (ROOT / '02_Futures_Lakehouse').glob('a0[1-4]_*')
    for p in directory.glob('*.py')
    if p != PATH.with_suffix('.py')
}
(snapshot / 'other_script_hashes.json').write_text(json.dumps(hashes, indent=2), encoding='utf-8')
PATH.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + '\n', encoding='utf-8', newline='\n')
print(json.dumps({'snapshot': str(snapshot), 'business_ast_unchanged': True, 'code_comments_unchanged': True}, ensure_ascii=False))
