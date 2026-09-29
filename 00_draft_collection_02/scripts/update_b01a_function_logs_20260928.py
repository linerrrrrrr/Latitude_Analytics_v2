"""b01a 第 3/5 轮：函数日志归位与真实 raw 就绪状态；不改变提交机制。"""
import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01a_position_rank_special_case_calibration.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id: c for c in notebook.cells}


def replace(cell_id, old, new):
    assert cells[cell_id].source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = cells[cell_id].source.replace(old, new)


source = cells['ad3d3d92'].source
split_at = source.index('\n\ndef calibration_manifest(')
parser, rest = source[:split_at], source[split_at:]
start = parser.index('    try:\n')
body = parser[start:]
body = body.replace('    source_rows = payload.get', '    log_phase = "source_rows"\n    source_rows = payload.get')
body = body.replace('''    official_rows = []
    for source_row in source_rows:
''', '''    log_phase = "row_scan"
    official_rows = []
    for source_row in source_rows:
        if log_scanned_rows and log_scanned_rows % 1000 == 0:
            log_now = time.perf_counter()
            if log_now - log_last_progress_at >= 2.0:
                click.echo(
                    "planning_progress: artifact=position_rank_special_cases; function=parse_official_volume_rows; phase=parse; status=running; "
                    f"case_id={special_case['case_id']}; scanned_source_rows={log_scanned_rows}/{len(source_rows)}; "
                    f"matched_rows={len(official_rows)}; elapsed_s={log_now - log_started_at:.3f}"
                )
                log_last_progress_at = log_now
        log_scanned_rows += 1
''')
body = body.replace('    official_rows = tuple(sorted(official_rows))', '    log_phase = "top_twenty"\n    official_rows = tuple(sorted(official_rows))')
body = body.replace('    return official_rows', '''    click.echo(
        "planning_progress: artifact=position_rank_special_cases; function=parse_official_volume_rows; phase=parse; status=completed; "
        f"case_id={special_case['case_id']}; scanned_source_rows={log_scanned_rows}/{len(source_rows)}; "
        f"matched_rows={len(official_rows)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    return official_rows''')
parser = parser[:start] + '''    log_started_at = time.perf_counter()
    log_last_progress_at = log_started_at
    log_phase = "decode"
    log_scanned_rows = 0
    click.echo(
        "planning_progress: artifact=position_rank_special_cases; function=parse_official_volume_rows; phase=parse; status=started; "
        f"case_id={special_case['case_id']}; response_bytes={len(response_content)}; elapsed_s=0.000"
    )
    try:
''' + textwrap.indent(body, '    ') + '''
    except Exception as log_error:
        click.echo(
            "planning_progress: artifact=position_rank_special_cases; function=parse_official_volume_rows; phase=parse; status=failed; "
            f"case_id={special_case['case_id']}; failed_phase={log_phase}; scanned_source_rows={log_scanned_rows}; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise'''
start = rest.index('    response_sha256 = hashlib.sha256(')
body = rest[start:]
body = body.replace('    official_rows = parse_official_volume_rows', '    log_phase = "parse_official_rows"\n    official_rows = parse_official_volume_rows')
body = body.replace('    return calibration_manifest(special_case, response_sha256, official_rows)', '''    log_phase = "manifest"
    manifest = calibration_manifest(special_case, response_sha256, official_rows)
    click.echo(
        "planning_progress: artifact=position_rank_special_cases; function=validate_response; phase=generate_manifest; status=completed; "
        f"case_id={special_case['case_id']}; rows={len(official_rows)}; sha256={response_sha256}; persisted=false; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    return manifest''')
rest = rest[:start] + '''    log_started_at = time.perf_counter()
    log_phase = "response_digest"
    click.echo(
        "验收原文并生成校准清单 / Validate response and build manifest\\n"
        "planning_progress: artifact=position_rank_special_cases; function=validate_response; phase=generate_manifest; status=started; "
        f"case_id={special_case['case_id']}; response_bytes={len(response_content)}; persisted=false; elapsed_s=0.000"
    )
    try:
''' + textwrap.indent(body, '    ') + '''
    except Exception as log_error:
        click.echo(
            "planning_progress: artifact=position_rank_special_cases; function=validate_response; phase=generate_manifest; status=failed; "
            f"case_id={special_case['case_id']}; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise'''
cells['ad3d3d92'].source = parser + rest

source = cells['b01a-verify-artifacts'].source
start = source.index('    actual_filenames = ')
body = source[start:]
body = body.replace('    response_content = ', '    log_phase = "response_read"\n    response_content = ')
body = body.replace('    expected_manifest = ', '    log_phase = "response_validate"\n    expected_manifest = ')
body = body.replace('    sidecar_sha256 = ', '''    log_verified_files = 1
    click.echo(
        "planning_progress: artifact=position_rank_special_cases; function=verify_artifacts; phase=verify; status=running; "
        f"case_id={special_case['case_id']}; path={artifact_path}; verified_files=1/3; file=response.dat; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    log_phase = "sidecar"
    sidecar_sha256 = ''')
body = body.replace('    actual_manifest = ', '''    log_verified_files = 2
    click.echo(
        "planning_progress: artifact=position_rank_special_cases; function=verify_artifacts; phase=verify; status=running; "
        f"case_id={special_case['case_id']}; path={artifact_path}; verified_files=2/3; file=response.sha256; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    log_phase = "manifest"
    actual_manifest = ''')
body = body.replace('    return actual_manifest', '''    log_verified_files = 3
    click.echo(
        "planning_progress: artifact=position_rank_special_cases; function=verify_artifacts; phase=verify; status=completed; "
        f"case_id={special_case['case_id']}; path={artifact_path}; verified_files=3/3; file=calibration.json; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    return actual_manifest''')
cells['b01a-verify-artifacts'].source = source[:start] + '''    log_started_at = time.perf_counter()
    log_phase = "file_set"
    log_verified_files = 0
    click.echo(
        "复读案例证据 / Verify case artifacts\\n"
        "planning_progress: artifact=position_rank_special_cases; function=verify_artifacts; phase=verify; status=started; "
        f"case_id={special_case['case_id']}; path={artifact_path}; verified_files=0/3; elapsed_s=0.000"
    )
    try:
''' + textwrap.indent(body, '    ') + '''
    except Exception as log_error:
        click.echo(
            "planning_progress: artifact=position_rank_special_cases; function=verify_artifacts; phase=verify; status=failed; "
            f"case_id={special_case['case_id']}; path={artifact_path}; failed_phase={log_phase}; verified_files={log_verified_files}/3; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise'''

fetch_source = '''def fetch_official_response(
    special_case: dict[str, object],
) -> tuple[requests.Response, dict[str, object]]:
    log_started_at = time.perf_counter()
    log_phase = "request"
    click.echo(
        "请求官方冻结来源 / Request official source\\n"
        "planning_progress: artifact=position_rank_special_cases; function=fetch_official_response; phase=fetch; status=started; "
        f"case_id={special_case['case_id']}; timeout_s=60; elapsed_s=0.000"
    )
    try:
        response = requests.get(special_case["official_url"], timeout=60)
        if response.status_code != 200:
            raise RuntimeError(
                "上期所特殊案例请求失败；"
                f"case_id={special_case['case_id']}, status={response.status_code}。"
            )
        click.echo(
            "planning_progress: artifact=position_rank_special_cases; function=fetch_official_response; phase=response_received; status=completed; "
            f"case_id={special_case['case_id']}; http_status=200; response_bytes={len(response.content)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "source_validate"
        manifest = validate_response(response.content, special_case)
        click.echo(
            "special_case_source_valid: artifact=position_rank_special_cases; function=fetch_official_response; phase=fetch; status=completed; "
            f"case_id={special_case['case_id']}; rows=20; sha256={manifest['official_response_sha256']}; persisted=false; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return response, manifest
    except Exception as log_error:
        click.echo(
            "planning_progress: artifact=position_rank_special_cases; function=fetch_official_response; phase=fetch; status=failed; "
            f"case_id={special_case['case_id']}; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise'''

commit_source = '''def commit_artifacts(
    artifact_path: pathlib.Path,
    response_content: bytes,
    manifest: dict[str, object],
    special_case: dict[str, object],
) -> None:
    log_started_at = time.perf_counter()
    log_phase = "staging_prepare"
    log_written_files = 0
    click.echo(
        "归档案例证据 / Commit case artifacts\\n"
        "planning_progress: artifact=position_rank_special_cases; function=commit_artifacts; phase=commit; status=started; "
        f"case_id={special_case['case_id']}; path={artifact_path}; written_files=0/3; elapsed_s=0.000"
    )
    try:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        staging_path = artifact_path.parent / (
            f".{special_case['case_id']}.staging-{uuid.uuid4().hex}"
        )
        try:
            staging_path.mkdir(parents=False, exist_ok=False)
            log_phase = "staging_write"
            (staging_path / "response.dat").write_bytes(response_content)
            log_written_files = 1
            click.echo(
                "planning_progress: artifact=position_rank_special_cases; function=commit_artifacts; phase=staging_write; status=running; "
                f"case_id={special_case['case_id']}; written_files=1/3; file=response.dat; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            (staging_path / "response.sha256").write_text(
                manifest["official_response_sha256"] + "\\n",
                encoding="ascii",
                newline="\\n",
            )
            log_written_files = 2
            click.echo(
                "planning_progress: artifact=position_rank_special_cases; function=commit_artifacts; phase=staging_write; status=running; "
                f"case_id={special_case['case_id']}; written_files=2/3; file=response.sha256; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            (staging_path / "calibration.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2) + "\\n",
                encoding="utf-8",
                newline="\\n",
            )
            log_written_files = 3
            click.echo(
                "planning_progress: artifact=position_rank_special_cases; function=commit_artifacts; phase=staging_write; status=completed; "
                f"case_id={special_case['case_id']}; written_files=3/3; file=calibration.json; persisted=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            log_phase = "staging_verify"
            verify_artifacts(staging_path, special_case)
            log_phase = "install"
            click.echo(
                "安装案例目录 / Install case directory\\n"
                "planning_progress: artifact=position_rank_special_cases; function=commit_artifacts; phase=install; status=started; "
                f"case_id={special_case['case_id']}; path={artifact_path}; batch_state=pending; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            if artifact_path.exists():
                raise RuntimeError("特殊案例正式证据目录在提交前并发出现。")
            os.replace(staging_path, artifact_path)
            log_phase = "formal_verify"
            click.echo(
                "planning_progress: artifact=position_rank_special_cases; function=commit_artifacts; phase=formal_verify; status=started; "
                f"case_id={special_case['case_id']}; path={artifact_path}; batch_state=pending; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            verify_artifacts(artifact_path, special_case)
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise
        click.echo(
            "special_case_committed: artifact=position_rank_special_cases; function=commit_artifacts; phase=commit; status=completed; "
            f"case_id={special_case['case_id']}; path={artifact_path}; persisted=true; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(
            "planning_progress: artifact=position_rank_special_cases; function=commit_artifacts; phase=artifact_state; status=completed; "
            f"case_id={special_case['case_id']}; path={artifact_path}; verified_files=3/3; persisted=true; date_watermark=none; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
    except Exception as log_error:
        click.echo(
            "planning_progress: artifact=position_rank_special_cases; function=commit_artifacts; phase=commit; status=failed; "
            f"case_id={special_case['case_id']}; failed_phase={log_phase}; written_files={log_written_files}/3; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise'''

source = cells['e38b03b7'].source
source = source.replace('    log_case_index = 0\n', '    log_case_index = 0\n    log_existing_cases = 0\n    log_committed_cases = 0\n    log_source_only_cases = 0\n')
start = source.index('                click.echo(\n                    "复读已有证据')
end = source.index('                manifest = verify_artifacts', start)
source = source[:start] + source[end:]
source = source.replace('                manifest = verify_artifacts(artifact_path, special_case)\n', '                manifest = verify_artifacts(artifact_path, special_case)\n                log_existing_cases += 1\n')
source = source.replace('function=main; phase=existing_verify; status=completed;', 'function=main; phase=case_state; status=completed; outcome=existing_verified;')
start = source.index('            log_phase = "source_request"')
end = source.index('            if not write:', start)
source = source[:start] + '''            log_phase = "fetch"
            response, manifest = fetch_official_response(special_case)
''' + source[end:]
source = source.replace('            if not write:\n', '            if not write:\n                log_source_only_cases += 1\n')
start = source.index('            log_phase = "staging_prepare"')
end = source.index('        click.echo("position_rank_special_cases_ready: true")', start)
source = source[:start] + '''            log_phase = "commit"
            commit_artifacts(artifact_path, response.content, manifest, special_case)
            log_committed_cases += 1

''' + source[end:]
source = source.replace('        click.echo("position_rank_special_cases_ready: true")', '''        click.echo(
            f"position_rank_special_cases_ready: {str(log_source_only_cases == 0).lower()}; "
            "artifact=position_rank_special_cases; function=main; phase=artifact_state; status=completed; scope=configured_cases; "
            f"existing_verified_cases={log_existing_cases}; committed_cases={log_committed_cases}; "
            f"source_only_cases={log_source_only_cases}; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )''')
cells['e38b03b7'].source = source


def flow(cell_id, title, body):
    return nbformat.v4.new_markdown_cell(title + '\n\n```mermaid\n'
        '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
        + textwrap.dedent(body).strip() + '\n```', id=cell_id)


fetch_cells = [
    nbformat.v4.new_markdown_cell('''## 单案例来源请求

`fetch_official_response()` 承担完整的“请求一次官方 URL、检查 HTTP 200、验收原文并返回响应及清单”操作。自行报告请求开始、响应字节数、来源验收完成和失败阶段；等待 HTTP 响应期间不虚构下载百分比，不新增请求、重试或心跳线程。

`special_case_source_valid:` 只在来源验收通过后发出，标记 `persisted=false`。它证明本次响应匹配冻结配置，不代表三个 raw 文件已经归档。''', id='b01a-fetch-heading'),
    flow('b01a-flow-fetch', '### 局部流程：请求与来源验收', '''
flowchart TD
    A["函数报告 fetch 开始"] --> B["单次 GET；timeout=60；检查 HTTP 200"]
    B --> C["报告收到响应与字节数"]
    C --> D["validate_response 自行报告原文验收和清单生成"]
    D --> E["报告 source_valid、persisted=false；返回响应和清单"]
    B -. 失败 .-> X["函数报告失败阶段；原异常继续抛出"]
    D -. 失败 .-> X
'''),
    nbformat.v4.new_code_cell(fetch_source, id='b01a-fetch-response'),
]
commit_cells = [
    nbformat.v4.new_markdown_cell('''## 单案例归档与提交状态

`commit_artifacts()` 接收已经验收的响应字节及清单，负责 staging 写入、复读、整目录安装和正式复读。三次文件写入后分别报告 `written_files=1/3`、`2/3`、`3/3`；复读由 `verify_artifacts()` 自行报告，不增加额外读取。安装及正式复读期间记 `batch_state=pending`。

只有正式复读通过，才由本函数输出 `special_case_committed:` 和 `phase=artifact_state; persisted=true; date_watermark=none`。本环节没有日期水位，落盘状态指向当前案例的三个证据文件。

提交机制保持原样：在目标旁暂存，确认正式目录仍不存在后用 `os.replace()` 安装。失败只清理 staging 并报告具体阶段；安装后的正式目录目前不回滚。共享事务属于后续第 10 项，此前成功案例也不在当前案例的恢复范围内。''', id='b01a-commit-heading'),
    flow('b01a-flow-commit', '### 局部流程：归档、复读与落盘状态', '''
flowchart TD
    A["函数报告 commit 开始；准备 staging"] --> B["逐个写三个文件；报告 1/3、2/3、3/3"]
    B --> C["verify_artifacts 自行报告 staging 复读"]
    C --> D["确认正式目录仍不存在；安装目录；仍 pending"]
    D --> E["verify_artifacts 自行报告正式复读"]
    E --> F["报告 committed 和 artifact_state 已落盘；无日期水位"]
    B -. 失败 .-> X["清理 staging；报告失败阶段；抛错"]
    C -. 失败 .-> X
    D -. 失败 .-> X
    E -. 失败 .-> X
    X --> Y["当前已安装目录不回滚；后续案例停止"]
'''),
    nbformat.v4.new_code_cell(commit_source, id='b01a-commit-artifacts'),
]
replace('db16e4ab', '本轮保留全部检查。', '''全部检查继续保留。

`validate_response()` 自行报告验收及清单生成起止、输出行数、摘要和失败阶段；清单在内存中生成，记 `persisted=false`。`calibration_manifest()` 只组装字段，不另外重复同一生成阶段日志。解析函数沿已有记录循环，每处理 1000 行检查一次 2 秒进度间隔，报告已扫描记录和已匹配官方行；不为日志再遍历响应。''')
replace('b01a-verify-heading', '任何异常继续向调用方抛出。', '任何异常继续向调用方抛出。函数自行报告开始、已验收文件数 `0/3` 至 `3/3`、当前路径、累计耗时和失败阶段；单个文件只有核对通过才增加计数。复读完成不新增写入，也不自行宣布整批案例已就绪。')
cells['ca173bcd'].source = '''## CLI：案例调度与整批就绪汇总

`main()` 检查案例路径边界，已有目录调用 `verify_artifacts()`，缺失目录调用 `fetch_official_response()`；启用 `--write` 后再调用 `commit_artifacts()`。入口保留运行边界、案例序号、已有证据结果摘要和只读跳过提交，不重复函数内部请求、生成、复读或提交起止日志。

日志继续使用 88 个 `=` 的运行边界和 `artifact/function/phase/status` 字段；各函数的 `elapsed_s` 分别从本次函数调用开始累计。`special_case_existing_valid:`、`special_case_source_valid:`、`special_case_committed:` 前缀继续供 monitor 识别。

`position_rank_special_cases_ready` 现在表示本次选定湖中全部配置案例的证据已通过正式复读：只有已有证据复读和本次提交成功可以计入就绪；只读下载验收计为 `source_only_cases`，只要有此类案例就报告 `false`，整次只读检查仍正常完成。已有证据数、新提交数和仅来源验收数直接沿原循环累计，不重新扫描文件。

本入口没有独立日期水位，统一记 `date_watermark=none`。失败时停止后续案例并保留原异常，不输出整批就绪或正常结束日志；具体失败阶段由负责操作的函数报告。'''
cells['b01a-flow-cli'].source = flow('b01a-flow-cli', '### 局部流程：调度与整批就绪汇总', '''
flowchart TD
    A["运行开始；逐案例检查路径"] --> B{"目录存在？"}
    B -->|是| C["调用 verify_artifacts；成功后累计已有证据"]
    B -->|否| D["调用 fetch_official_response；函数自行报告"]
    D --> E{"启用写入？"}
    E -->|否| F["累计 source_only；报告跳过提交"]
    E -->|是| G["调用 commit_artifacts；成功后累计新提交"]
    C --> H["继续下一案例"]
    F --> H
    G --> H
    H --> I{"全部案例结束；source_only 为 0？"}
    I -->|是| J["报告证据 ready=true；无日期水位"]
    I -->|否| K["报告 ready=false；只读检查正常完成"]
    C -. 失败 .-> X["入口报告运行失败；原异常继续抛出"]
    D -. 失败 .-> X
    G -. 失败 .-> X
''').source
replace('b01a-flow-response', '生成含身份、来源、摘要和全部官方行的清单', '生成清单；函数报告完成、persisted=false')
replace('b01a-flow-response', '筛选目标合约、1—20 名次；核对字段类型', '沿原循环报告扫描进度；筛选目标合约并核对类型')
replace('b01a-flow-verify', '核对目录恰好包含三个证据文件', '函数报告开始；核对三个文件集合')
replace('b01a-flow-verify', '返回已验收清单', '报告 verified_files=3/3；返回已验收清单')
replace('b01a-flow-overview', '全部案例通过后报告本轮完成', '汇总正式证据就绪状态；只读来源不算落盘')

updated = []
for cell in notebook.cells:
    if cell.id == 'ca173bcd':
        updated.extend(fetch_cells + commit_cells)
    updated.append(cell)
notebook.cells = updated

assert before.metadata == notebook.metadata
newcells = {c.id: c for c in notebook.cells}
for old in before.cells:
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in newcells[old.id].items() if k != 'source'}
comments = lambda text: [t.string for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type == tokenize.COMMENT]
old_code = '\n\n'.join(c.source for c in before.cells if c.cell_type == 'code')
new_code = '\n\n'.join(c.source for c in notebook.cells if c.cell_type == 'code')
assert comments(old_code) == comments(new_code)
for index, cell in enumerate(notebook.cells):
    if cell.cell_type == 'code':
        compile(cell.source, str(PATH), 'exec')
        assert '```mermaid' in notebook.cells[index-1].source
assert newcells['30c24e87'] == next(c for c in before.cells if c.id == '30c24e87')
assert 'StagedPathTransaction' not in new_code
nbformat.validate(notebook)
nbformat.write(notebook, PATH)
print('b01a function logs and raw readiness updated; comments, cell state and entry preserved; 9 diagrams.')
