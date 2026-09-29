"""b01a 第 2/5 轮：说明、流程图和入口日志，不改变业务 AST。"""
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
cells = {cell.id: cell for cell in notebook.cells}


def flow(cell_id, title, body):
    return nbformat.v4.new_markdown_cell(
        title + '\n\n```mermaid\n'
        '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
        + textwrap.dedent(body).strip() + '\n```', id=cell_id,
    )


cells['f8c42dd9'].source = '''# b01a 成交持仓排名特殊案例校准证据

本环节按 `config/futures_lakehouse/futures_position_rank_special_cases.py` 中的冻结案例，准备上期所官方原文、SHA-256 sidecar 和校准清单，供 a02/b02 判断是否可以应用已确认的成交量榜校准。

| 上下游 | 与本环节的关系 |
| --- | --- |
| a02/b01 报告日历 | 运行顺序上的前置阶段；本环节不读取或回写报告日历。 |
| 特殊案例共享配置 | 唯一的案例名单、URL、目标合约、响应摘要和完整 Top 20 校准值来源；配置同时供 b02 使用。 |
| 上期所官方历史文件 | 证据缺失时每个配置案例请求一次；不查询 JQData、不重试。 |
| a02/b02 成交持仓报告 | 只有指定格点的完整 JQData 坏载荷指纹精确匹配，且正式 raw 证据通过核对，才替换该榜；官方值原样通过，第三种载荷仍失败。 |

输出固定为 `raw/shfe/position_rank_special_cases/<case_id>/` 下的 `response.dat`、`response.sha256`、`calibration.json`。本环节没有 silver Schema、分区日历或独立日期水位，也不自动发现未知特殊案例。

raw 证据验收与下游 silver 校准是两个环节；本环节完成不代表报告事实已采集。文本权威见湖仓根目录 `README.md` 和 `03_Futures_Database/AGENTS.md`，冻结内容以共享配置为准。'''
boundary = nbformat.v4.new_markdown_cell('''## 运行分支与写入边界

| 当前案例目录 | 不带 `--write` | 带 `--write` |
| --- | --- | --- |
| 已存在 | 复读三个文件；通过则继续，不联网。 | 同样只复读，不覆盖已有证据。 |
| 不存在 | 请求一次官方 URL，核对响应与冻结值，不落盘。 | 请求并核对后暂存三个文件，复读通过再整目录安装，随后正式复读。 |
| 已存在但验收不通过 | 直接失败。 | 直接失败；不自动重新请求或替换。 |

`--lake-root` 默认使用 `settings.futures_lake_root`；本入口没有日期、`--full` 或单案例筛选参数，依次处理配置中的所有案例。每个案例单独归档，此前成功案例不会因后一个案例失败而撤销。

当前安装使用同目录 `os.replace()`。正式安装后复读失败时，现有异常分支仅清理 staging，已安装的案例目录仍在原位；这里如实描述现状，共享事务与失败恢复由后续改动处理。''', id='b01a-boundary')
overview = flow('b01a-flow-overview', '## 总流程：冻结案例证据归档', '''
flowchart TD
    A["读取配置案例；定位 raw 根"] --> B["依次定位案例目录；检查路径边界"]
    B --> C{"案例目录已存在？"}
    C -->|是| D["复读三个文件与冻结配置；不联网"]
    C -->|否| E["请求一次官方 URL；要求 HTTP 200"]
    E --> F["核对完整摘要、目标合约和 Top 20；生成清单"]
    F --> G{"启用 --write？"}
    G -->|否| H["仅来源验证通过；不落盘"]
    G -->|是| I["暂存三个文件并复读；确认正式目录仍不存在"]
    I --> J["整目录安装；正式复读"]
    D -->|通过| K["报告当前案例完成；继续下一案例"]
    H --> K
    J -->|通过| K
    K --> L["全部案例通过后报告本轮完成"]
    D -. 失败 .-> X["抛错并停止后续案例；已成功案例保留"]
    E -. 失败 .-> X
    F -. 失败 .-> X
    I -. 失败 .-> Y["仅清理 staging；抛错；已安装目录不回滚"]
    J -. 失败 .-> Y
    Y --> X
''')
init_heading = nbformat.v4.new_markdown_cell('''## 初始化与冻结配置

按项目标记文件定位根目录，导入 Click、Requests、项目设置及唯一案例配置。`ARTIFACT_FILENAMES` 定义每个案例必须具备的三个文件名；此格只加载定义，不请求来源或写入数据。''', id='b01a-init-heading')
cells['raw-demo-title'].source = '''## raw 归档样例

仅在交互内核且没有 `__file__` 时调用共享 raw 浏览器。按配置案例展示文件是否存在、大小和已保存摘要；空库显示尚未归档。浏览器不读取响应正文、不校准数据、不请求来源或创建证据文件。

此处的文件概览不替代后面的完整证据验收。普通脚本运行或模块导入跳过展示。'''
cells['db16e4ab'].source = '''## 官方原文验收与冻结清单

`validate_response()` 先计算完整响应字节的 SHA-256，与配置精确比较；随后由 `parse_official_volume_rows()` 解析 UTF-8 JSON 的 `o_cursor` 列表，筛选目标合约的成交量榜。

| 检查 | 当前处理 |
| --- | --- |
| 目标合约与名次 | 合约代码去空白、转大写后匹配；其他合约跳过。非整数、布尔或不在 1—20 的名次跳过，随后要求最终结果恰好覆盖 1—20。 |
| 成员与指标 | 会员名非空；成交量为非负整数，增减量为整数；布尔值不能作为整数。 |
| 完整 Top 20 | 按名次排序后检查完整覆盖、成交量非递增，并逐项等于冻结官方值。 |
| 校准清单 | `calibration_manifest()` 汇集案例身份、交易日、来源 URL、实际摘要和已验证的全部官方行；不写文件。 |

完整响应摘要与目标 Top 20 是 raw 证据的核心验收条件。网络响应、staging 原文及正式原文均通过同一套函数验收；本轮保留全部检查。'''
verify_heading = nbformat.v4.new_markdown_cell('''## 三文件证据复读

`verify_artifacts()` 用于已有案例、staging 和正式安装后的验收。目录内容必须恰好为三个约定文件；先读取 `response.dat` 并重新执行原文验收，再核对 `response.sha256` 与实际摘要，最后比较 `calibration.json` 与从原文及配置生成的完整清单。

每次调用均直接读取当前路径的文件，验收通过才返回清单。文件概览、曾经下载成功或仅有文件名都不能替代这里的核对；任何异常继续向调用方抛出。''', id='b01a-verify-heading')
validation_source = cells['ad3d3d92'].source
split_at = validation_source.index('\n\ndef verify_artifacts(')
cells['ad3d3d92'].source = validation_source[:split_at]
verify_cell = nbformat.v4.new_code_cell(validation_source[split_at:].lstrip(), id='b01a-verify-artifacts')
cells['ca173bcd'].source = '''## CLI：逐案例请求、验收与归档

`main()` 保持原有执行顺序：确认案例路径位于 raw 根内；已有目录直接复读并跳过请求；缺失目录只请求一次，HTTP 200 且冻结验收通过后才决定是否写入。

写入时在目标目录旁创建本批唯一 staging，按原字节写 `response.dat`，以 LF 写摘要和 UTF-8 JSON 清单；复读 staging 后再次确认正式目录没有出现，再用 `os.replace()` 安装整个目录并正式复读。异常只清理 staging 并抛出，停止后续案例；当前代码尚无安装后的回滚。

日志使用 88 个 `=` 的运行边界、中文阶段说明与 `artifact/function/phase/status` 字段，`elapsed_s` 为本次入口调用的累计秒数；案例序号沿原循环计数，不额外扫描文件。保留 monitor 识别的 `special_case_existing_valid:`、`special_case_source_valid:` 和 `special_case_committed:` 前缀，分别表示已有证据验收、来源验收和本次正式提交完成。

本轮仅统一入口日志样式，读取、请求与提交仍位于原函数边界。历史结尾 `position_rank_special_cases_ready: true` 原样保留：当前只读下载也会输出它，因此它只能按“本轮检查完成”理解，不能单独作为正式 raw 已就绪的凭证；该状态语义在后续日志归位轮处理。异常记录失败阶段，不输出正常完成结尾。'''
cells['7566e6fc'].source = '''## 当前 Notebook 与脚本运行入口

当前 Notebook 分支显式传入 `args=[]`、`standalone_mode=False`，避开内核的 `-f` 参数。不带 `--write`；证据缺失时仍会请求官方来源，已有证据则只复读。阅读 Markdown 和流程图不会执行请求。

当前判断仅检查 `ipykernel` 是否已加载，所以 Notebook 中导入同名模块也可能触发入口。直接运行 `.py` 时读取 CLI 参数。此处描述当前执行方式；显式 `notebook_args` 与导入不执行的对齐留在后续第 11 项。'''

flows = {
    '3d5940bc': flow('b01a-flow-init', '### 局部流程：初始化', '''
flowchart TD
    A["当前目录及父目录"] --> B{"包含三个项目标记？"}
    B -->|是| C["加入项目根与湖仓模块路径"]
    B -->|否| D["抛出未找到项目根目录"]
    C --> E["导入依赖与冻结配置；定义三个文件名"]
'''),
    'raw-demo-preview': flow('b01a-flow-browser', '### 局部流程：raw 文件概览', '''
flowchart TD
    A{"交互内核且没有文件路径变量？"} -->|是| B["按配置案例展示文件状态、大小与摘要"]
    B --> C["不读正文、不联网、不写入"]
    A -->|否| D["跳过展示"]
'''),
    'ad3d3d92': flow('b01a-flow-response', '### 局部流程：原文验收与清单生成', '''
flowchart TD
    A["完整响应字节与冻结案例配置"] --> B["核对完整 SHA-256"]
    B --> C["解析 UTF-8 JSON；检查 o_cursor 列表及记录类型"]
    C --> D["筛选目标合约、1—20 名次；核对字段类型"]
    D --> E["排序；核对完整名次、非递增及冻结值"]
    E --> F["生成含身份、来源、摘要和全部官方行的清单"]
    B -. 不匹配 .-> X["抛出异常"]
    C -. 不合法 .-> X
    D -. 字段不合法 .-> X
    E -. 不匹配 .-> X
'''),
    'b01a-verify-artifacts': flow('b01a-flow-verify', '### 局部流程：三文件完整复读', '''
flowchart TD
    A["核对目录恰好包含三个证据文件"] --> B["读取 response.dat；重新验收完整原文"]
    B --> C["读取 sidecar；核对实际摘要"]
    C --> D["读取 calibration.json；核对完整清单"]
    D --> E["返回已验收清单"]
    A -. 失败 .-> X["抛错；不请求来源或修复文件"]
    B -. 失败 .-> X
    C -. 失败 .-> X
    D -. 失败 .-> X
'''),
    'e38b03b7': flow('b01a-flow-cli', '### 局部流程：入口分支与单案例安装', '''
flowchart TD
    A["报告运行开始；逐案例检查路径边界"] --> B{"目录存在？"}
    B -->|是| C["完整复读；报告 existing_valid"]
    B -->|否| D["请求一次；HTTP 200 后验收原文"]
    D --> E["报告 source_valid"]
    E --> F{"启用写入？"}
    F -->|否| G["报告跳过提交；不建 staging"]
    F -->|是| H["写三个 staging 文件；完整复读"]
    H --> I["确认目标仍不存在；整目录 os.replace"]
    I --> J["正式复读；报告 committed"]
    H -. 失败 .-> X["清理 staging；原异常继续抛出"]
    I -. 失败 .-> X
    J -. 失败 .-> X
    X --> Y["入口报告失败；已安装目录当前不回滚"]
    C --> K["继续下一案例；全部完成后报告运行结束"]
    G --> K
    J --> K
'''),
    '30c24e87': flow('b01a-flow-entry', '### 局部流程：当前执行入口', '''
flowchart TD
    A{"ipykernel 已加载？"} -->|是| B["main.main；空参数；standalone_mode=False"]
    B --> C["只读检查；证据缺失仍请求一次"]
    A -->|否| D{"直接运行脚本？"}
    D -->|是| E["main 读取命令行参数"]
    D -->|否| F["普通模块导入不运行入口"]
'''),
}
cells['3d5940bc'].source = cells['3d5940bc'].source.replace('import sys\n', 'import sys\nimport time\n')
source = cells['e38b03b7'].source


def replace(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new)


replace('''    click.echo(
        f"special_case_count={len(POSITION_RANK_SPECIAL_CASES)}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )''', '''    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    log_phase = "run"
    log_case_id = None
    log_case_index = 0
    click.echo(
        f"{log_boundary}\\n特殊案例校准证据检查开始 / Special-case evidence run started\\n"
        "planning_progress: artifact=position_rank_special_cases; function=main; phase=run; status=started; "
        f"special_case_count={len(POSITION_RANK_SPECIAL_CASES)}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )''')
replace('''    for special_case in POSITION_RANK_SPECIAL_CASES:
        artifact_path''', '''    for special_case in POSITION_RANK_SPECIAL_CASES:
        log_case_index += 1
        log_case_id = special_case["case_id"]
        log_phase = "case_path"
        click.echo(
            "检查案例目录 / Inspect case path\\n"
            "planning_progress: artifact=position_rank_special_cases; function=main; phase=case_path; status=started; "
            f"case_id={log_case_id}; case_index={log_case_index}/{len(POSITION_RANK_SPECIAL_CASES)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        artifact_path''')
replace('''        if artifact_path.exists():
            manifest''', '''        if artifact_path.exists():
            log_phase = "existing_verify"
            click.echo(
                "复读已有证据 / Verify existing evidence\\n"
                "planning_progress: artifact=position_rank_special_cases; function=main; phase=existing_verify; status=started; "
                f"case_id={log_case_id}; path={artifact_path}; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            manifest''')
replace('''                "special_case_existing_valid: "''', '''                "special_case_existing_valid: artifact=position_rank_special_cases; function=main; phase=existing_verify; status=completed; "''')
replace('''        response = requests.get''', '''        log_phase = "source_request"
        click.echo(
            "请求官方冻结来源 / Request official source\\n"
            "planning_progress: artifact=position_rank_special_cases; function=main; phase=source_request; status=started; "
            f"case_id={log_case_id}; timeout_s=60; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        response = requests.get''')
replace('''        manifest = validate_response(response.content, special_case)''', '''        log_phase = "source_validate"
        manifest = validate_response(response.content, special_case)''')
replace('''            "special_case_source_valid: "''', '''            "special_case_source_valid: artifact=position_rank_special_cases; function=main; phase=source_validate; status=completed; "''')
source = source.replace('f"sha256={manifest[\'official_response_sha256\']}"', 'f"sha256={manifest[\'official_response_sha256\']}; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
replace('''        if not write:
            continue''', '''        if not write:
            click.echo(
                "planning_progress: artifact=position_rank_special_cases; function=main; phase=commit; status=skipped; "
                f"case_id={log_case_id}; reason=read_only; write=false; "
                f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            continue''')
replace('''        artifact_path.parent.mkdir''', '''        log_phase = "staging_prepare"
        click.echo(
            "暂存案例证据 / Stage case evidence\\n"
            "planning_progress: artifact=position_rank_special_cases; function=main; phase=staging_prepare; status=started; "
            f"case_id={log_case_id}; files=3; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        artifact_path.parent.mkdir''')
replace('''            verify_artifacts(staging_path, special_case)''', '''            log_phase = "staging_verify"
            click.echo(
                "planning_progress: artifact=position_rank_special_cases; function=main; phase=staging_verify; status=started; "
                f"case_id={log_case_id}; path={staging_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            verify_artifacts(staging_path, special_case)
            log_phase = "install"
            click.echo(
                "安装案例目录 / Install case directory\\n"
                "planning_progress: artifact=position_rank_special_cases; function=main; phase=install; status=started; "
                f"case_id={log_case_id}; path={artifact_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )''')
replace('''            os.replace(staging_path, artifact_path)
            verify_artifacts''', '''            os.replace(staging_path, artifact_path)
            log_phase = "formal_verify"
            click.echo(
                "复读正式案例证据 / Verify installed evidence\\n"
                "planning_progress: artifact=position_rank_special_cases; function=main; phase=formal_verify; status=started; "
                f"case_id={log_case_id}; path={artifact_path}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            verify_artifacts''')
replace('''            "special_case_committed: "
            f"case_id={special_case['case_id']}; path={artifact_path}"''', '''            "special_case_committed: artifact=position_rank_special_cases; function=main; phase=commit; status=completed; "
            f"case_id={special_case['case_id']}; path={artifact_path}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"''')
start = source.index('    for special_case in POSITION_RANK_SPECIAL_CASES:')
body = source[start:]
source = source[:start] + '    try:\n' + textwrap.indent(body, '    ') + '''
        click.echo(
            f"{log_boundary}\\n特殊案例校准证据检查完成 / Special-case evidence run completed\\n"
            "planning_progress: artifact=position_rank_special_cases; function=main; phase=run; status=completed; "
            f"processed_cases={log_case_index}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )
    except Exception as log_error:
        click.echo(
            "planning_progress: artifact=position_rank_special_cases; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; case_id={log_case_id}; "
            f"case_index={log_case_index}/{len(POSITION_RANK_SPECIAL_CASES)}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise'''
cells['e38b03b7'].source = source

updated = []
for cell in notebook.cells:
    if cell.id == '3d5940bc':
        updated.append(init_heading)
    if cell.id in flows:
        updated.append(flows[cell.id])
    updated.append(cell)
    if cell.id == 'f8c42dd9':
        updated.extend([boundary, overview])
    if cell.id == 'ad3d3d92':
        updated.extend([verify_heading, flows[verify_cell.id], verify_cell])
notebook.cells = updated


class RemoveLogging(ast.NodeTransformer):
    def visit_Import(self, node):
        return None if len(node.names) == 1 and node.names[0].name == 'time' else node

    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and t.id.startswith('log_') for t in node.targets):
            return None
        return self.generic_visit(node)

    def visit_AugAssign(self, node):
        return None if isinstance(node.target, ast.Name) and node.target.id.startswith('log_') else self.generic_visit(node)

    def visit_Try(self, node):
        is_logging_wrapper = len(node.handlers) == 1 and node.handlers[0].name == 'log_error'
        node = self.generic_visit(node)
        return node.body if is_logging_wrapper else node


old_code = '\n\n'.join(c.source for c in before.cells if c.cell_type == 'code')
new_code = '\n\n'.join(c.source for c in notebook.cells if c.cell_type == 'code')
assert ast.dump(RemoveLogging().visit(ast.parse(old_code))) == ast.dump(RemoveLogging().visit(ast.parse(new_code)))
comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
assert comments(old_code) == comments(new_code)
assert notebook.metadata == before.metadata
newcells = {c.id: c for c in notebook.cells}
for old in before.cells:
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in newcells[old.id].items() if k != 'source'}
for index, cell in enumerate(notebook.cells):
    if cell.cell_type == 'code':
        compile(cell.source, str(PATH), 'exec')
        assert '```mermaid' in notebook.cells[index-1].source
nbformat.validate(notebook)
nbformat.write(notebook, PATH)
print('b01a docs/logs updated: 7 flowcharts; business AST, comments, existing cell IDs/metadata/outputs/counts preserved.')
