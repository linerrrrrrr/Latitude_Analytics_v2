"""境外期货：叶事务、入口与说明同步；Notebook 是唯一编辑源。"""
import ast
import copy
import hashlib
import json
import pathlib
import shutil
import tempfile

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT/'02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id: c for c in notebook.cells}
assert 'StagedPathTransaction' not in cells['b03-c03-03'].source
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b03-tx-before-'))
paths = [ROOT/'AGENTS.md', ROOT/'03_Futures_Database/AGENTS.md', ROOT/'config/data_contracts.py',
         *sorted((ROOT/'02_Futures_Lakehouse').rglob('*.md')),
         *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb')),
         *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.py')),
         *sorted((ROOT/'02_Futures_Lakehouse').glob('a00_*.py'))]
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}, indent=2), encoding='utf8')
for path in (PATH, PATH.with_suffix('.py'), ROOT/'AGENTS.md', ROOT/'02_Futures_Lakehouse/AGENTS.md', ROOT/'02_Futures_Lakehouse/README.md'):
    destination = snapshot/path.relative_to(ROOT)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, destination)

cells['b03-c03-03'].source = cells['b03-c03-03'].source.replace(
    'from config.settings import settings',
    'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')

# 保留原 staging、正式逐值验收和成功日志；只替换安装及恢复部分。
cell = cells['a03-b03-fact-commit']
source = cell.source
start = source.index('        saved_path = backup_path / relative_path')
formal_start = source.index('            # 正式路径复读成功是日历可以推进完成水位的前提。', start)
formal_end = source.index('        except Exception as commit_error:', formal_start)
formal = ''.join('    '+line if line.strip() else line for line in source[formal_start:formal_end].splitlines(keepends=True))
end = source.index('        click.echo(\n            f"partition_committed:', formal_end)
cell.source = source[:start]+'''        target_marker_path = target_path / "schema.parquet"
        staging_marker_path = staging_path / "schema.parquet"
        marker_created = not target_marker_path.exists()

        try:
            log_phase = "install"
            click.echo(
                f"planning_progress: dataset={DATASET_NAME}; function=commit_complete_fact_partition; phase=install; status=started; "
                f"scope=fact_leaf; target={destination_path}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"dataset={DATASET_NAME}; function=commit_complete_fact_partition; partition={partition_key}",
            ) as transaction:
                if marker_created:
                    transaction.replace(
                        target_path=target_marker_path, staged_path=staging_marker_path,
                        quarantine_new=False,
                    )
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path if complete_table.num_rows else None,
                )

'''+formal+'''        finally:
            # 进入事务前的异常也清理 staging；正式路径恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)

'''+source[end:]

cell = cells['a03-b03-calendar-commit']
source = cell.source
start = source.index('            saved_path = backup_path / relative_path')
formal_start = source.index('                log_phase = "formal_readback"', start)
formal_end = source.index('            except Exception as commit_error:', formal_start)
formal = ''.join('    '+line if line.strip() else line for line in source[formal_start:formal_end].splitlines(keepends=True))
end = source.index('            log_committed_partitions += 1', formal_end)
cell.source = source[:start]+'''
            try:
                log_phase = "install"
                click.echo(
                    f"planning_progress: dataset={DATASET_NAME}; function=commit_calendar_partitions; phase=install; status=started; "
                    f"scope=calendar_leaf; target={destination_path}; transaction_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
                with StagedPathTransaction(
                    root_path=target_path,
                    staging_dir=staging_path,
                    backup_dir=backup_path,
                    quarantine_dir=quarantine_path,
                    log_context=f"dataset={DATASET_NAME}; function=commit_calendar_partitions; partition={partition_key}",
                ) as transaction:
                    transaction.replace(target_path=destination_path, staged_path=source_path)

'''+formal+'''            finally:
                # 当前叶独立恢复；此前成功事实和日历叶保留。
                shutil.rmtree(staging_path, ignore_errors=True)
'''+source[end:]

cells['b03-c03-21'].source = cells['b03-c03-21'].source.replace(
    'if "ipykernel" in sys.modules:', 'if "ipykernel" in sys.modules and "__file__" not in globals():').replace(
    '# Notebook 默认执行正式湖自动 dry-run；测试写入必须显式使用非正式湖。',
    '# Notebook 默认不写入；有待办仍查询 API；显式日期写入必须使用非正式湖。').replace(
    '    main.main(', '    notebook_args = []\n    main.main(').replace('args=[]', 'args=notebook_args')

descriptions = {
'b03-c03-08': '''## Dataset 物理契约与表身份读取

`open_exact_dataset()` 检查 Dataset 与各 Parquet fragment 的字段、类型、nullable，以及 `table_name/primary_key/partition_columns` 身份 metadata。Hive 分区字段按权威列序补回。纯描述性 metadata 差异以当前 `config/data_contracts.py` 为准，不触发历史文件重写。

启动时只打开一次日历和可选事实；提交复读直接定位当前完整叶，使用 `partition_base_dir` 补回 Hive 字段。空事实通过根级零行标记复读。函数报告结构读取、fragment 进度和失败阶段；`materialized=false` 表示尚未读取数据行。''',
'a03-b03-calendar-validation-text': '''## dirty 日历叶业务验收

`validate_calendar_table()` 接收已按权威 Schema 转换的 Arrow 表，对待提交完整叶检查主键、状态枚举、年月、原因、计数、required 关系和审计时间，按主键排序返回。上游正式日历不重复业务验收；内存状态生成不再完整复核，统一由提交函数对 dirty 完整叶验收一次。''',
'a03-b03-read-fact-text': '''## 可选事实读取

事实目录不存在或没有 Parquet 时返回权威空表；否则检查物理契约、读取并按主键排序，信任生产者已经完成的业务验收。启动时仍读取正式事实，计算本入口完成判断所需的逐日期计数和 OHLC 旁证；状态修复后和批末不再重新读取全表。''',
'a03-b03-merge-text': '''## 完整月份事实合并

`full_fact_partition()` 消费当前年月旧事实叶，删除触达日期的旧行，再拼接本次已归一化响应；未触达日期原样保留。确认空日期不生成占位行，结果为空时返回权威空表。合并只生成内存结果，完整业务验收交给事实提交函数执行一次。

主循环前按年月建立事实和日历叶映射。每月只处理当前叶，不对全历史逐分区做布尔扫描，也不逐月重建整张事实表。''',
'b03-c03-14': '''## staging 事实分区表达式

`fact_partition_expression()` 按权威分区顺序构造当前年月过滤条件。事实 staging 只包含一个完整叶与零行契约标记；该过滤选择当前叶数据。正式复读直接打开目标叶，不扫描正式表根。描述性 metadata 的整根迁移分支已移除。''',
'a03-b03-fact-commit-text': '''## 完整事实叶与标记共同提交

当前完整叶执行业务验收后写 staging 和零行标记，再按物理契约复读、按主键排序并逐值比较。每个事实叶使用一个 `StagedPathTransaction`：已有根标记保留；缺失的零行标记与当前叶纳入同一恢复范围。空结果以删除当前旧叶表达。

正式复读及完整内容比较在事务内完成；非空时直读目标叶，空结果确认叶不存在并复读零行标记。共享模块只负责安装和恢复，业务验收仍在本函数中。首次备份失败保留原目标；安装或验收失败恢复当前旧叶、删除本批新增标记。已安装的失败新叶保存在 `.failed-<run_id>`，恢复不完整时另保留 `.backup-<run_id>`；staging 清理。

成功退出事务后才报告事实已持久化，`calendar_state=not_updated` 表明日历尚未提交。此前成功事实叶保留。''',
'b03-c03-16': '''## 日历完成与失败状态生成

`apply_calendar_completion()` 依据正式事实计数与 OHLC 结论生成成功或确认空状态，补齐批次、完成和质检时间。`apply_calendar_failure()` 保留已有正式事实计数，将失败日期设为未完成并记录原因。

两者只生成内存状态，完整业务验收由 dirty 日历叶提交承担。常规月循环传入当前叶；批前无 API 修复复用一次规划得到的事实证据。请求或转换失败只回写失败日期，不提交当前尚未完成月份的事实；此前成功月份保留。''',
'a03-b03-calendar-commit-text': '''## 日历完整叶独立提交

触达日期定位 `dataset_name/year/month` 完整叶；分组索引、列名、排序键和根路径在循环前准备。同叶未触达行保留，dirty 完整叶只执行业务验收一次；staging 与正式目标均检查物理契约并逐值比较，不再往返转换或重复业务验收。

每个日历叶独立使用共享事务，正式复读在事务内进行。失败只恢复当前叶，此前成功事实和日历叶保留；失败新叶隔离保留，恢复不完整另保留旧备份，staging 清理。已有日历根标记不替换。

日历失败不撤销已提交事实，下次运行可无 API 修复。成功日志在退出事务后发出；全部触达叶成功后报告触达数、完成数与 `date_watermark=none`。错误状态成功落盘不表示采集完成。本实现不提供跨表原子可见性、进程终止后的自动恢复或并发写入协调。''',
'b03-c03-18': '''## CLI：一次规划、月份推进与运行日志

`main()` 先检查日期参数与正式写入边界，读取本数据集日历和可选事实，一次计算已完成、无 API 修复和 API 待办集合，并复用计数及 OHLC 证据。纯状态修复不认证 JQData；有待办才连接，按年月逐日查询。

每月汇总已归一化响应、合并完整事实叶，先提交事实并取得正式计数。完整内容已逐值一致，因此沿用本批来源 OHLC 结论生成日历，再独立提交日历叶；只读模式仅汇总内存结果。修复后与批末不再全表求差或业务复验，完成证据来自各提交函数的正式逐值复读。

日志使用 `function/phase/status/elapsed_s`，批次起止有 `=` 分隔线。读取、查询、归一化、质量、合并、生成与提交由所属函数报告；main 负责参数、批次和月份进度。内存结果标记 `persisted=false`，月份成功只有在事实和日历均提交后报告；失败保留阶段、月份、日期与原因链。''',
'b03-c03-20': '''## Notebook 与脚本执行入口

与 a01/b01、b02 一样，用显式 `notebook_args` 和 `standalone_mode=False` 执行 Notebook；默认 `[]` 不写湖，但有 API 待办时仍认证并查询 JQData。交互分支同时要求没有 `__file__`，在内核中导入同名 Python 模块不会触发业务；直接运行 `.py` 时读取终端参数。

最后一格仅保存手动终端命令注释，运行全部单元格不会额外启动正式写入。正式写入使用 `--write`，不得用显式日期截断自动范围。''',
}
for cell_id, source in descriptions.items():
    cells[cell_id].source = source
cells['b03-c03-02'].source += '\n\n湖仓级 `a00_04_staged_path_transaction.py` 只负责 staging 路径安装与失败恢复，更新范围、事实合并、质量验收和事务边界仍由本环节决定。'
cells['b03-c03-12'].source += '\n\n规划同时返回本次已计算的事实计数与 warning 映射，供修复和失败回写复用；正式逐值验收成功后不重新做全表规划。'
cells['a03-b03-quality-text'].source += '\n\n直接读取已契约化事实的标量记录，不为旁证再构造 Arrow 表。'
cells['a03-b03-fact-validation-text'].source += '\n\n该验收用于来源归一化结果和提交前 dirty 完整叶；staging 与正式复读只核对物理契约和完整内容。转换前有限数检查保留，避免残留 NaN 在 Pandas 转 Arrow 时被误当成合法 null。'

def flow(title, body, *, overall=False):
    heading = '## 总流程：' if overall else '### 局部流程：'
    return heading+title+'\n\n```mermaid\n%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\nflowchart TD\n'+body+'\n```'

graphs = {
'overview': ('日期证据与独立叶事务', '''    A["参数门禁；读取正式日历与可选事实"] --> B["一次规划；计算计数和 OHLC 证据"]
    B --> C["已完成跳过；陈旧状态按 write 无 API 修复"]
    C --> D{"还有 API 待办？"}
    D -->|否| Z["报告结果；结束"]
    D -->|是| E["认证；按月逐日查询与归一化"]
    E --> F{"启用 write？"}
    F -->|否| G["累计内存结果；继续下一月"]
    F -->|是| H["合并当前完整事实叶；校验与 staging 逐值复读"]
    H --> I["事实共享事务：当前叶与新增标记；正式逐值验收"]
    I --> J["正式计数与已验证来源质量；生成日历状态"]
    J --> K["日历独立叶事务；正式逐值验收"]
    K --> G
    G --> Z
    I -. 失败 .-> R["恢复当前目标；保留失败证据；停止"]
    K -. 失败 .-> R
    R --> S["此前成功事实保留；下次可无 API 修复日历"]'''),
'09': ('物理契约与表身份', '''    A["表根、叶目录或零行标记"] --> B["打开 Dataset；恢复 Hive 字段"]
    B --> C["检查物理字段、类型、nullable 和身份 metadata"]
    C --> D["逐 fragment 检查同一物理契约"]
    D --> E["返回 Dataset；描述性差异采用当前契约"]'''),
'calendar-validation': ('dirty 日历叶验收', '''    A["已按 Schema 转换的 dirty Arrow 叶"] --> B["主键唯一性；枚举和日期关系"]
    B --> C["数量、required、完成状态和审计时间"]
    C --> D["按主键排序；返回已验收 Arrow 叶"]'''),
'quality': ('OHLC 来源旁证', '''    A["契约化事实标量记录"] --> B["检查有限 OHLC 跨列关系"]
    B --> C["按日期汇总异常代码与来源原值"]
    C --> D["返回 warning 映射；不改写事实"]'''),
'read-fact': ('可选正式事实', '''    A{"有正式 Parquet？"} -->|否| B["权威空事实表"]
    A -->|是| C["物理契约与身份检查"]
    C --> D["一次物化并排序；信任已提交业务证明"]
    B --> E["返回事实供一次规划使用"]
    D --> E'''),
'merge': ('当前月份合并', '''    A["当前事实叶和本批已归一化响应"] --> B["保留未触达日期；移除触达日期旧行"]
    B --> C["拼接本批新行；全空则用权威空表"]
    C --> D["返回内存完整叶；提交函数统一验收"]'''),
'15': ('staging 分区表达式', '''    A["权威分区列与年月值"] --> B["构造 Arrow 分区相等条件"]
    B --> C["只选择 staging 当前叶；排除零行标记"]'''),
'fact-commit': ('事实叶与标记共享事务', '''    A["dirty 完整叶业务验收"] --> B["staging 写入；物理契约和逐值复读"]
    B --> C["共享事务：需要时安装标记；替换或删除当前叶"]
    C --> D["事务内直读正式叶或零行标记；逐值比较"]
    D --> E["退出成功；报告事实已提交；日历尚未更新"]
    C -. 失败 .-> R["按实际移动记录恢复；删除新增标记"]
    D -. 失败 .-> R
    R --> S["失败新叶隔离；恢复不全保留备份；抛错"]'''),
'17': ('日历内存状态', '''    A{"已提交事实证据或请求失败？"} -->|事实证据| B["按计数和 warning 生成成功或确认空"]
    A -->|失败| C["保留当前事实计数；记错误且未完成"]
    B --> D["返回内存日历；未持久化"]
    C --> D
    D --> E["后续 dirty 完整叶提交承担业务验收"]'''),
'calendar-commit': ('日历独立叶事务', '''    A["触达日期定位完整叶；循环前建立分组索引"] --> B["当前 dirty 叶业务验收一次"]
    B --> C["staging 写入；物理契约和逐值复读"]
    C --> D["当前叶共享事务；安装并正式逐值验收"]
    D --> E["退出成功；报告叶完成；继续下一叶"]
    E --> F["全部触达叶成功；报告完成数；无独立水位"]
    D -. 失败 .-> R["恢复当前旧叶；保留失败现场；停止"]
    R --> S["此前事实和成功日历叶保留"]'''),
'19': ('CLI 与月份推进', '''    A["参数门禁；读取；一次规划"] --> B["按 write 决定是否修复陈旧日历"]
    B --> C{"有 API 待办？"}
    C -->|否| Z["最终结果"]
    C -->|是| D["认证；循环前准备叶映射"]
    D --> E["当前月逐日查询、归一化、质量旁证"]
    E --> F{"启用 write？"}
    F -->|否| G["累计内存结果"]
    F -->|是| H["合并并提交事实叶；正式逐值复读"]
    H --> I["正式计数与来源质量；生成并提交日历叶"]
    I --> G
    G --> J{"还有月份？"}
    J -->|是| E
    J -->|否| Z
    E -. 失败 .-> R["write 时只回写失败日期；停止"]
    H -. 失败 .-> S["恢复当前事务；停止"]
    I -. 失败 .-> S'''),
'21': ('Notebook 与脚本入口', '''    A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；默认不写入"]
    A -->|否| C{"直接执行脚本？"}
    C -->|是| D["Click 读取终端参数并运行"]
    C -->|否| E["模块导入不运行"]'''),
}
for suffix, (title, body) in graphs.items():
    cells['a03-b03-flow-'+suffix].source = flow(title, body, overall=suffix=='overview')
notebook.cells.append(nbformat.v4.new_markdown_cell(flow('终端手动运行', '''    A["在终端激活 latitude；切换项目根目录"] --> B["人工执行对应 Python 脚本 --write"]
    B --> C["自动计算待办；独立提交事实和日历叶"]'''), id='a03-b03-flow-manual'))
notebook.cells.append(nbformat.v4.new_code_cell('''# conda env list
# conda activate latitude
# cd E:\\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\\a03_External_Market_Data\\b03_overseas_futures.py --write''', id='a03-b03-manual'))
assert notebook.metadata == before.metadata
for old in before.cells:
    assert {k:v for k,v in old.items() if k!='source'} == {k:v for k,v in cells[old.id].items() if k!='source'}
for cell in notebook.cells:
    if cell.cell_type=='code': ast.parse(cell.source)
nbformat.validate(notebook)
PATH.write_text(nbformat.writes(notebook)+'\n', encoding='utf8', newline='\n')

boundary = ('a03/b03 对每个事实完整叶及本批新增零行标记使用同一个共享事务；每个日历完整叶独立使用共享事务，'
            '事实先提交、日历随后回写，日历失败不撤销已提交事实，下次从正式证据无 API 修复。'
            'dirty 完整叶业务验收一次，staging 与正式路径继续物理契约检查和逐值比较；正式验收在事务内，逐叶直读不扫描正式表根。'
            '启动时保留契约要求的事实计数和 OHLC 状态核对，一次规划复用证据；修复后与批末不再全表复验，描述性 metadata 差异不重写历史。'
            '首次备份失败保留原目标，失败新叶隔离留存，恢复不完整另保留旧备份，staging 清理；无独立日期水位。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT/relative
    text = path.read_text(encoding='utf8')
    members = 'a01/b01、b02、b03、b04、b05、b06、b07、b08、a02/b01、b01a、b02、b03 与 a03/b01、b02'
    assert members in text
    text = text.replace(members, members+'、b03')
    if relative!='AGENTS.md':
        anchor = '已有日历根标记保持原样，没有独立日期水位。'
        assert text.count(anchor)==1
        text = text.replace(anchor, anchor+boundary)
    path.write_text(text, encoding='utf8', newline='\n')
print(snapshot)
