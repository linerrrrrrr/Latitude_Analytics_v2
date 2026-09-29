"""同步 a04/b01 说明、日历下游兼容入口和既有测试；不执行采集。"""
import ast
import dataclasses
import pathlib
import shutil
import sys
import textwrap

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
path = ROOT / '02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb'
notebook = nbformat.read(path, 4)
cells = {cell.id: cell for cell in notebook.cells}
cells['a2e386f4'].source = cells['a2e386f4'].source.replace('后续函数分别判断物理兼容和 metadata 精确一致。', '后续读取函数判断物理兼容、表身份和契约版本。')

cells['ae2ed8cd'].source = cells['ae2ed8cd'].source.replace('现有 metadata 迁移分支', '旧契约版本迁移分支')
start = cells['ae2ed8cd'].source.index('当前 `main()`')
end = cells['ae2ed8cd'].source.index('相关材料：', start)
cells['ae2ed8cd'].source = cells['ae2ed8cd'].source[:start] + '''当前契约版本信任既有正式状态；生成时按三个政策字段逐格点比较，政策相同才继承 `STATE_COLUMNS`（包括原 `updated_at`）。来源配置变化、可用日到达等使政策不同的行使用新初始化状态，不先把配置变化判作历史文件损坏。

物理兼容但 `schema_version` 不同的历史进入旧契约路径：只做一次当前业务规则识别，能够通过则保留逐格点继承，不能通过则不继承旧表状态、完整重新生成。旧策略不兼容的显式日期检查仍拒绝；旧契约版本的写入迁移必须使用无日期自动模式。

表名、主键、分区和契约版本与描述性 metadata 分开处理。纯说明文字变化以当前 Schema 为准，不形成分区差异或触发历史重写；b02/b03 对本日历使用相同读取边界。新写入文件携带完整当前 metadata。

''' + cells['ae2ed8cd'].source[end:]
cells['a04-b01-doc-compatible'].source = '''## 物理结构、表身份与契约版本读取

`open_compatible_dataset()` 确认存在 Parquet 后打开 Hive Dataset。在同一次 fragment 遍历中确认字段顺序、类型、nullable、表名、主键和分区身份，并判断是否都是当前 `schema_version`；缺失身份或版本、物理不兼容直接拒绝。

描述性 metadata 不参与历史重写决策，以当前 Schema 解释。返回 Dataset 与契约版本是否当前的标志，不物化记录。函数报告文件数、检查进度和 `materialized=false`；实际记录读取留在调用方。
'''
cells['a04-b01-doc-exact'].source = '''## 当前契约读取

`open_exact_dataset()` 在完整物理和表身份检查后，要求所有文件均为当前契约版本，供 staging 与正式安装复读使用。这里的精确约束是物理结构、表身份及版本，不要求历史说明文字逐字相同。新文件仍由具名 Schema 写入完整当前 metadata。
'''
cells['2dc99ad7'].source = '''## 生成结果的单次业务验收

`validate_macro_calendar_table()` 接收已经按权威 Schema 转换的 Arrow 表，检查主键、数据集和状态枚举、年月、计数、审计时间、共享系列、理论频率、项目可用日、版本化原因及状态组合。它只做本日历业务验收，不再重复调用转换契约；返回按主键排序的 Pandas 表。

普通路径只在生成结果上执行一次完整业务校验。范围外旧行继承正式提交证明，日期范围互斥后合并；提交、staging 和正式复读不再重复业务规则。只有旧契约版本的兼容识别会额外执行一次当前规则检查，决定是否能继承旧状态。

时间字段列表、审计字段列表和完成状态集合移到行循环前。进度利用既有遍历计数，每 10000 行检查一次 2 秒间隔；业务失败仍报具体上下文。
'''
cells['05826f25'].source = '''## 理论格点生成与状态继承

旧行直接转为记录字典并按主键索引，不再先做一次 Pandas—Arrow 往返。普通工作日、月末和季末三个日期序列在系列循环前各生成一次，25 个系列按频率复用。

每行计算项目可用日和 required，初始化 pending 或 not_required。旧行存在且三个政策字段逐值相同，才继承全部状态及原更新时间；其余保留新状态。共享配置变动只通过这些政策字段影响继承，不额外发明采集范围。

生成完成后仅转换一次权威 Arrow 并执行一次完整业务验收。函数自行报告逐系列、累计生成/继承/初始化行数，均为 `persisted=false`。旧契约不能通过当前规则识别时，main 传入空继承表；这一旧契约路径仍会整体重置。
'''
cells['1bece26c'].source = '''## 完整内容摘要

`table_digest()` 接收已经准备好的 Arrow 表，按主键排序并用权威 Schema 重建稳定行表示，再计算 Arrow IPC 的 SHA-256。摘要覆盖全部字段值，保留用于消除切片/缓冲区表示差异的稳定重建；删除了每次求摘要前重复的 Pandas→Arrow 转换。

期望、旧数据和复读数据都按当前 Schema 形成同一内容表示，因此纯描述性 metadata 差异不构成内容变化。
'''
cells['a04-b01-doc-changed'].source = '''## 一次分组后比较完整叶

`changed_partition_keys()` 为期望和旧表各建立一次分区位置索引、各转换一次 Arrow 表，然后遍历分区键并集。新出现或被撤销的叶、行数不同的叶直接记为变化，其余只对当前叶取行并比较完整内容摘要。

分区循环不再对两张整表逐列构造布尔掩码，也不重复转换整表。函数用既有循环报告当前键、序号和变化数，保留约 2 秒一次的进度节奏。
'''
cells['62335bcd'].source = '''## 分组复用、单次 staging 物化与共同恢复

完整期望已经由生成结果与不相交范围外可信旧行组成。`commit_partitions()` 排序并转换一次 Arrow，循环前准备分区位置映射、叶路径、字段列表和空表；这些对象供选取变化行、摘要、staging 检查与安装共同复用，不重新验收全表业务。

staging 写入后只物化一次完整表，按分区窄列建立索引，在内存逐叶核对行数和完整内容摘要；不再逐叶调用 `to_table()`，也不逐叶扫描完整期望表。强制旧契约迁移仍核对完整 staging 摘要。

普通更新按本批全部变化叶依次备份、安装或删除，保留未触达叶和既有根标记；新表、完整期望为空或旧契约迁移使用整根交换。安装后的正式整表只复读一次，检查物理契约、总行数和完整内容摘要。这一次完整验收仍是本日历全量比较契约的一部分，未改为逐叶最终确认。

现有手写恢复范围不变，尚未接入共享模块：恢复完整时清理 staging、备份和失败隔离目录，恢复不完整保留现场。一处恢复错误仍会停止其余恢复，后续第 10 项处理。安装阶段标为待验收；正式复读及现有清理完成后才报告 `calendar_state=committed; date_watermark=none; persisted=true`，不代表下游事实已完成。
'''
cells['2dcd351e'].source = '''## CLI：可信状态、旧契约识别和完整计划

main 保留日期配对、顺序、正式湖写入门禁及完整理论范围。读取时确认物理结构、表身份和契约版本，再物化一次正式日历；当前版本不重验既有正式业务状态。只有旧版本才做一次当前规则识别，决定能否继承，写入迁移仍限无日期自动模式。

显式范围将已验收的范围内生成结果与范围外可信旧行拼接，不重复全表业务校验。新增、撤销和政策重置统计直接使用行字典，不再为了计数反复转 Arrow；`policy_resets` 仍仅表示三个政策字段发生变化的交集主键数。

没有变化且表存在则报告 up_to_date；其余只读计划或调用提交。读取、生成、比较和提交函数报告自己的工作，main 只保留自己执行的物化、范围合并、计划汇总和批次结果。每个函数耗时以该次调用为起点，失败继续传播原异常。
'''

graphs = {
    'a04-b01-flow-a04-b01-code-compatible': '''A["发现 Parquet；打开 Hive Dataset"] --> B["检查 Dataset 物理结构和表身份"]
B --> C["单次遍历 fragment：物理结构、身份及契约版本"]
C --> D["返回 Dataset 和版本标志；不物化记录"]
B -. 不兼容 .-> X["拒绝；报告失败阶段"]
C -. 缺少身份或版本 .-> X''',
    'a04-b01-flow-a04-b01-code-exact': '''A["物理兼容与表身份读取"] --> B{"全部属于当前契约版本？"}
B -->|否| X["拒绝；交由自动模式迁移"]
B -->|是| C["返回 Dataset；描述文字采用当前 Schema"]''',
    'a04-b01-flow-ebbfdc80': '''A["已完成权威转换的 Arrow 表"] --> B["主键与逐行业务规则验收"]
B --> C["检查配置频率、可用日、原因及状态组合"]
C --> D["按主键排序返回 Pandas；不提交"]
B -. 失败 .-> X["记录上下文与进度；抛错"]
C -. 失败 .-> X''',
    'a04-b01-flow-d9a85e0d': '''A["旧行直接按主键索引"] --> B["循环前生成三种频率的日期序列"]
B --> C["按系列取日期；计算可用日和初始状态"]
C --> D{"旧行存在且政策逐值相同？"}
D -->|是| E["继承全部状态和旧更新时间"]
D -->|否| F["保留新状态"]
E --> G["汇总全部行；一次权威转换和业务验收"]
F --> G
G --> H["返回内存结果；报告生成进度"]''',
    'a04-b01-flow-db0b5fa0': '''A["已准备的 Arrow 表"] --> B["按主键排序；当前 Schema 重建稳定表示"]
B --> C["完整字段值写 IPC 并计算 SHA-256"]
C --> D["返回内容摘要；不重复 Pandas 转换"]''',
    'a04-b01-flow-a04-b01-code-changed': '''A["两表各分组一次并各准备一次 Arrow"] --> B["遍历分区键并集；查位置索引"]
B --> C{"缺少一侧或行数不同？"}
C -->|是| E["记录变化键"]
C -->|否| D["仅取当前叶并比较完整摘要"]
D --> H{"摘要相同？"}
H -->|否| E
H -->|是| F["跳过该叶"]
E --> G["遍历所有分区；报告进度并返回变化键"]
F --> G''',
    'a04-b01-flow-f4c3582d': '''A["完整期望排序转 Arrow；预建分区及路径映射"] --> B["选取变化行；写 staging"]
B --> C["staging 一次物化；内存逐叶核对完整摘要"]
C --> D{"新表、空表或旧契约迁移？"}
D -->|是| E["整根备份与安装"]
D -->|否| F["依次安装或删除全部变化叶"]
E --> G["正式整表复读一次：物理契约、行数、完整摘要"]
F --> G
G --> H["清理结束后报告日历已提交；无独立水位文件"]
E -. 失败 .-> R["原手写共同恢复；完整则清理，不完整保留现场"]
F -. 失败 .-> R
G -. 失败 .-> R
B -. 失败 .-> X["清理 staging；抛错"]
C -. 失败 .-> X''',
}
for cell_id, graph in graphs.items():
    title = cells[cell_id].source.split('```mermaid')[0]
    cells[cell_id].source = title + '```mermaid\nflowchart TD\n' + graph + '\n```\n'
cells['a04-b01-flow-cd6a9f44'].source = cells['a04-b01-flow-cd6a9f44'].source.replace('读取旧表；识别旧 metadata 和旧策略', '读取可信旧表；仅旧契约识别兼容策略')
cells['a04-b01-flow-overview'].source = cells['a04-b01-flow-overview'].source.replace('J --> K[', 'J -. 下游另行运行 .-> K[')
nbformat.validate(notebook)
path.write_text(nbformat.writes(notebook) + '\n', encoding='utf8', newline='\n')

# 下游仅调整本日历的 metadata 读取边界；两张事实表仍保留原精确检查。
for name in ('b02_interest_rate', 'b03_macro_release'):
    relative = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates') / (name + '.ipynb')
    for source_relative in (relative, relative.with_suffix('.py')):
        target = SNAPSHOT / source_relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copyfile(ROOT / source_relative, target)
    nb = nbformat.read(ROOT / relative, 4)
    cell = next(cell for cell in nb.cells if 'def dataset_has_exact_schema_metadata(' in cell.source)
    function = next(node for node in ast.parse(cell.source).body if isinstance(node, ast.FunctionDef) and node.name == 'dataset_has_exact_schema_metadata')
    first = function.body[0]
    addition = '''
if schema is MACRO_RELEASE_CALENDAR_SCHEMA:
    # 日历生产者不因描述文字变化重写历史；消费端按同一物理、身份和版本边界读取。
    actual_schema = reconstructed_schema(dataset, schema)
    identity_keys = (b"table_name", b"primary_key", b"partition_columns", b"schema_version")
    if not physical_schema_matches(actual_schema, schema) or any(
        (actual_schema.metadata or {}).get(key) != schema.metadata[key]
        for key in identity_keys
    ):
        return False
    expected_file_schema = pa.schema([field for field in schema if field.name not in partition_columns])
    return all(
        physical_schema_matches(fragment.physical_schema, expected_file_schema)
        and all((fragment.physical_schema.metadata or {}).get(key) == schema.metadata[key] for key in identity_keys)
        for fragment in dataset.get_fragments()
    )
'''
    lines = cell.source.splitlines(keepends=True)
    lines[first.lineno - 1:first.lineno - 1] = [textwrap.indent(textwrap.dedent(addition).strip() + '\n', '    ')]
    cell.source = ''.join(lines)
    for md in nb.cells:
        if md.cell_type == 'markdown' and md.source.startswith('## 契约化 Dataset 读取'):
            fact_label = 'SHIBOR 事实' if name == 'b02_interest_rate' else '宏观事实'
            md.source = '## 契约化 Dataset 读取\n\n宏观日历逐 fragment 检查物理字段、顺序、类型、nullable、表名、主键、分区和当前契约版本。描述性 metadata 采用当前 Schema，不要求 b01 因说明文字变化重写历史；旧契约版本仍须先由 b01 自动迁移。\n\n' + fact_label + '只在无日期自动模式下允许读取物理兼容但 metadata 过期的旧表，随后整根 staging 升级；事实表的普通读取、staging 和正式复读继续逐 fragment 精确匹配原有全部 metadata。\n'
            break
    nbformat.validate(nb)
    (ROOT / relative).write_text(nbformat.writes(nb) + '\n', encoding='utf8', newline='\n')

# 对应的既有回归采用真实配置变动，不再把当前版本的原因变化预设为文件损坏。
test_path = ROOT / '00_draft_collection_02/tests/test_b04_c01_macro_release_calendar.py'
source = test_path.read_text(encoding='utf8').replace('import pathlib\n', 'import pathlib\nfrom dataclasses import replace\n')
tree = ast.parse(source)
calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call) and ast.unparse(node.func) == 'self.module.table_digest']
digest_replacements = []
for node in calls:
    old = ast.get_source_segment(source, node)
    assert len(node.args) == 1
    new = 'self.module.table_digest(self.module.pandas_to_arrow(' + ast.unparse(node.args[0]) + ', self.module.MACRO_RELEASE_CALENDAR_SCHEMA))'
    digest_replacements.append((old, new))
for old, new in set(digest_replacements):
    source = source.replace(old, new)
tree = ast.parse(source)
node = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == 'test_current_metadata_policy_damage_is_not_silently_migrated')
replacement = '''
def test_current_contract_configuration_change_resets_only_changed_policy(self) -> None:
    current = self.build_frame()
    completed_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    observation_date = date(2026, 8, 17)
    completed = current["report_date"].eq(observation_date) & current["series_code"].isin(["SHIBOR_ON", "SHIBOR_1W"])
    self.assertEqual(int(completed.sum()), 2)
    for column, value in {
        "is_fetch_completed": True, "fetch_result_status": "success", "actual_record_count": 1,
        "quality_status": "passed", "quality_reason": "既有正式事实已复读。", "fetch_run_id": "existing-complete",
        "fetch_completed_at": completed_at, "quality_checked_at": completed_at, "updated_at": completed_at,
    }.items():
        current.loc[completed, column] = value
    revised_series = tuple(
        replace(series, series_name_zh=series.series_name_zh + "（配置修订）")
        if series.series_code == "SHIBOR_ON" else series
        for series in self.module.MACRO_RELEASE_SERIES
    )
    with tempfile.TemporaryDirectory(prefix="macro-policy-revision-") as directory:
        lake_root = pathlib.Path(directory)
        target_path = lake_root / "silver" / self.module.TABLE_NAME
        write_partitioned_calendar(self.module, current, target_path)
        with (
            mock.patch.object(self.module, "settings", types.SimpleNamespace(futures_lake_root=lake_root, futures_data_start_date=date(2026, 7, 17))),
            mock.patch.object(self.module, "MACRO_RELEASE_SERIES", revised_series),
            mock.patch.object(self.module, "MACRO_RELEASE_SERIES_BY_KEY", {(series.dataset_name, series.series_code): series for series in revised_series}),
        ):
            result = CliRunner().invoke(self.module.main, ("--write",))
            self.assertEqual(result.exit_code, 0, msg=str(result.exception))
            dataset = self.module.open_exact_dataset(target_path, "配置变化后日历")
            frame = self.module.validate_macro_calendar_table(dataset.to_table(columns=self.module.MACRO_RELEASE_CALENDAR_SCHEMA.names), "配置变化后")
        rows = frame.loc[frame["report_date"].eq(observation_date)].set_index("series_code")
        self.assertEqual(rows.loc["SHIBOR_ON", "fetch_result_status"], "pending")
        self.assertTrue(pd.isna(rows.loc["SHIBOR_ON", "fetch_run_id"]))
        self.assertEqual(rows.loc["SHIBOR_1W", "fetch_result_status"], "success")
        self.assertEqual(rows.loc["SHIBOR_1W", "fetch_run_id"], "existing-complete")
        self.assertFalse(recovery_paths(lake_root / "silver", self.module.TABLE_NAME))
'''
lines = source.splitlines(keepends=True)
lines[node.lineno - 1:node.end_lineno] = [textwrap.indent(textwrap.dedent(replacement).strip() + '\n', '    ')]
source = ''.join(lines)
ast.parse(source)
test_path.write_text(source, encoding='utf8', newline='\n')

def edit_text(relative, old, new):
    target = ROOT / relative
    source = target.read_text(encoding='utf8')
    assert source.count(old) == 1, (relative, old)
    target.write_text(source.replace(old, new), encoding='utf8', newline='\n')

edit_text('03_Futures_Database/AGENTS.md', '''`dataset_name/year/month` 叶分区，旧 metadata 或旧策略只能在无日期自动模式中整根迁移；staging 与
正式路径均须通过精确 Schema/metadata、完整叶和完整表逐值复读。该入口本身不得把项目规则可用日描述成''', '''`dataset_name/year/month` 叶分区。当前契约版本的既有状态信任正式提交证明，配置政策变化由生成时逐格点比较处理；
只有物理兼容但 `schema_version` 不同的历史才执行一次当前规则兼容识别，旧版本写入迁移必须使用无日期自动模式，
不能通过当前规则时不继承旧状态。生成结果执行一次完整业务验收；staging 一次物化后在内存逐叶核对，正式整表复读一次，
均保留物理结构、表身份、契约版本、行数和完整内容摘要验收。纯描述性 metadata 差异不形成迁移或历史重写，
b02/b03 消费本日历时采用同一兼容边界，新写文件仍携带当前完整 metadata。该入口本身不得把项目规则可用日描述成''')
edit_text('config/data_contracts.py', '规则变化重置对应下游状态且不得静默继承；staging 与正式路径均须通过精确 Schema/metadata、完整叶分区和完整表逐值复读。', '规则变化重置对应下游状态且不得静默继承；当前契约正式状态可信，生成结果执行一次完整业务验收；staging 一次物化后核对完整叶，正式整表复读一次，检查物理结构、表身份、契约版本、行数和完整内容摘要；纯描述性 metadata 差异采用当前契约说明，不触发历史重写。')
edit_text('02_Futures_Lakehouse/README.md', '并以完整 `dataset_name/year/month` 叶分区提交和正式复读。a04/b02', '并以完整 `dataset_name/year/month` 叶分区提交和正式复读。当前版本信任正式旧状态，生成结果完整业务验收一次；先分组复用 Arrow，staging 物化一次后在内存逐叶核对，正式整表只复读一次。仅旧契约版本进入兼容识别与自动迁移，纯描述性 metadata 差异不重写历史；b02/b03 的日历读取同步采用物理结构、表身份及契约版本边界。a04/b02')
edit_text('02_Futures_Lakehouse/AGENTS.md', '- `a01/b08` 的全量审计边界固定为', '- `a04/b01` 生成完整理论格点并只对生成结果做一次业务验收；当前契约版本的旧状态信任正式提交证明，政策变化按格点比较。旧契约版本才执行一次当前规则识别，写入迁移限无日期自动模式，无法通过时不继承旧状态。期望与旧表按分区分组并复用 Arrow，staging 一次物化后内存逐叶核对，正式整表仍复读一次以检查物理结构、表身份、契约版本、行数和完整内容摘要。纯描述性 metadata 差异不重写历史；a04/b02、b03 对该日历同步使用相同读取边界，两张事实表的既有验收规则不变。详细契约见 [数据库规则](../03_Futures_Database/AGENTS.md)。\n- `a01/b08` 的全量审计边界固定为')
print('notebook_docs_downstream_calendar_boundary_contracts_and_tests_synced')
