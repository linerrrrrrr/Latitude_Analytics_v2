"""保存本轮验证证据并检查最终变更范围。"""
import ast
import hashlib
import io
import json
from pathlib import Path
import shutil
import tokenize

import nbformat
from nbconvert.exporters import PythonExporter

root = Path(__file__).resolve().parents[2]
txn = Path(r'C:\Users\31918\AppData\Local\Temp\a04-b03-transaction-before-don73okq')
validation = Path(r'C:\Users\31918\AppData\Local\Temp\a04-b03-validation-before-b16hpn1n')
relative = Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')
artifacts = root / '00_draft_collection_02/artifacts/a04_b03_transaction_20260928'
current = nbformat.read(root / relative, 4)
original = nbformat.read(txn / relative, 4)
first_export = nbformat.read(txn / 'isolated_lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb', 4)
code = lambda nb: '\n\n'.join(c.source for c in nb.cells if c.cell_type == 'code')
assert code(current) == code(first_export), '最后只允许说明修订；已测业务代码必须保持原样'
nbformat.validate(current)
assert original.metadata == current.metadata
assert [c.id for c in original.cells] == [c.id for c in current.cells]
for old, new in zip(original.cells, current.cells, strict=True):
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in new.items() if k != 'source'}
definitions = lambda nb: {n.name: ast.dump(n) for n in ast.parse(code(nb)).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
a, b = definitions(original), definitions(current)
changed_functions = {name for name in a if a[name] != b[name]}
assert changed_functions == {'upgrade_fact_metadata', 'commit_complete_fact_partition', 'commit_calendar_partition'}
comments = lambda nb: [t.string for t in tokenize.generate_tokens(io.StringIO(code(nb)).readline) if t.type == tokenize.COMMENT]
assert all(c in comments(current) for c in comments(original))
previous = nbformat.read(validation / relative, 4)
missing_previous_comments = [c for c in comments(previous) if c not in comments(current)]
assert set(missing_previous_comments) == {'# 日历生产者不因描述文字变化重写历史；消费端按同一物理、身份和版本边界读取。', '# 最终必须从两个正式路径重读并再次求差，防止日历领先于事实。'}, missing_previous_comments
export, _ = PythonExporter().from_notebook_node(current)
assert (root / relative.with_suffix('.py')).read_bytes() == export.encode('utf8')
assert '\r' not in export and b'\r' not in (root / relative).read_bytes()
compile(export, str(relative), 'exec')
hashes = json.loads((txn / 'production_hashes.json').read_text(encoding='utf8'))
changed_files = [name for name, digest in hashes.items() if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest]
expected = {str(relative), str(relative.with_suffix('.py')), str(Path('02_Futures_Lakehouse/AGENTS.md')), str(Path('02_Futures_Lakehouse/README.md'))}
assert set(changed_files) == expected, changed_files
for c in current.cells:
    if c.cell_type == 'markdown':
        assert '手写' not in c.source
render = json.loads((artifacts / 'render_results.json').read_text(encoding='utf8'))
assert len(render) == 28 and not any(item['clipped'] for item in render)
sha = hashlib.sha256(export.encode('utf8')).hexdigest()
for source, name in [(txn / 'macro_verification.json', 'transaction_comparison.json'),
                     (validation / 'macro_verification.json', 'validation_comparison.json'),
                     (txn / 'shared_tests.log', 'shared_tests.log'),
                     (txn / 'shared_module_tests.log', 'shared_module_tests.log'),
                     (txn / 'final_export/export_verification.json', 'export_verification.json')]:
    shutil.copyfile(source, artifacts / name)
record = {'sha256': sha, 'only_markdown_changed_after_business_tests': True,
          'default_export_exact': True, 'changed_transaction_functions': sorted(changed_functions),
          'original_comments_and_notebook_state_preserved': True, 'flowcharts_rendered': len(render),
          'changed_lakehouse_files_since_transaction_baseline': changed_files,
          'shared_module_and_other_workflows_unchanged': True}
(artifacts / 'final_scope_verification.json').write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding='utf8')
report = root / '00_draft_collection_02/a04_b03_docs_logs_verification_20260928.md'
s = report.read_text(encoding='utf8')
s = s.replace('本记录按轮次保留证据。第 1—4 项中“函数内进度和日志归位尚未实施”仅表示当时状态；第 5—6 项已完成，见文末。第 7—12 项的代码精简、共享事务与执行格仍未实施。', '第 1—12 项现已完成。以下按轮次保留证据，历史章节中的“尚未实施”仅描述当时状态；当前结果见文末第 7—9 项及第 10—12 项。本文是检查记录，不新增生产规范。')
s += '''

## 第 7—9 项：重复校验、全表工作与循环不变量

来源输出与每次 dirty 完整叶各做一次业务验收；事实 value 的空值、布尔值和有限数门禁保留。当前版本历史只检查物理、表身份及版本；纯描述性 metadata 不重写历史。复用验收后的 Arrow 写 staging；staging 与正式路径只检查物理契约和全部内容，不再重新执行完整业务规则。日历验收直接接收 Arrow，删除重复完整转换和生产侧 table_digest/partition_expression/二次 metadata fragment 遍历。

启动保留一次事实计数、完成状态及 available_date 对账。循环前建立事实和日历叶映射、空事实表及列/排序/文件 Schema 常量；报告频率、请求日期过滤和请求列等不变量移到所属循环外。每次只合并和修改当前叶，正式复读直达当前叶或零行标记；成功修复及同月后续报告继承已提交映射，不重新拼接累计事实、规划或在批末扫描全历史。

以 2026-06-30 至 2026-07-31、30 个宏观格点、7 个报告窗口的同一场景实测：

| 调用或范围 | 修改前 | 修改后 |
| --- | --- | --- |
| 日历读取 / 事实读取 / 规划 | 各 2 次 | 各 1 次 |
| 日历完整业务验收 | 30 次 | 7 次，每次 dirty 叶一次 |
| 事实完整业务验收 | 43 次 | 14 次，7 次来源输出及 7 次 dirty 叶 |
| 提交时打开正式表根 | 21 次 | 0 次 |
| 日历状态函数输入 | 每次全部 30 行 | 六月叶 17 行或七月叶 13 行 |
| 七月事实合并输入 | 累计 17、26、28 行 | 当月 0、9、11 行 |

前后 9 个固定情景比较了请求、退出结果、全部行值与完整 Schema：只读、首次写入、确认空、来源失败后继续其他窗口、无新增、纯状态修复、旧版本迁移、空叶删除、同月修复与采集交错。均通过。复用 Arrow 后事实文件由多个行组改为单个行组，原写出物理行序也可能随批次调度变化，因此部分 Parquet 字节不同；逐主键排序后的全部值、字段及 metadata 一致。只读和无新增自身文件字节保持不变。该差异未通过新增业务代码掩盖，也未触发正式历史重写。

同步了 Notebook 的解释与流程图、湖仓和数据库规则，以及 MACRO_RELEASE_SCHEMA 的 quality_rules_zh 描述；字段、类型、主键、分区、版本和来源配置没有变动。随二次 metadata 遍历及批末全表复读的删除，移除了两条对应注释，其余原注释保留；当前读取边界已写入对应 Markdown。

## 第 10—12 项：共享事务、执行入口与最终验证

三个提交函数已接入现有 a00_04_staged_path_transaction.StagedPathTransaction；没有修改共享模块或新增生产抽象：

1. upgrade_fact_metadata：兼容旧版本事实整根安装，旧备份与失败新根各在本批恢复目录的 _root 下。
2. commit_complete_fact_partition：当前事实完整月叶与本次必要的新建 schema.parquet 共同恢复；空叶通过 staged_path=None 显式删除。已有标记保持原样，新增标记回滚时直接移除。
3. commit_calendar_partition：只提交当前 macro_release 月叶；此前成功事实、日历叶、interest_rate 叶及根级标记保留。日历失败不撤销事实，下次人工运行仍可无 API 修复。

所有正式复读与逐值验收均在 with 事务内；成功退出后才报告 committed。首次备份失败保留原目标；失败新数据隔离留存，恢复不完整另保留旧备份，staging 清理。不递归删除正式表根。共享模块不提供多路径对外原子可见、并发写入协调或进程被终止后的自动恢复，没有新增业务重试。

最后执行格与 a01/b01 对齐：交互内核且无 __file__ 时，显式 notebook_args=[]、standalone_mode=False，不读取内核 -f；默认无 --write，只读求差，有待办仍请求 API。脚本正常接收命令行参数；普通或内核中的模块导入不启动业务或 Schema 浏览。

同步 Notebook 总图/相关块图及根目录、湖仓规则和 README 中的共享模块使用清单、恢复证据与完成语义。事务接入前后仅三个提交函数的业务 AST 改变，其余函数完全一致；全部原注释、单元格 ID/metadata/执行计数/输出及 Notebook metadata 保留。最后修订仅调整流程图中的 write 条件，已测代码单元格逐字不变。

### 验证结果与情景假设

- latitude 环境预检通过。
- 原有业务测试 13 项通过（3.014 秒）；本入口专项 12 项通过（5.818 秒）；共享模块 11 项通过（0.133 秒），共 36 项。
- 接入前后同样的 9 个固定情景中，请求参数、退出类型和所有落盘文件字节完全相同。该对照使用已经完成第 7—9 项的版本作为起点，与上一节跨第 7—9 项的行组差异比较不同。
- 28 张 Mermaid 图使用本机 PyCharm 随附 Mermaid 离线渲染全部通过，文字溢出 0；总图、事实事务图和执行图已目视检查。
- 默认 PythonExporter 逐字节一致、Notebook 格式、Python 语法及 LF 检查通过。标准同步入口在临时副本运行 19 组 --write/--check，只发布 b03 导出；不覆盖其他正在修改的入口。
- 普通 Python 与带 ipykernel 的模块导入安全，业务子目录执行 --help 正常；入口检查覆盖 Notebook、脚本及模块导入分支。

本轮专项情景：

| 情景 | 验证要求 |
| --- | --- |
| 三种事务在正式复读时内容不一致 | 原文件字节恢复；已安装失败新数据留存；不报告 committed |
| staging 验收失败 | 不进入安装；正式文件不变；staging 清理 |
| 本批 backup 目录已存在 | 不覆盖旧证据；staging 清理；失败退出 |
| 整根首次备份或安装失败 | 原根逐字保留或恢复；不报告成功 |
| 新标记与事实叶共同提交后失败 | 移除新标记；保留无关文件；新叶隔离 |
| 恢复旧叶失败 | 保留旧备份，继续撤销本批新标记，清理 staging |
| 空叶删除后验收失败 | 恢复原叶；既有标记不变 |
| 同月最后报告的日历安装失败 | 前两个报告状态及全部已提交事实保留；下次无 API 修复 |
| 上游当前日历叶缺失 | 不创建替代上游叶，写入前失败 |
| dirty 输入 value 为布尔、空或无穷 | 暂存前拒绝，原正式文件不变 |
| 只有描述 metadata 不同 | 无 API、无整根迁移、所有文件字节不变 |

测试使用临时目录、真实临时 Parquet、由 b01 生成的两个月日历、固定 Eastmoney 响应、固定 UTC 时间/run_id 及受控文件操作失败。假设单写入者及同文件系统 staging/正式/backup 路径；故障注入是验证异常路径，不表示现场文件损坏或断言这些故障日常会发生。没有真实 API 请求、正式湖读写或后台批次；秒级测试时长不代表生产批次耗时。没有测试断电、进程强制结束或并发写入，代码也未新增这些能力。

### 当前证据

- [事务专项与双轨、范围、流程图证据](artifacts/a04_b03_transaction_20260928/)：transaction_comparison.json、validation_comparison.json、shared_tests.log、shared_module_tests.log、export_verification.json、final_scope_verification.json 及 28 张流程图。
- 事务前快照：C:\\Users\\31918\\AppData\\Local\\Temp\\a04-b03-transaction-before-don73okq。
- 第 7—9 项前快照：C:\\Users\\31918\\AppData\\Local\\Temp\\a04-b03-validation-before-b16hpn1n。
- 检查代码：tests/test_a04_b03_shared_transaction.py、scripts/check_a04_b03_transaction_entry_20260928.py。既有测试只把文件操作故障注入点从 shutil.move 切换到共享模块 os.replace，生产摘要删减后改用测试侧摘要，不减少原业务断言。
'''
s += '\n最终导出 SHA-256：`' + sha + '`。\n'
report.write_text(s, encoding='utf8', newline='\n')
print(json.dumps(record, ensure_ascii=False, indent=2))
