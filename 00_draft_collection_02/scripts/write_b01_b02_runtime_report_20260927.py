"""把本轮测量和逐项场景假设汇总为可审阅记录，不新增项目规范。"""

import ast
import json
import pathlib
import statistics

PROJECT = pathlib.Path(__file__).resolve().parents[2]
OUTPUT = PROJECT / '00_draft_collection_02/staged_path_transaction_refactor/runtime_check_20260927'
results = json.loads((OUTPUT / 'results.json').read_text(encoding='utf-8'))
path_cost = json.loads((OUTPUT / 'path_cost.json').read_text(encoding='utf-8'))
live = json.loads((OUTPUT / 'live_readonly.json').read_text(encoding='utf-8'))

# 类别：正常业务；规则边界；故障注入；误用门禁。后两类均不代表现场发生。
descriptions = [
    ('正常业务 + 故障注入', '模拟交易日 API 返回 1 个交易日，另一次直接抛异常；验证 collect 自己输出起止日志且失败不报成功。', '正常分支合理；异常分支验证传播，未实际制造网络故障。'),
    ('维护分支', '人工造出跨年缺一日、另一日 is_trading_day 改变的旧表；模拟全量来源，运行 --full --write。', '显式历史维护契约；不认为日常一定发生缺口，也不能据此要求日常扫描。'),
    ('规则边界', '只改变 Schema 中文描述，物理字段与身份 metadata 不变。', '说明文字更新可以真实发生；包含 b03 的纯 Schema 对照。'),
    ('规则边界', '把 Schema 的 primary_key 从 calendar_date 改成 date_key。', '错误契约输入；当前未观察到正式文件出现这种变化。'),
    ('正常业务', '把时钟固定到测试日期，正式最大自然日已经到有效日；collect 被 mock，只检查没有被调用。', '日常无新增；验证分支选择，不检验网络。'),
    ('规则边界', '模拟 --full 来源；另外同时传 --full 和成对日期。', '真实 CLI 互斥契约；来源是假数据。'),
    ('正常业务 / 开销约束', '已有两天，新增一天；计数输入与合并后 dirty 年份的业务 validator 调用。', '验证不重复业务校验，实际写临时 Parquet。'),
    ('故障注入', '正式年份复读时主动让物理 validator 抛 TypeError。', '只证明回滚；没有证明正式文件自然会变坏。'),
    ('正常业务', 'b01/b02 的最新交易日相同；collect mock 断言未调用。', '日常无上游新增。'),
    ('正常业务', 'b01 比 b02 多一天；collect 返回模拟行，commit 也是 mock。', '只证明 main 选新日期与调用参数，不证明该例完成落盘。'),
    ('故障注入', 'b02 正式叶验收被主动替换为抛 TypeError。', '检验恢复和隔离，不是自然出现故障的证据。'),
    ('正常业务', 'FakeJQData 提供固定合约目录，含 4 个有效品种与连续/无效代码；真实 collect 和临时提交。', '覆盖目录过滤与维度不受事实白名单裁剪；不是实时 JQData 目录。'),
    ('规则边界', '上游临时表仅有 1 月 3 日，请求 1 月 2—4 日；消费上游现有交易日。', '验证消费者信任上游责任；不表示生产者允许漏自然日。'),
    ('正常业务', '请求范围没有上游交易日，检查空结果且不认证。', '周末/假期等空选择可以真实发生。'),
    ('故障注入', '模拟 get_all_securities 直接抛 RuntimeError。', '验证不重试、无成功日志；未实际断网。'),
    ('错误来源输入', '模拟 API 目录只有 start_date、缺其他必需列。', '验证已有入口契约；当前未观察到真实 API 返回这种载荷。'),
    ('故障注入', '无交易日时，主动让 pa.Table.from_batches 抛 RuntimeError。', '固定合法空 Schema 正常不会如此失败；只验证异常日志，不应当作增加正常分支的依据。'),
    ('规则边界', '显式日期只读运行；collect 与 commit 都被 mock。', '只检查 CLI 调度与没有写入调用。'),
    ('维护分支', '模拟 --full 的缺键、计数修订、无差异和来源不再包含旧键。', '缺键/修订可由显式维护处理；额外旧键硬失败。不会把整表清空当自动修复。'),
    ('入口规则', '执行 b02 入口 cell，模拟 Notebook、脚本和 import 三种环境。', '是 Python 环境模拟，不是打开 PyCharm 点击运行。'),
    ('直接提交接口', '跨月、跨交易所完整区间替换；构造某些叶或整个输入为空，检查范围外保留、删除与 marker。', '局部 commit 支持该语义；不能据此断言当前默认 CLI 会清空整张成熟市场日历。'),
    ('故障注入', '让第二个正式叶验收主动失败，第一叶已经安装。', '证明同一调用整组恢复；不估计故障概率。'),
    ('人工改变物理契约', '构造两个文件，后一个分别改变字段名、类型、nullable、表名、主键或分区身份；只测试 b01/b02。', '当前未发现这种混合文件；它检验既有物理门禁，不是日常损坏的证据。'),
    ('规则边界', '构造旧描述和旧 schema_version 的物理文件，身份与物理字段保持一致。', '历史描述落后于代码可以真实发生，不应因此重写历史。'),
    ('开销约束', '分别测试 collect、空湖提交和合并提交，统计 Arrow 契约检查次数。', '验证去重没有失效；来源是模拟值。'),
    ('错误输入', '主动把 source 设为 None 或 weekday 设为与日期不符的值。', '验证调用方传错值会提前失败；未观察到正常 collect 产生它们。'),
    ('开销约束 / 文件布局', '强制每个 Parquet 最多 1 行或 10 行，计数正式逐文件校验。', '多文件是人为布局，并非损坏；检查每文件仅一次。当前正式叶大多单文件。'),
    ('开销约束 / 模式', '尾部、无新增、全量、显式四模式；模拟目录，计数上游打开、验证及认证调用。', '覆盖各模式读取责任；不是网络耗时测试。'),
    ('故障注入', '分别让 b01 staging 叶读取或安装系统调用抛 OSError。', '证明备份/恢复顺序；未在真实文件上触发锁或故障。'),
    ('人工改变新文件', '在 staging 写完或正式安装后，测试钩子主动重写第二个 Parquet 的字段类型。', '当前单写者流程没有这个自然步骤；不能把该人工操作解释为自然损坏或真实已复现事故。'),
    ('故障注入 / 人工改变 marker', '主动把零行 marker 写成错类型、非零行，或强制读取失败；分别有无旧数据。', '合法空表创建情景真实；这三种故障是注入，正常固定 Schema 写入不应自行变成这些值。'),
    ('入口规则', '执行 b01 入口 cell，模拟 Notebook、脚本和 import。', '只验证分支及参数，不测 GUI。'),
    ('日志协议', '用模拟来源运行全量无差异、全量有差异、显式只读；用 worker 前缀识别日志。', '验证 monitor 可消费的日志格式，没有启动后台 worker 或监控窗口。'),
    ('共享接口形态', '临时目录中同组替换整表根、另一表的叶及 marker，旁边保留不相关叶。', '源于 b05/b08 的实际目标形态；没有宣称 b05/b08 已接入或完成端到端测试。'),
    ('直接接口 + 误用门禁', '先提供不存在的 staging，后用 staged_path=None 明确删除。', '缺失来源是调用方错误，不应解释为删除；删除本身是显式接口语义。'),
    ('故障注入', '安装、删除、新建 marker 都完成后，with 内主动 raise ValueError。', '覆盖正式验收失败的整组恢复，不是现场业务发现错误。'),
    ('故障注入', '第一次备份的 os.replace 直接抛 PermissionError。', '文件权限/占用可导致移动失败，但本测试没有真实占用文件。'),
    ('故障注入', '第二个 staging 安装调用直接抛 OSError，前一个已完成。', '检验半途安装失败的范围，不证明一定发生。'),
    ('失败现场策略', '主动抛验收异常，要求保留失败新叶，但删除新 marker。', '验证 b02 的既有隔离策略。'),
    ('故障注入', '发生验收异常后，再让某个旧备份恢复抛 PermissionError。', '复合故障；检验不丢旧备份、继续恢复其他目标。没有现场概率证据。'),
    ('误用门禁', '同一事务先替换叶，又要求替换它的父目录。', '调用方计划错误；当前 b01/b02 生成的不同叶不应触发。'),
    ('中断注入', '回滚旧备份时主动抛 KeyboardInterrupt。', '只模拟可捕获中断；不覆盖 kill、断电、系统崩溃，也不证明这些情况下能自动恢复。'),
    ('误用门禁', '目标越过根目录、指向 staging，或 staging 包含 backup。', '路径构造/配置错误；当前正常固定目录构造不应触发。'),
    ('误用门禁', '预先创建 backup，再试图把它作为本批目录使用。', '错误复用批次目录；当前 b01/b02 使用 UUID，正常不应走这条路。'),
]

files = ('test_b01_c01_c02_daily_tail_modes.py', 'test_futures_variety_calendar_complete_catalog.py',
         'test_b01_fragment_physical_contracts.py', 'test_b01_b02_commit_semantics.py', 'test_staged_path_transaction.py')
inventory = []
for file in files:
    path = PROJECT / '00_draft_collection_02/tests' / file
    tree = ast.parse(path.read_text(encoding='utf-8'))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith('test_'):
            inventory.append((path, node))
assert len(inventory) == len(descriptions) == len(results['tests']) == 44

lines = [
    '# b01/b02 抽象后运行检查与情景清单', '',
    '日期：2026-09-27。本文为检查记录，所有耗时来自本机本次测量，不新增性能门槛。正式业务代码本轮未改。', '',
    '## 现有运行时间要求', '',
    '- [根目录 AGENTS](../../../AGENTS.md#长时间任务的人工启动后台执行与可见监控)：预计超过 10 分钟的任务须在当前交互明确授权单批；10 分钟是管理边界，不是脚本的性能上限。',
    '- 后台批次需要独立可见 Terminal，显示阶段、可量化进度、累计耗时、心跳及失败；确认健康后 Codex 结束回合，不以轮询维持活动。普通失败、配额停止、监控异常都停止后续，不自动重试。',
    '- 时长本身不强制追加样本或 dry-run；本次用户明确要求测试，因此执行本地测试。',
    '- [仓单专用门槛](../../../02_Futures_Lakehouse/README.md)：指定 a02/b03 批次首 50 个成功分区的总耗时中位数超过 15.4 秒就停止；它不适用于 b01/b02。',
    '- 日常信任正式历史，避免重复完整业务校验；未找到 b01/b02 固定多少秒的已确认上限，因此不能凭测试自行制定“合格秒数”。', '',
    '## 测量前提', '',
    f'- Python：`{results["python"]}`。单进程、同机 E 盘、同一文件系统，无其他写者。测试没有清空 OS 文件缓存，不能当冷启动磁盘基准。',
    '- 输入来自正式表的只读快照；目标为草稿目录下独有临时目录的完整复制，非硬链接。提交计时包含合并、验证、staging、安装、正式复读、清理和日志文件写入；不包括复制种子、测试后的全量比对、Python 进程启动、API/认证和采集转换。',
    '- 每种提交场景按“旧→新、新→旧、旧→新”顺序测 3 轮；报告中位数及范围。重复测量是预先定义的性能采样，不是业务失败后的自动重试。',
    '- 无新增 CLI 在同一已导入进程内测 3 轮；时钟固定为快照截止日 21:00，认证函数若被调用就报错。它不代表 2026-09-27 的正式湖已经更新到今天。',
    '- 独立旧/新输出比较和正式文件哈希复查位于计时之外，不加入生产程序。', '',
    '## 数据规模', '',
    '| 表 | 行数 | 实际叶 | 文件 | 日期范围 | 补最后一天的输入/叶 |',
    '|---|---:|---:|---:|---|---|',
]
for record in results['inventory']:
    lines.append(f'| {record["table"]} | {record["rows"]:,} | {record["actual_partitions"]:,} | {record["files"]:,} | {record["min_date"]}—{record["max_date"]} | {record["tail_rows"]} 行 / {record["tail_partitions"]} 叶 |')
lines += ['', 'b02 空库提交计划为 6 个交易所 × 200 个月 = 1,200 个组合，其中实际非空叶为 944。未开展交易的早期月份可为空；这由当前环节的计划逻辑产生，测试没有额外放大分区数。', '',
          '## 耗时与正常运行', '', '| 场景 | 抽象前中位数（最小—最大） | 当前中位数（最小—最大） | 差值 |', '|---|---:|---:|---:|']
for records in (results['benchmark'], results['cli']):
    for table, scenario in sorted({(r['table'], r['scenario']) for r in records}):
        values = {v: [r['elapsed_s'] for r in records if (r['table'], r['scenario'], r['version']) == (table, scenario, v)] for v in ('before', 'current')}
        old, current = (statistics.median(values[v]) for v in ('before', 'current'))
        summaries = [f'{statistics.median(values[v]):.3f}s（{min(values[v]):.3f}—{max(values[v]):.3f}）' for v in ('before', 'current')]
        lines.append(f'| {table} / {scenario} | {summaries[0]} | {summaries[1]} | {current-old:+.3f}s / {(current/old-1)*100:+.1f}% |')
current_records = [r for r in results['benchmark'] if r['version'] == 'current']
lines += ['', f'- 44 项回归测试全部通过，测试主体耗时 {results["regression_elapsed_s"]:.3f} 秒；整个本地检查（含准备、重复测量、结果比对、清理）{results["total_elapsed_s"]:.3f} 秒。',
          '- 所有计时调用正常返回；输出内容与真实快照逐项相同，尾部更新没有改变未触达文件；临时 staging/backup 清理完成。正式文件前后哈希一致。',
          f'- 提交进度 1/N 至 N/N 连续且终态成功；前缀被现有 worker 识别。当前版本最长相邻进度间隔 {max(r["max_progress_gap_s"] for r in current_records):.3f} 秒。未启动真实 detached worker/monitor，不能把前缀检查称为可见监控验收。',
          '- 日常少量分区路径正常；b02 完整初建出现稳定耗时回退，不能用“仍少于 10 分钟”掩盖该回退。此处尚无 b01/b02 数值 SLA，结论是管理边界满足、本地功能通过、完整初建性能存在差异。', '',
          '## 耗时差异定位', '',
          '- [共享模块目标比较循环](../../../02_Futures_Lakehouse/a00_04_staged_path_transaction.py:90) 对每个新目标遍历所有已登记目标，检查双向父子关系，因此目标数 N 时有 N(N-1)/2 对比较。',
          f'- 使用与 b02 相同的 6 交易所 × 200 月路径层级、互不重叠目标，在内存单独重放原比较表达式，3 次耗时为 {path_cost[1]["elapsed_s"]} 秒，中位数 {statistics.median(path_cost[1]["elapsed_s"]):.3f} 秒。没有文件读写、resolve、exists、mkdir、移动、Parquet 或日志。',
          '- 1,200 目标有 719,400 对比较 / 1,438,800 次 is_relative_to；6 目标只有 15 对 / 30 次。这个数量级差异解释了为什么日常几乎无变化，空库初建明显变慢。模型路径是同形状重放，不是系统函数级 profiler；其耗时与实际增加约 12.69 秒相符，支持“主要来自目标两两比较”的判断。',
          '- 路径重复/重叠是调用方构造错误的门禁（T41、T43）。当前 b01/b02 已生成不同年份或 exchange/year/month 叶；这些合法路径不互相覆盖。本次没有用测试结果证明必须保留二次复杂度检查，也没有自动删除门禁。可在用户判断边界后用目标/祖先集合避免两两扫描，或明确由调用方保证。', '',
          '## 真实只读来源检查', '',
          f'- 固定请求 2026-08-28 单日，不带 --write；每个 CLI 最多 90 秒，失败立即停止。b01 的实际进程耗时 {live["runs"][0]["elapsed_s"]:.3f} 秒，退出码 {live["runs"][0]["return_code"]}。',
          '- b01 在 authenticate_jqdata → jqdatasdk.auth → socket.connect 阶段遇到 WinError 10013（套接字访问被拒绝），未获得来源结果。现有记录不能进一步区分沙箱网络限制、系统防火墙或其他权限原因；不能归因为这次文件事务抽象。',
          '- b02 按失败停止约定未调用，没有自动重试，没有改连接配置。正式文件哈希保持一致。这个失败发生在 --write 分支之外，也不证明共享提交逻辑失败。',
          '- 因此真实 API 链路未验收通过。没有把失败的 3 秒连接耗时当成成功采集时长。原始已脱敏日志见 [b01 只读日志](b01_trade_calendar-live-readonly.log)。', '',
          '## 本轮性能场景编号', '',
          '1. **P01 b01 无新增**：复制截至 2026-08-28 的完整表，把当前时间设为同日 21:00，main 应直接退出。',
          '2. **P02 b02 无新增**：复制同日截止的 b01/b02，上游没有新增交易日，main 应直接退出。',
          '3. **P03 b01 尾部一天**：仅在测试副本删除 2026-08-28，再提交该日真实行；这是模拟前一日已完成状态，不是假设文件自然丢行。',
          '4. **P04 b02 尾部一天**：仅在测试副本删除最后交易日的所有真实品种行，再提交它们；同月其他行和其他月保持。',
          '5. **P05 b01 空库初建**：把全部当前真实日历作为已采集结果提交空目录；17 年份，合法初次建表情景。',
          '6. **P06 b02 空库初建**：把全部当前真实品种日历作为已采集结果提交空目录；1,200 个计划组合。合法初建情景，不是日常会反复全量重写的假设。', '',
          '7. **P07 目标重叠比较**：同层数、同组合数的互不重叠路径，只重放共享模块比较循环以定位耗时；不推断文件故障。',
          '8. **P08 真实只读单日**：b01/b02 各计划运行一次单日 CLI，90 秒边界；b01 连接失败后 b02 未执行。', '',
          '## 44 项既有测试：逐项假设与合理性边界', '',
          '下表每一项都已实际执行；“通过”表示断言通过。对故障注入项，实际成功的是正确拒绝/恢复，而不是故障会在现实中发生。一个测试方法可含多个参数子情景。回归小样本使用人造 2023/2024 日期与模拟目录，有些交易日标记仅服务分支测试；不能把它们作为现实节假日或品种生命周期的证据。真实数据规模计时另用上面的正式只读快照。', '',
          '| 编号 | 类别 | 条件与检查动作 | 是否有正常业务依据 / 不代表什么 | 测试入口 |',
          '|---|---|---|---|---|']
for index, ((path, node), (category, condition, boundary)) in enumerate(zip(inventory, descriptions, strict=True), 1):
    lines.append(f'| T{index:02d} | {category} | {condition} | {boundary} | [{node.name}]({path.as_posix()}:{node.lineno}) |')
lines += ['', '## 需要用户重点判断的情景', '',
          '- T30、T31 主动改变新 Parquet/marker 的类型或内容，T23 主动制造混合物理契约；当前没有自然触发证据。它们可以证明门禁与回滚机制，但不能证明该防御的生产必要性。',
          '- T17 主动让固定合法空表转换失败；其主要价值是日志异常路径，不是正常业务完整性。',
          '- T21 的清空整表、T35 的直接删除属于 commit 接口能力；当前 --full 对额外旧键是硬失败，不能混称为日常 CLI 自然情景。',
          '- T41、T43、T44 是错误调用计划/路径/批次复用；当前 b01/b02 固定构造通常不会触发。公共模块是否承担这些门禁，应按实际未来调用方决定，而非以“测试通过”代替判断。',
          '- T37—T40 的系统调用和复合恢复失败有技术可能性，但本轮只做函数注入，没有实际锁冲突证据；T42 不能替代 kill/断电恢复测试。', '',
          '原始计时与每测试时间见 [results.json](results.json)，全部逐次日志保留在本目录。计时脚本为 [check_b01_b02_transaction_runtime_20260927.py](../../scripts/check_b01_b02_transaction_runtime_20260927.py)。', '',
          '测试工具修正记录：首次计时适配器把字符串缓存命名为 buffer，被 Click 当成二进制输出流，计时启动时退出；仅修正草稿计时器为 pending_text 并声明文本编码，再完整执行。该问题不在 b01/b02 或共享模块中。正式业务代码本轮逐字节保持不变。', '']
(OUTPUT / 'README.md').write_text('\n'.join(lines), encoding='utf-8', newline='\n')
print(OUTPUT / 'README.md')
