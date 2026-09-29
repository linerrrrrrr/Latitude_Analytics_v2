"""生成正式归属确认后的文档变更候选；当前不修改规范文件。"""

import difflib
import json
import pathlib

DRAFT = pathlib.Path(__file__).resolve().parent
PROJECT = DRAFT.parents[1]
MODULE = 'a00_04_staged_path_transaction.py'
changes = {
    'AGENTS.md': [
        ('根级 `a00_01`—`a00_03` 支撑脚本编号', '根级 `a00_01`—`a00_04` 支撑脚本编号'),
        ('- [00_draft_collection_02](00_draft_collection_02)：',
         f'- [02_Futures_Lakehouse/{MODULE}](02_Futures_Lakehouse/{MODULE})：湖仓级 staging 路径安装与失败恢复实现；当前由 a01/b01、b02 共用，业务合并、数据验收和共同回滚范围仍由环节决定。\n- [00_draft_collection_02](00_draft_collection_02)：'),
    ],
    '02_Futures_Lakehouse/AGENTS.md': [
        ('`a00_03_notebook_schema_browser.py` 排列，分别负责环境检查、导出同步和 Schema/样例浏览',
         '`a00_03_notebook_schema_browser.py`、`a00_04_staged_path_transaction.py` 排列，分别负责环境检查、导出同步、Schema/样例浏览和 staging 路径安装/失败恢复'),
        ('## 共享配置与 Notebook 展示归属',
         '## 共享配置与 Notebook 展示归属\n\n- 湖仓级安装与恢复操作位于 [a00_04_staged_path_transaction.py](a00_04_staged_path_transaction.py)，当前由 a01/b01、b02 共用。它按调用方给出的目标备份、安装或显式删除，并在安装或正式验收失败时共同恢复；业务合并、Parquet 写入及复读、状态推进和一次共同回滚的范围仍留在环节。其他入口继续使用现有实现，按用户选定的环节逐项接入。'),
        ('- 修改本文件时，必须同步检查根目录规范索引',
         f'- [{MODULE}]({MODULE})：a01/b01、b02 共用的路径安装与失败恢复操作；不承载数据契约或业务更新范围。\n- 修改本文件时，必须同步检查根目录规范索引'),
    ],
    '02_Futures_Lakehouse/README.md': [
        ('├─ a00_03_notebook_schema_browser.py # Schema 与数据样例浏览',
         '├─ a00_03_notebook_schema_browser.py # Schema 与数据样例浏览\n├─ a00_04_staged_path_transaction.py # staging 路径安装与失败恢复'),
        ('根级 `a00_01`—`a00_03` 按环境检查、导出同步、契约浏览排列，是共用支撑脚本；业务组从 `a01` 开始。',
         '根级 `a00_01`—`a00_04` 按环境检查、导出同步、契约浏览、路径安装与恢复排列，是共用支撑脚本/模块；业务组从 `a01` 开始。\n\n'
         'a01/b01、b02 已共用 [StagedPathTransaction](a00_04_staged_path_transaction.py)：环节在事务内逐项安装并直接执行正式复读，全部通过后完成整组提交；安装或验收失败时倒序恢复实际移动的目标。合并方式、业务校验、更新范围、事务分组和返回值仍由各环节决定。b01 清理失败的新分区，b02 保留隔离的新分区；恢复不完整时保留旧备份。该模块使用同一文件系统内的路径替换，不提供跨目录原子可见性、进程终止后的自动恢复或并发写入协调。其余入口尚未接入，按 [湖仓目录规则](AGENTS.md) 逐项迁移。'),
    ],
}
diffs = []
for relative, replacements in changes.items():
    before = (PROJECT / relative).read_text(encoding='utf-8')
    after = before
    for old, new in replacements:
        assert after.count(old) == 1, (relative, old, after.count(old))
        after = after.replace(old, new, 1)
    diffs.append(''.join(difflib.unified_diff(before.splitlines(keepends=True), after.splitlines(keepends=True),
                                            fromfile=f'a/{relative}', tofile=f'b/{relative}')))
(DRAFT / 'documentation_changes.patch').write_text('\n'.join(diffs), encoding='utf-8')
(DRAFT / 'documentation_changes.json').write_text(json.dumps(changes, ensure_ascii=False, indent=2), encoding='utf-8')
print('documentation_candidate: AGENTS.md, 02_Futures_Lakehouse/AGENTS.md, 02_Futures_Lakehouse/README.md')
