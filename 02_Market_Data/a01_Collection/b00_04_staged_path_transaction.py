"""按调用方给定的范围安装 staging 路径，并在验收异常时恢复旧目标。

只处理文件系统事务，不读取 Parquet、不合并数据、不决定业务更新范围。
调用方在 with 内逐项 replace() 后直接验收；退出 with 才表示整组成功。
移动使用同一文件系统内的 os.replace，不退化为跨盘复制。多目标并不对
外部读者原子可见，也不提供进程终止后的自动恢复或并发写入协调。
"""

from __future__ import annotations

import os
import pathlib
import shutil
from dataclasses import dataclass

import click


@dataclass
class _Replacement:
    target_path: pathlib.Path
    backup_path: pathlib.Path
    quarantine_new: bool
    old_moved: bool = False
    new_installed: bool = False


class StagedPathTransaction:
    """共同回滚一组文件或目录；业务验收代码保留在调用方。

    root_path 限制正式目标范围，staging_dir 限制待安装来源。backup_dir
    和可选 quarantine_dir 必须是本批独有路径。未指定 quarantine_dir
    时删除失败的新目标；指定后保留新目标，且在异常中报告隔离位置。
    staging 最终清理；只有提交成功或完整回滚后才清理 backup。
    """

    def __init__(
        self, *, root_path: pathlib.Path, staging_dir: pathlib.Path,
        backup_dir: pathlib.Path, log_context: str,
        quarantine_dir: pathlib.Path | None = None,
    ) -> None:
        self.root_path = root_path.resolve()
        self.staging_dir = staging_dir.resolve()
        self.backup_dir = backup_dir.resolve()
        self.quarantine_dir = quarantine_dir.resolve() if quarantine_dir is not None else None
        self.log_context = log_context
        self._records: list[_Replacement] = []
        self._entered = False
        self._active = False

    def __enter__(self) -> StagedPathTransaction:
        if self._entered:
            raise RuntimeError('每个 StagedPathTransaction 只能进入一次。')
        recovery_paths = [self.staging_dir, self.backup_dir]
        if self.quarantine_dir is not None:
            recovery_paths.append(self.quarantine_dir)
        for index, path in enumerate(recovery_paths):
            if self.root_path.is_relative_to(path):
                raise ValueError(f'事务临时目录不能包含正式根目录：{path}')
            for other in recovery_paths[:index]:
                if path.is_relative_to(other) or other.is_relative_to(path):
                    raise ValueError(f'事务临时目录不能互相覆盖：{path}, {other}')
        if self.quarantine_dir is not None and self.quarantine_dir.exists():
            raise FileExistsError(f'隔离目录必须为本批独有：{self.quarantine_dir}')
        self.backup_dir.mkdir(parents=True, exist_ok=False)
        self._entered = self._active = True
        return self

    def replace(
        self, *, target_path: pathlib.Path, staged_path: pathlib.Path | None,
        quarantine_new: bool = True,
    ) -> None:
        """备份旧目标，再安装已准备路径；staged_path=None 明确表示删除。

        目标可以是完整叶、表根或 marker 文件。一个事务内目标不能重复或
        互为父子。quarantine_new=False 用于失败时只需移除的临时 marker。
        来源缺失始终报错，不从路径缺失推断业务删除。
        """
        if not self._active:
            raise RuntimeError('replace() 必须在事务 with 内调用。')
        target_path = target_path.resolve()
        if not target_path.is_relative_to(self.root_path):
            raise ValueError(f'目标超出本次正式根目录：{target_path}')
        for auxiliary_path in (self.staging_dir, self.backup_dir, self.quarantine_dir):
            if auxiliary_path is not None and (
                target_path.is_relative_to(auxiliary_path)
                or auxiliary_path.is_relative_to(target_path)
            ):
                raise ValueError(f'正式目标与事务临时目录重叠：{target_path}')
        for previous in self._records:
            if target_path.is_relative_to(previous.target_path) or previous.target_path.is_relative_to(target_path):
                raise ValueError(f'同一事务的目标重复或重叠：{target_path}')
        if staged_path is not None:
            staged_path = staged_path.resolve()
            if not staged_path.is_relative_to(self.staging_dir):
                raise ValueError(f'待安装路径超出本批 staging：{staged_path}')
            if not staged_path.exists():
                raise FileNotFoundError(f'staging 缺少 {staged_path}。')

        # 保留相对目录，恢复证据可直接对应正式目标；整根替换单独占一个目录。
        relative_path = target_path.relative_to(self.root_path)
        backup_path = self.backup_dir / (relative_path if relative_path.parts else '_root')
        record = _Replacement(target_path, backup_path, quarantine_new)
        self._records.append(record)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        # 只在移动成功后登记。第一次备份失败时，原正式目标不是待删除的新数据。
        if target_path.exists():
            os.replace(target_path, backup_path)
            record.old_moved = True
        if staged_path is not None:
            os.replace(staged_path, target_path)
            record.new_installed = True

    def __exit__(self, error_type, commit_error, traceback) -> bool:
        self._active = False
        rollback_errors = []
        quarantined_paths = []
        recovery_complete = commit_error is None
        try:
            if commit_error is not None:
                # 按实际移动记录倒序恢复；一个目标失败不阻断其余目标的恢复。
                for record in reversed(self._records):
                    try:
                        if record.new_installed:
                            if self.quarantine_dir is not None and record.quarantine_new:
                                quarantine_path = self.quarantine_dir / record.backup_path.relative_to(self.backup_dir)
                                quarantine_path.parent.mkdir(parents=True, exist_ok=True)
                                os.replace(record.target_path, quarantine_path)
                                quarantined_paths.append((record.target_path, quarantine_path))
                            elif record.target_path.is_dir():
                                shutil.rmtree(record.target_path)
                            else:
                                record.target_path.unlink()
                        if record.old_moved:
                            os.replace(record.backup_path, record.target_path)
                    except Exception as rollback_error:
                        rollback_errors.append(f'{record.target_path}: {type(rollback_error).__name__}: {rollback_error}')

                recovery_complete = not rollback_errors
                # 先恢复，再报告，日志输出失败也不会阻止旧数据恢复。
                click.echo(
                    f'planning_progress: {self.log_context}; phase=commit; status=failed; '
                    f'error={type(commit_error).__name__}; rollback_partitions={len(self._records)}; '
                    f'backup_dir={self.backup_dir}'
                )
                if rollback_errors:
                    click.echo(
                        f'planning_progress: {self.log_context}; phase=rollback; status=failed; '
                        f'failed_partitions={len(rollback_errors)}; backup_dir={self.backup_dir}'
                    )
                    raise RuntimeError(
                        f'提交失败且回滚不完整；备份保留在 {self.backup_dir}；'
                        f'隔离目录={self.quarantine_dir}；' + '; '.join(rollback_errors)
                    ) from commit_error
                click.echo(
                    f'planning_progress: {self.log_context}; phase=rollback; status=completed; '
                    f'partitions={len(self._records)}; quarantine_dir={self.quarantine_dir}'
                )
                if quarantined_paths:
                    raise RuntimeError(
                        f'分区提交失败；旧目标已恢复，新目标隔离在 {self.quarantine_dir}；'
                        + '; '.join(f'{target} -> {quarantine}' for target, quarantine in quarantined_paths)
                    ) from commit_error
            return False  # 验收/安装异常继续向调用方传播，不吞掉业务错误。
        finally:
            shutil.rmtree(self.staging_dir, ignore_errors=True)
            if recovery_complete:
                shutil.rmtree(self.backup_dir, ignore_errors=True)
            if self.quarantine_dir is not None and self.quarantine_dir.exists() and not any(self.quarantine_dir.iterdir()):
                shutil.rmtree(self.quarantine_dir, ignore_errors=True)
