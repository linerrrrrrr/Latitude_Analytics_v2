"""One manually authorized five-turn batch; no scheduling or automatic retries."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import time

import psutil

THREAD_ID = "01a0de77-46d8-7eb0-934f-91b8424d653a"
TARGET = "02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01a_position_rank_special_case_calibration.ipynb"
ROUNDS = [
    ("plan", "进入下一个脚本 a02/b01a。阅读上下游、牵连脚本和规范，列出适配本脚本的 12 项清单、执行分支、提交/失败恢复边界及验收条件。本轮只分析，不改业务文件。后面四轮已经获用户授权，不需要再次请求常规实施确认。"),
    ("docs_logs", "执行清单第 1—4 项：整理解释性 Markdown；增加总流程图；各逻辑块前增加流程图单元格；统一日志样式。保留业务逻辑，尽量不动代码注释，保留已有输出和执行计数。"),
    ("function_progress", "执行清单第 5—6 项：读取/请求/校准证据生成函数自行报告进度；生成、提交和状态日志归位。日志沿真实操作统计，不为日志重复读取。没有日期水位就不得新增或虚构水位。"),
    ("redundancy", "执行清单第 7—9 项：只删除有现行保证来源的重复校验和转换；消除逐分区/逐案例重复全表处理，若没有就明确说明；外移循环不变量。raw 响应摘要、目标合约和完整 Top 20 冻结值匹配属于本环节核心契约，不能机械套用 silver 上游信任规则删除它们。"),
    ("transaction_entry", "执行清单第 10—12 项：评估并在语义适用时接入 a00_04 共享安装与回滚模块，保持本脚本原子归档与恢复范围；对齐 Notebook 显式参数、脚本 CLI 和模块导入不执行的入口；同步所有受影响说明、流程图和双轨并完成必要验证。不适用项必须给具体依据，不能为接入而改变业务语义。本轮完成后结束本批，不进入下一脚本。"),
]
COMMON = """
这是用户在当前交互明确授权的自动逐轮局部重构批次，仅限 a02/b01a，共五轮。
原任务已完成 a01/b01—b08 及 a02/b01；延续既有偏好，以当前文件和规范为准。
遵循根 AGENTS.md、湖仓 AGENTS.md 及相关数据库规范。先读上下游和现有实现。
只编辑 Notebook 源，通过 a00_02_sync_notebook_exports.py 标准导出 .py。
保留已有未提交改动；不提交 Git、不回退其他任务，不新增正式脚本。
新增一次性脚本/测试只能在 00_draft_collection_02。使用 latitude Python。
只做代码重构和必要的模拟/临时目录验证，禁止请求真实业务 API、运行生产采集或写正式 raw/silver/gold。
不要读取或输出凭据。不要创建定时任务、自动恢复或新的批次，不派生其他 Agent。
避免凭假设增加防御；区分真实缺陷、契约补齐、文档修正与可选加固。没有新证据不扩大检查。
本轮有失败、额度/权限阻塞、必须由用户决定的业务语义时立即结束，返回 blocked 或 failed，不自重试、不绕过权限。
最终用指定 JSON 格式报告。只有本轮实际完成且必要检查通过才 status=completed；
轮次和 target 必须准确。summary 用中文，evidence 列实际检查和结果，不能把未运行写成通过。
"""


def publish_status(status_path, status):
    """Atomic control-plane publication; only Windows sharing errors retry."""
    status["heartbeat_at"] = time.time()
    temporary_path = status_path.with_suffix(".tmp")
    with temporary_path.open("w", encoding="utf-8") as stream:
        json.dump(status, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    for attempt in range(6):
        try:
            os.replace(temporary_path, status_path)
            return
        except OSError as error:
            if error.winerror not in (5, 32) or attempt == 5:
                raise
            time.sleep(0.1 * (attempt + 1))


def stop_child(child):
    if child is None or child.poll() is not None:
        return
    parent = psutil.Process(child.pid)
    descendants = parent.children(recursive=True)
    for process in reversed(descendants):
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass
    try:
        parent.kill()
    except psutil.NoSuchProcess:
        pass
    child.wait(timeout=15)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex", required=True, type=Path)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--round-timeout-minutes", type=int, default=45)
    args = parser.parse_args()
    project_markers = [".git", ".env", "config/settings.py"]
    current_path = Path.cwd().resolve()
    for candidate_root in [current_path, *current_path.parents]:
        if all((candidate_root / marker).exists() for marker in project_markers):
            break
    else:
        raise RuntimeError("未找到项目根目录")
    run_dir = args.run_dir.resolve()
    if not run_dir.is_relative_to(candidate_root / "00_draft_collection_02/run_status"):
        raise ValueError("run-dir must be inside the draft run_status directory")
    run_dir.mkdir(parents=True, exist_ok=True)
    schema = {
        "type": "object", "additionalProperties": False,
        "properties": {
            "status": {"type": "string", "enum": ["completed", "blocked", "failed"]},
            "round": {"type": "string"}, "target": {"type": "string"},
            "summary": {"type": "string"},
            "evidence": {"type": "array", "items": {"type": "string"}},
            "needs_user_input": {"type": "boolean"},
        },
        "required": ["status", "round", "target", "summary", "evidence", "needs_user_input"],
    }
    schema_path = run_dir / "response.schema.json"
    schema_path.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
    for number, (name, instruction) in enumerate(ROUNDS, 1):
        prompt = COMMON + f"\n本轮 {number}/5，round={name}，target={TARGET}\n" + instruction
        (run_dir / f"{number:02d}-{name}.prompt.txt").write_text(prompt, encoding="utf-8")
    if not args.execute:
        print(f"Prepared five prompts and schema: {run_dir}")
        return
    # An existing run is never restarted implicitly, even after an earlier failure.
    with (run_dir / "started.lock").open("x", encoding="utf-8") as stream:
        stream.write(str(os.getpid()))
    status_path = run_dir / "status.json"
    status = dict(state="starting", worker_pid=os.getpid(), child_pid=None,
                  round=0, completed_rounds=0, total_rounds=5, phase="monitor_start",
                  started_at=time.time(), last_event="", error="", thread_id=THREAD_ID)
    child = None
    try:
        publish_status(status_path, status)
        monitor_path = Path(__file__).with_name("monitor_refactor_rounds.ps1")
        monitor = subprocess.Popen([
            "powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
            str(monitor_path), "-RunDir", str(run_dir),
        ], creationflags=subprocess.CREATE_NEW_CONSOLE)
        status["monitor_pid"] = monitor.pid
        monitor_heartbeat = run_dir / "monitor.heartbeat"
        deadline = time.monotonic() + 20
        while not monitor_heartbeat.exists():
            if monitor.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("Visible monitor did not become ready")
            time.sleep(0.5)
        (run_dir / "baseline-status.txt").write_bytes(subprocess.check_output(
            ["git", "status", "--short"], cwd=candidate_root))
        for number, (name, _) in enumerate(ROUNDS, 1):
            status.update(state="running", round=number, phase=name, last_event="", child_pid=None)
            publish_status(status_path, status)
            prefix = run_dir / f"{number:02d}-{name}"
            reply_path = Path(str(prefix) + ".reply.json")
            # Explicit ID, shared history, normal workspace sandbox and automatic approval review.
            command = [str(args.codex), "--no-daemon", "exec", "--approve-for-me", "-C", str(candidate_root),
                       "resume", THREAD_ID, "--json", "--output-schema", str(schema_path),
                       "--output-last-message", str(reply_path), "-"]
            with Path(str(prefix) + ".prompt.txt").open("rb") as prompt_file, \
                 Path(str(prefix) + ".events.jsonl").open("wb") as event_file, \
                 Path(str(prefix) + ".stderr.log").open("wb") as error_file:
                child = subprocess.Popen(command, cwd=candidate_root, stdin=prompt_file,
                                         stdout=event_file, stderr=error_file)
                status["child_pid"] = child.pid
                round_start = time.monotonic()
                event_offset = 0
                completed_event = False
                failed_event = False
                while True:
                    if monitor.poll() is not None or time.time() - monitor_heartbeat.stat().st_mtime > 20:
                        raise RuntimeError("Visible monitor stopped or heartbeat expired")
                    if (run_dir / "STOP").exists():
                        raise RuntimeError("Manual STOP requested")
                    if time.monotonic() - round_start > args.round_timeout_minutes * 60:
                        raise RuntimeError("Round time limit reached; no automatic retry")
                    with Path(str(prefix) + ".events.jsonl").open("rb") as event_reader:
                        event_reader.seek(event_offset)
                        while event_line := event_reader.readline():
                            if not event_line.endswith(b"\n"):
                                if child.poll() is not None:
                                    raise RuntimeError("CLI exited with an incomplete JSONL event")
                                break
                            event_offset = event_reader.tell()
                            event = json.loads(event_line)
                            event_type = event.get("type", "")
                            status["last_event"] = event_type
                            if event_type == "thread.started" and event.get("thread_id") != THREAD_ID:
                                raise RuntimeError("CLI resumed an unexpected task")
                            completed_event |= event_type == "turn.completed"
                            failed_event |= event_type in ("turn.failed", "error")
                    publish_status(status_path, status)
                    if failed_event:
                        raise RuntimeError("CLI reported a failed turn/error; inspect event log")
                    if child.poll() is not None:
                        # Read the final bytes on the next iteration if exit raced the last read.
                        if Path(str(prefix) + ".events.jsonl").stat().st_size > event_offset:
                            continue
                        break
                    time.sleep(1)
                if child.returncode != 0 or not completed_event:
                    raise RuntimeError(f"CLI did not complete successfully: exit={child.returncode}")
            reply = json.loads(reply_path.read_text(encoding="utf-8"))
            if (reply.get("status") != "completed" or reply.get("needs_user_input") is not False
                    or reply.get("round") != name or reply.get("target") != TARGET
                    or not reply.get("summary") or not reply.get("evidence")):
                raise RuntimeError(f"Round not accepted: {reply}")
            status["completed_rounds"] = number
            status["summary"] = reply["summary"]
            publish_status(status_path, status)
        status.update(state="completed", child_pid=None, phase="batch_complete")
        publish_status(status_path, status)
    except BaseException as error:
        stop_child(child)
        status.update(state="failed", child_pid=None, error=str(error))
        publish_status(status_path, status)
        raise


if __name__ == "__main__":
    main()
