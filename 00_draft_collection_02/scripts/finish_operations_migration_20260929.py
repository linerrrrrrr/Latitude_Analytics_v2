"""Update existing control tests and Notebook narrative for the console migration."""
import ast
import json
import pathlib

import nbformat
from nbconvert.exporters import PythonExporter

root = pathlib.Path.cwd().resolve()
operations = root / "02_Futures_Lakehouse/operations"
test_path = operations / "tests/test_monitor_runtime_and_snapshots.py"
text = test_path.read_text(encoding="utf-8")
text = text.replace('MONITOR_PATH = OPERATIONS_ROOT / "watch_batch.ps1"', 'sys.path.insert(0, str(OPERATIONS_ROOT / "runtime"))\nimport background_worker as worker')
text = text.replace('RUNTIME_PROBE_PATH = OPERATIONS_ROOT / "verify_operations_runtime.py"', 'RUNTIME_PROBE_PATH = OPERATIONS_ROOT / "runtime" / "verify_operations_runtime.py"')
start = text.index("    def run_monitor_once(")
end = text.index("    def test_operations_runtime_probe", start)
text = text[:start] + '''    def test_shared_reader_allows_repeated_atomic_replace(self) -> None:
        import threading
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "status.json"
            worker.atomic_write_json(path, {"counter": 0})
            failures = []
            finished = threading.Event()

            def read_loop():
                try:
                    while not finished.is_set():
                        self.assertIsInstance(worker.read_json_shared(path)["counter"], int)
                except BaseException as error:
                    failures.append(error)

            thread = threading.Thread(target=read_loop)
            thread.start()
            try:
                for counter in range(1, 101):
                    worker.atomic_write_json(path, {"counter": counter})
            finally:
                finished.set()
                thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(worker.read_json_shared(path), {"counter": 100})

    def test_complete_reference_archive_and_preserved_history(self) -> None:
        import stat
        import zipfile
        reference = OPERATIONS_ROOT / "referance"
        manifest = json.loads((reference / "snapshot_manifest.json").read_text(encoding="utf-8"))
        archive_path = reference / manifest["archive"]
        if not archive_path.exists():
            self.skipTest("Local reference ZIP is intentionally not tracked by Git.")
        self.assertEqual(hashlib.sha256(archive_path.read_bytes()).hexdigest(), manifest["archive_sha256"])
        if os.name == "nt":
            for path in (archive_path, reference / "snapshot_manifest.json"):
                self.assertTrue(path.stat().st_file_attributes & stat.FILE_ATTRIBUTE_READONLY)
        self.assertEqual(len(manifest["files"]), 154)
        with zipfile.ZipFile(archive_path) as archive:
            self.assertIsNone(archive.testzip())
            for entry in manifest["files"]:
                payload = archive.read("operations/" + entry["path"])
                self.assertEqual(len(payload), entry["byte_size"])
                self.assertEqual(hashlib.sha256(payload).hexdigest(), entry["sha256"])
                if entry["path"].startswith(("run_history/", "archived_batches/")):
                    self.assertEqual((OPERATIONS_ROOT / entry["path"]).read_bytes(), payload)

''' + text[end:]
test_path.write_text(text, encoding="utf-8", newline="\n")

changes = {
    "b07_suspected_session_reconciliation": (
        "默认 worker 在 b06 后执行本环节，`--skip-optional-quality` 可跳过 b07；b08 仍是人工独立入口。",
        "operations 总控台的日常快捷选择在 b06 后包含本环节，操作者可取消勾选；也可单独选择本环节并配置原 CLI 参数。b08 不进入日常快捷选择，需人工单独勾选并显式确认。",
    ),
    "b08_full_minute_quality": (
        "b08 是人工入口，不进入任何 operations batch，也不由 `--skip-optional-quality` 控制；该选项只跳过 b07。",
        "b08 是人工入口，不进入 operations 总控台的日常快捷选择；可在总控台单独勾选并显式启用 `--confirm-full-quality`，提交时另选 `--write`，随后由本次确认的后台批次执行。",
    ),
}
for stem, (before, after) in changes.items():
    notebook_path = root / "02_Futures_Lakehouse/a01_Futures_Market_Data" / (stem + ".ipynb")
    script_path = notebook_path.with_suffix(".py")
    old_script_ast = ast.dump(ast.parse(script_path.read_text(encoding="utf-8-sig")))
    notebook_text = notebook_path.read_text(encoding="utf-8")
    assert before in notebook_text
    # Preserve every other byte of the notebook, including existing outputs.
    notebook_path.write_text(notebook_text.replace(before, after), encoding="utf-8", newline="\n")
    notebook = nbformat.read(notebook_path, as_version=4)
    nbformat.validate(notebook)
    exported, _ = PythonExporter().from_notebook_node(notebook)
    assert ast.dump(ast.parse(exported)) == old_script_ast, "Business code must remain unchanged"
    script_path.write_text(exported, encoding="utf-8", newline="\n")
    assert script_path.read_bytes() == exported.encode("utf-8")
    print("Narrative only, business AST unchanged:", stem)

path = root / "02_Futures_Lakehouse/README.md"
text = path.read_text(encoding="utf-8")
text = text.replace("默认日常 worker 编排其余 18 个阶段。", "总控台的日常快捷选择包含其余 18 个阶段。")
text = text.replace("不恢复或增加其他 `--groups` 阶段；b08 永不进入 worker。正式启动命令、运行目录和 monitor 用法只在", "也可单独选择任意正式环节并配置原参数；b08 需另行勾选并显式确认，不进入日常快捷选择。正式启动、运行目录和监控用法只在")
text = text.replace("默认 worker 只编排其中 18 个日常阶段，不把 b08 变成日常前置条件。", "日常快捷选择只包含其中 18 个阶段，不把 b08 变成日常前置条件。")
path.write_text(text, encoding="utf-8", newline="\n")
print("Existing tests and affected Notebook descriptions synchronized.")
