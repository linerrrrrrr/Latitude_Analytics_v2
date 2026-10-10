"""导出检查级别：仅使用临时 Notebook，不执行导出代码或业务 API。"""

from contextlib import redirect_stderr, redirect_stdout
import importlib.util
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import nbformat
from nbconvert.exporters import PythonExporter


project_markers = [".git", ".env", "config/settings.py"]
current_path = Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
MODULE_PATH = PROJECT_ROOT / "R02_Market_Data/a01_Collection" / "b00_02_sync_notebook_exports.py"
SPEC = importlib.util.spec_from_file_location("notebook_export_check_levels", MODULE_PATH)
export_check = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = export_check
SPEC.loader.exec_module(export_check)


class NotebookExportCheckLevelsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.notebook = nbformat.v4.new_notebook(cells=[
            nbformat.v4.new_markdown_cell("# 采集说明\n原文说明。"),
            nbformat.v4.new_code_cell(
                '"""保留这个正文说明。"""\n'
                'limit = 3\n'
                'def selected(value):\n'
                '    return value + limit\n'
                'raise RuntimeError("导出检查不得执行这段代码")\n'
            ),
        ])
        cls.notebook_text = nbformat.writes(cls.notebook)
        cls.export_source, _ = PythonExporter().from_notebook_node(cls.notebook)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "R02_Market_Data/a01_Collection/checks/tests")
        self.addCleanup(self.temp.cleanup)
        self.fixture_root = Path(self.temp.name)
        self.workflow_dirs = tuple(self.fixture_root / f"b{number:02d}" for number in range(1, 5))
        for directory in self.workflow_dirs:
            directory.mkdir()
        self.notebook_paths = []
        for index in range(19):
            notebook_path = self.workflow_dirs[index % 4] / f"c{index + 1:02d}_example.ipynb"
            notebook_path.write_text(self.notebook_text, encoding="utf-8", newline="\n")
            notebook_path.with_suffix(".py").write_text(self.export_source, encoding="utf-8", newline="\n")
            self.notebook_paths.append(notebook_path)
        self.script_path = self.notebook_paths[0].with_suffix(".py")

    def run_check(self, *arguments):
        output = io.StringIO()
        errors = io.StringIO()
        with (
            patch.object(export_check, "PROJECT_DIR", self.fixture_root),
            patch.object(export_check, "WORKFLOW_DIRS", self.workflow_dirs),
            patch.object(sys, "argv", [str(MODULE_PATH), *arguments]),
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            exit_code = export_check.main()
        return exit_code, output.getvalue(), errors.getvalue()

    def test_default_full_rejects_non_code_changes_while_code_warns_without_writing(self):
        self.script_path.write_text(
            self.export_source.replace("# 原文说明。", "# 旧的 Markdown 说明。"),
            encoding="utf-8", newline="\n",
        )
        before_bytes = self.script_path.read_bytes()
        before_mtime = self.script_path.stat().st_mtime_ns
        exit_code, output, errors = self.run_check()
        self.assertEqual(exit_code, 1)
        self.assertIn("check_level=full", output)
        self.assertIn("failed=1", output)
        self.assertIn("完整导出不一致", errors)
        exit_code, output, errors = self.run_check("--check", "--check-level", "code")
        self.assertEqual(exit_code, 0, errors)
        self.assertIn("WARN:", output)
        self.assertIn("passed=19; warnings=1; failed=0", output)
        self.assertIn("尚未完整同步", output)
        self.assertEqual(self.script_path.read_bytes(), before_bytes)
        self.assertEqual(self.script_path.stat().st_mtime_ns, before_mtime)

    def test_code_ignores_comments_cell_numbers_formatting_and_line_endings(self):
        formatted_source = self.export_source.replace("# In[ ]:", "# In[98]:").replace(
            "limit = 3", "limit=3  # 仅注释变化"
        ).replace("    return value + limit", "    return (value+limit)")
        self.script_path.write_bytes(formatted_source.replace("\n", "\r\n").encode("utf-8"))
        exit_code, output, errors = self.run_check("--check-level", "code")
        self.assertEqual(exit_code, 0, errors)
        self.assertIn("warnings=1", output)

    def test_code_rejects_docstring_literal_and_statement_changes(self):
        source_changes = (
            ("保留这个正文说明。", "更改了正文说明。"),
            ("limit = 3", "limit = 4"),
            ("return value + limit", "return value - limit"),
        )
        for old, new in source_changes:
            with self.subTest(change=new):
                self.script_path.write_text(
                    self.export_source.replace(old, new), encoding="utf-8", newline="\n"
                )
                exit_code, output, errors = self.run_check("--check-level", "code")
                self.assertEqual(exit_code, 1)
                self.assertIn("正文 AST", errors)
                self.assertIn("passed=18; warnings=0; failed=1", output)

    def test_code_rejects_missing_script_and_continues_other_checks(self):
        self.script_path.unlink()
        exit_code, output, errors = self.run_check("--check-level", "code")
        self.assertEqual(exit_code, 1)
        self.assertIn("FileNotFoundError", errors)
        self.assertIn("checked=19; passed=18", output)

    def test_code_rejects_syntax_and_invalid_statement_context(self):
        for invalid_source in ("def broken(:\n", "return 1\n"):
            with self.subTest(source=invalid_source):
                self.script_path.write_text(invalid_source, encoding="utf-8", newline="\n")
                exit_code, output, errors = self.run_check("--check-level", "code")
                self.assertEqual(exit_code, 1)
                self.assertIn("SyntaxError", errors)
                self.assertIn("failed=1", output)

    def test_invalid_notebook_export_blocks_all_writes(self):
        invalid_notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("return 1")])
        nbformat.write(invalid_notebook, self.notebook_paths[0])
        self.script_path.write_text("old = True\n", encoding="utf-8", newline="\n")
        script_bytes = {path.with_suffix(".py"): path.with_suffix(".py").read_bytes() for path in self.notebook_paths}
        exit_code, output, errors = self.run_check("--write", "--check-level", "code")
        self.assertEqual(exit_code, 1)
        self.assertIn("SyntaxError", errors)
        self.assertIn("exported=18; checked=0", output)
        self.assertEqual({path: path.read_bytes() for path in script_bytes}, script_bytes)

    def test_write_with_code_level_restores_full_default_export(self):
        self.script_path.write_text("old = True\n", encoding="utf-8", newline="\n")
        exit_code, output, errors = self.run_check("--write", "--check-level", "code")
        self.assertEqual(exit_code, 0, errors)
        self.assertIn("始终完整导出", output)
        self.assertIn("write_ok: True; check_level=full", output)
        for notebook_path in self.notebook_paths:
            self.assertEqual(notebook_path.with_suffix(".py").read_bytes(), self.export_source.encode("utf-8"))

    def test_write_postcheck_remains_strict_when_code_would_match(self):
        original_write_text = Path.write_text

        def write_with_comment(script_path, text, *args, **kwargs):
            if script_path == self.script_path:
                text += "\n# 模拟写后注释差异\n"
            return original_write_text(script_path, text, *args, **kwargs)

        with patch.object(Path, "write_text", write_with_comment):
            exit_code, output, errors = self.run_check("--write", "--check-level", "code")
        self.assertEqual(exit_code, 1)
        self.assertIn("write_ok: False; check_level=full", output)
        self.assertIn("完整导出不一致", errors)


if __name__ == "__main__":
    unittest.main()
