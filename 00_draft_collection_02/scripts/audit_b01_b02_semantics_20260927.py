"""复核 b01/b02 语义审计修复；仅临时湖与模拟 API，不修改业务文件。"""

import pathlib
import sys
import unittest

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "00_draft_collection_02" / "tests"))
        break
else:
    raise RuntimeError("未找到项目根目录")

if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromName("test_b01_b02_commit_semantics")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
