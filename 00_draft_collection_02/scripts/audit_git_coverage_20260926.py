"""Prepare an explicit code/document push inventory; never print secret values."""
from collections import Counter
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT / "00_draft_collection_02/git_coverage_20260926"
REPORT_DIR.mkdir(exist_ok=True)


def git_paths(*arguments):
    output = subprocess.check_output(["git", *arguments, "-z"], cwd=ROOT)
    return [item.decode("utf-8") for item in output.split(b"\0") if item]


untracked = git_paths("ls-files", "--others", "--exclude-standard")
modified = git_paths("diff", "HEAD", "--name-only")
tracked = set(git_paths("ls-files"))
source_suffixes = {".py", ".ipynb", ".ps1", ".bat", ".cmd", ".sh", ".sql", ".js", ".ts", ".toml", ".yml", ".yaml", ".ini", ".snapshot"}
document_suffixes = {".md", ".rst", ".txt", ".pdf", ".bib", ".tex", ".docx", ".pptx", ".svg"}
runtime_parts = {"run_status", "run_history", "runs", "results", "output", "outputs", "migration_reports", "git_coverage_20260926", "lakehouse_layout_20260926"}
data_suffixes = {".parquet", ".arrow", ".feather", ".duckdb", ".sqlite", ".db", ".npz", ".npy", ".pkl", ".pickle", ".zip", ".gz", ".xz", ".pid", ".jsonl", ".sha256"}
secret_values = []
for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
    name, separator, value = line.partition("=")
    value = value.strip().strip("\"'")
    if separator and re.search(r"password|passwd|token|secret|api_key", name, re.I) and len(value) >= 8:
        secret_values.append(value)

secret_patterns = {
    "literal_auth_credentials": re.compile(r"\b(?:auth|authenticate|login)\s*\(\s*['\"][^'\"\n]{3,}['\"]\s*,\s*['\"]([^'\"\n]{4,})['\"]", re.I),
    "literal_token_call": re.compile(r"\b(?:set_token|pro_api)\s*\(\s*['\"]([^'\"\n]{20,})['\"]", re.I),
    "literal_secret_assignment": re.compile(r"\b(?:\w*(?:password|passwd|api_key|access_token|secret_key)|token|pwd)\s*(?::\s*str\s*)?=\s*['\"]([^'\"\n]{6,})['\"]", re.I),
    "private_key": re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----"),
    "provider_token": re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}|sk-proj-[A-Za-z0-9_-]{30,})\b"),
}
placeholder = re.compile(r"example|placeholder|your[_ -]|test|dummy|fake|changeme|password|secret|token|<|>|\{", re.I)

selected = []
excluded = []
findings = []
for relative in sorted(set(untracked + modified)):
    path = ROOT / relative
    if not path.exists():
        selected.append(relative)
        continue
    suffix = path.suffix.lower()
    parts = set(path.relative_to(ROOT).parts)
    reason = None
    if "git_coverage_20260926" in parts or "lakehouse_layout_20260926" in parts:
        reason = "local_audit_or_backup"
    elif suffix in data_suffixes or "futures_lake_pre_rebuild_20260810" in parts:
        reason = "data_or_archive"
    elif parts & runtime_parts:
        reason = "runtime_output"
    elif "experiment_records" in parts and suffix != ".md":
        reason = "experiment_runtime_evidence"
    elif "_before_" in path.name:
        reason = "local_backup"
    elif path.name in {".gitignore", ".gitattributes", ".env.template", ".env.example"}:
        pass
    elif suffix in source_suffixes | document_suffixes:
        pass
    elif suffix == ".json" and any(part in parts for part in ("literature", "references", "archived_batches")):
        pass
    elif path.name == "attachment_manifest.json":
        pass
    elif suffix in {".png", ".html", ".csv"} and ("references" in parts or "papers" in parts or "literature" in parts or path.name == "03_来源清单.csv"):
        pass
    else:
        reason = "data_or_generated_artifact"
    if reason:
        excluded.append({"path": relative, "reason": reason, "bytes": path.stat().st_size, "tracked": relative in tracked})
        continue
    if suffix not in {".pdf", ".png", ".docx", ".pptx"}:
        content = path.read_text(encoding="utf-8-sig", errors="replace")
        if suffix == ".ipynb":
            notebook = json.loads(content)
            content = "\n".join("".join(cell.get("source", [])) + "\n" + json.dumps(cell.get("outputs", []), ensure_ascii=False) for cell in notebook["cells"])
        file_findings = []
        for rule, pattern in secret_patterns.items():
            for match in pattern.finditer(content):
                if match.lastindex and placeholder.search(match.group(1)):
                    continue
                file_findings.append({"rule": rule, "line": content.count("\n", 0, match.start()) + 1})
        for value in secret_values:
            if value in content:
                file_findings.append({"rule": "matches_local_env_secret", "line": content.count("\n", 0, content.index(value)) + 1})
        if file_findings:
            findings.append({"path": relative, "findings": file_findings})
            excluded.append({"path": relative, "reason": "credential_review", "bytes": path.stat().st_size, "tracked": relative in tracked})
            continue
    selected.append(relative)

report = {
    "untracked_before": len(untracked),
    "selected": selected,
    "excluded": excluded,
    "credential_findings": findings,
    "selected_by_root": dict(Counter(Path(p).parts[0] for p in selected)),
    "excluded_by_reason": dict(Counter(p["reason"] for p in excluded)),
    "previously_tracked_data_count": sum(Path(p).suffix.lower() in data_suffixes for p in tracked),
}
(REPORT_DIR / "coverage.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
(REPORT_DIR / "selected.paths").write_bytes(b"\0".join(p.encode("utf-8") for p in selected) + b"\0")
print(json.dumps({k: v for k, v in report.items() if k not in {"selected", "excluded"}}, ensure_ascii=False, indent=2))
