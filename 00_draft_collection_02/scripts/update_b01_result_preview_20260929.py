"""Apply the b01 UI-result publication change to its Notebook, then export only b01."""
from pathlib import Path
import nbformat
from nbconvert.exporters import PythonExporter

root = Path(__file__).resolve().parents[2]
notebook_path = root / "02_Futures_Lakehouse/a01_Futures_Market_Data/b01_trade_calendar.ipynb"
notebook = nbformat.read(notebook_path, as_version=4)
changed = 0
for cell in notebook.cells:
    if cell.cell_type == "code" and "def collect(" in cell.source:
        old = "    validate_calendar_table(pandas_to_arrow(calendar_df, TRADE_CALENDAR_SCHEMA))\n"
        new = '''    validated_calendar_table = validate_calendar_table(pandas_to_arrow(calendar_df, TRADE_CALENDAR_SCHEMA))
    # 总控台只在本批 run_history 中指定此路径；结果预览不等于正式提交。
    # 独立运行 / Notebook 未设置该变量时不产生额外文件。
    import os
    preview_path_text = os.environ.get('LATITUDE_B01_PREVIEW_PATH')
    if preview_path_text:
        import pyarrow.parquet as pq
        preview_path = pathlib.Path(preview_path_text)
        temporary_preview_path = preview_path.with_suffix('.parquet.tmp')
        try:
            preview_path.parent.mkdir(parents=True, exist_ok=True)
            with temporary_preview_path.open('wb') as preview_stream:
                pq.write_table(validated_calendar_table, preview_stream)
                preview_stream.flush()
                os.fsync(preview_stream.fileno())
            os.replace(temporary_preview_path, preview_path)
        except OSError as preview_error:
            # 展示失败不能改变采集和正式事务的结果；日志保留可核查原因。
            click.echo(f'WARNING: 日历结果预览未保存；{type(preview_error).__name__}: {preview_error}')
'''
        if old not in cell.source and "preview_path_text" not in cell.source:
            raise RuntimeError("collect source has changed; inspect before retrying")
        cell.source = cell.source.replace(old, new, 1)
        changed += 1
    if cell.cell_type == "markdown" and cell.source.startswith("## JQData 交易日采集") and "总控台启动时" not in cell.source:
        cell.source += "\n\n总控台启动时，已验收的生成结果另存为本批 `run_history/.../artifacts/b01_generated.parquet`，供界面直接展示。该文件属于运行证据，不是 silver 提交；只读模式也可生成预览，预览失败只记录 warning。独立 CLI / Notebook 未设置控制面预览路径时不增加文件。"
if changed != 1:
    raise RuntimeError(f"Expected one collect definition; got {changed}")
notebook_path.write_text(nbformat.writes(notebook), encoding='utf-8', newline='\n')
exported, _ = PythonExporter().from_notebook_node(notebook)
notebook_path.with_suffix('.py').write_text(exported, encoding='utf-8', newline='\n')
assert notebook_path.with_suffix('.py').read_bytes() == exported.encode('utf-8')
print("b01 Notebook updated; default PythonExporter bytes verified; no business execution")
