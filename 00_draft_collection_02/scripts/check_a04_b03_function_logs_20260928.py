"""a04/b03 函数日志：结构等价、分页证据、独立调用和正式复读失败边界。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import tokenize
from datetime import date, datetime, timezone
from unittest.mock import patch

import nbformat
from nbconvert.exporters import PythonExporter
from click.testing import CliRunner

ROOT=pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT=pathlib.Path(sys.argv[1])
REL=pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')
old,new=nbformat.read(SNAPSHOT/REL,4),nbformat.read(ROOT/REL,4)
source=lambda n:'\n\n'.join(c.source for c in n.cells if c.cell_type=='code')
draft=ast.parse((ROOT/'00_draft_collection_02/scripts/update_a04_b03_function_logs_20260928.py').read_text(encoding='utf8'))
selected=[n for n in draft.body if isinstance(n,(ast.ClassDef,ast.FunctionDef)) and n.name in {'RemoveLogging','canonical'}]
NAMED_RETURNS={'validated_calendar_df','validated_fact_df','empty_fact_df','merged_fact_df','updated_calendar_df'}
exec(compile(ast.Module(body=selected,type_ignores=[]),'<canonical>','exec'))
assert canonical(source(old))==canonical(source(new))
comments=lambda s:[t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type==tokenize.COMMENT]
assert comments(source(old))==comments(source(new)) and old.metadata==new.metadata
for a,b in zip(old.cells,new.cells,strict=True):
    assert {k:v for k,v in a.items() if k!='source'}=={k:v for k,v in b.items() if k!='source'}
    if '```mermaid' in a.source or a.id=='14766fcd':assert a.source==b.source
nbformat.validate(new)
exported,_=PythonExporter().from_notebook_node(new)
script=(ROOT/REL).with_suffix('.py')
assert script.read_bytes()==exported.encode('utf8') and b'\r\n' not in script.read_bytes()
ast.parse(exported)

spec=importlib.util.spec_from_file_location('macro_function_log_fixtures',ROOT/'00_draft_collection_02/tests/test_b04_c03_macro_release.py')
fixtures=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixtures)
M=fixtures.C03
checks=[]

def capture(name, operation, expected_error=None):
    output=io.StringIO();value=None;error=None
    with contextlib.redirect_stdout(output):
        try:value=operation()
        except Exception as exc:error=exc
    text=output.getvalue()
    (SNAPSHOT/(name+'.log')).write_text(text,encoding='utf8')
    if expected_error is None:
        assert error is None,(name,error,text)
    else:
        assert isinstance(error,expected_error),(name,error,text)
    return value,text

class PagedSession:
    def __init__(self,drift=False):self.calls=[];self.drift=drift
    def get(self,_url,*,params,timeout):
        page=params['pageNumber'];self.calls.append(dict(params))
        return fixtures.FakeResponse({'success':True,'result':{'pages':3 if self.drift and page==2 else 2,'count':3 if self.drift and page==2 else 2,'data':[fixtures.source_row('RPT_ECONOMY_CPI',date(2026,5+page,1))]}})

with patch.object(M,'EASTMONEY_PAGE_SIZE',1):
    session=PagedSession()
    rows,output=capture('two_pages',lambda:M.query_eastmoney_report_range(session,'RPT_ECONOMY_CPI',date(2026,6,1),date(2026,7,1)))
    assert len(rows)==2 and len(session.calls)==2
    assert 'page=1/2; page_rows=1; received_rows=1/2' in output
    assert 'page=2/2; page_rows=1; received_rows=2/2' in output
    assert output.count('phase=request; status=completed')==1
    assert 'persisted=true' not in output and 'normalized=true' not in output
    checks.append('two_pages_report_validated_page_and_total_counts')
    session=PagedSession(drift=True)
    _,output=capture('page_drift',lambda:M.query_eastmoney_report_range(session,'RPT_ECONOMY_CPI',date(2026,6,1),date(2026,7,1)),M.MacroReleaseRequestError)
    assert len(session.calls)==2
    assert 'phase=request; status=completed' not in output
    assert 'page=2; accepted_rows=1; partial_result_returned=false' in output
    checks.append('partial_pagination_failure_never_reports_request_complete')

for name,payload in [('metadata_empty',{'success':True,'result':{'pages':0,'count':0,'data':[]}}),('legacy_empty',{'success':False,'code':9201,'message':'返回数据为空'})]:
    session=type('EmptySession',(),{'get':lambda self,*a,**k:fixtures.FakeResponse(payload)})()
    rows,output=capture(name,lambda:M.query_eastmoney_report_range(session,'RPT_ECONOMY_CPI',date(2026,7,1),date(2026,7,1)))
    assert rows==[] and 'response_rows=0; empty_response=true; normalized=false; persisted=false' in output
checks.append('both_existing_empty_response_contracts_report_zero_without_claiming_confirmation')

with tempfile.TemporaryDirectory(prefix='macro-function-logs-') as directory:
    lake=pathlib.Path(directory)
    fixtures.build_calendar(lake,date(2026,7,31))
    calendar_path=lake/'silver'/M.CALENDAR_TABLE_NAME
    fact_path=lake/'silver'/M.TABLE_NAME
    calendar_df,output=capture('direct_read',lambda:M.read_macro_calendar(calendar_path))
    assert 'function=read_macro_calendar; phase=read; status=completed' in output
    fact_and_flag,output=capture('empty_fact',lambda:M.read_optional_fact(fact_path))
    assert 'rows=0; reason=no_parquet' in output
    plan,output=capture('direct_plan',lambda:M.plan_macro_release_grids(calendar_df,fact_and_flag[0]))
    assert 'function=plan_macro_release_grids; phase=plan; status=completed' in output
    pending=plan[0]
    cpi_codes={s.series_code for s in M.SERIES_BY_REPORT['RPT_ECONOMY_CPI']}
    cpi_pending=pending.loc[pending['series_code'].isin(cpi_codes)]
    converted,output=capture('direct_normalize',lambda:M.normalize_macro_release_response([fixtures.source_row('RPT_ECONOMY_CPI',date(2026,7,1))],'RPT_ECONOMY_CPI',cpi_pending,date(2026,7,1),date(2026,7,1),datetime.now(timezone.utc)))
    assert 'function=normalize_macro_release_response; phase=normalize; status=completed' in output
    assert 'fact_rows=9; empty_grids=0; normalized=true; persisted=false' in output
    fact_df,counts=converted
    generated,output=capture('direct_merge',lambda:M.full_fact_partition(fact_and_flag[0],fact_df,set(counts),(2026,7)))
    assert 'function=full_fact_partition; phase=generate; status=completed' in output
    committed,output=capture('direct_fact_commit',lambda:M.commit_complete_fact_partition(generated,lake,(2026,7)))
    assert 'function=commit_complete_fact_partition; phase=commit; status=completed' in output
    assert output.index('phase=formal_readback; status=completed')<output.index('phase=commit; status=completed')
    assert 'calendar_state=not_committed_by_this_function' in output
    updated,output=capture('direct_state',lambda:M.apply_calendar_completion(calendar_df,counts,{key:M.API_SUCCESS_REASON for key in counts},'function-log-check',datetime.now(timezone.utc)))
    assert 'changed_grids=9' in output and 'persisted=true' not in output
    _,output=capture('direct_calendar_commit',lambda:M.commit_calendar_partition(updated,lake,(M.DATASET_NAME,2026,7)))
    assert 'calendar_state=committed; collection_completion=per_grid' in output
    assert 'date_watermark=none' in output
    checks.append('direct_read_plan_normalize_merge_state_and_commit_report_their_own_progress')

    original_digest=M.table_digest
    for name,operation in [('upgrade',lambda:M.upgrade_fact_metadata(committed,lake)),('fact',lambda:M.commit_complete_fact_partition(committed,lake,(2026,7))),('calendar',lambda:M.commit_calendar_partition(updated,lake,(M.DATASET_NAME,2026,7)))]:
        before={p.relative_to(lake):p.read_bytes() for p in lake.rglob('*') if p.is_file()}
        calls=[]
        def mismatched_formal_digest(*args):
            calls.append(1)
            result=original_digest(*args)
            return 'injected-formal-mismatch' if len(calls)==3 else result
        with patch.object(M,'table_digest',side_effect=mismatched_formal_digest):
            _,output=capture(name+'_readback_failure',operation,ValueError)
        assert len(calls)==4
        assert 'phase=formal_readback; status=failed' in output
        assert 'phase=rollback; status=completed' in output
        assert 'phase=commit; status=completed' not in output and 'persisted=true' not in output
        assert before=={p.relative_to(lake):p.read_bytes() for p in lake.rglob('*') if p.is_file()}
    checks.append('all_three_commit_paths_restore_after_formal_readback_failure_without_success_logs')

    fake=fixtures.FakeEastmoneySession(fixtures.all_report_rows(date(2026,7,1)))
    with patch.object(M,'create_eastmoney_session',return_value=fake):
        result=CliRunner().invoke(M.main,['--lake-root',str(lake)])
    assert result.exit_code==0,result.exception
    assert 'committed:' not in result.output and 'persisted=true' not in result.output
    assert 'function=main; phase=request;' not in result.output
    assert result.output.splitlines()[0]==result.output.splitlines()[-1]=='='*80
    assert fake.closed
    checks.append('main_readonly_has_no_duplicate_request_or_commit_claims')

report={'business_ast_unchanged':True,'original_comments_and_notebook_state_preserved':True,'flowcharts_and_entry_unchanged':True,'default_export_exact':True,'checks':checks,'real_api_calls':0,'formal_lake_io':False,'sha256':hashlib.sha256(script.read_bytes()).hexdigest()}
(SNAPSHOT/'function_logs_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
