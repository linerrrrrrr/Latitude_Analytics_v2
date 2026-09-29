"""b01a 循环不变量差分与读取次数检查；仅模拟请求和临时 raw。"""
import ast
import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import tokenize
from unittest.mock import patch

import nbformat
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
PATH = ROOT/'02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01a_position_rank_special_case_calibration.py'
sys.path.insert(0, str(ROOT/'00_draft_collection_02/tests'))
from test_position_rank_special_case_calibration import FakeResponse, PositionRankSpecialCaseCalibrationTests


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def quiet(function, *args):
    with contextlib.redirect_stdout(io.StringIO()):
        return function(*args)


old = load('b01a_before_invariants', SNAPSHOT/PATH.name)
new = load('b01a_after_invariants', PATH)
fixture = PositionRankSpecialCaseCalibrationTests()
case, content = fixture.special_case(), fixture.response_content()
checks, metrics = [], {}
before = nbformat.read(SNAPSHOT/PATH.with_suffix('.ipynb').name, as_version=4)
after = nbformat.read(PATH.with_suffix('.ipynb'), as_version=4)
old_code = '\n\n'.join(c.source for c in before.cells if c.cell_type=='code')
new_code = '\n\n'.join(c.source for c in after.cells if c.cell_type=='code')
replacements = {name:ast.parse(expr, mode='eval').body for name,expr in {
    'official_instrument_id':'special_case["official_instrument_id"]',
    'log_source_row_count':'len(source_rows)',
    'resolved_raw_root':'raw_root.resolve()',
    'log_case_count':'len(POSITION_RANK_SPECIAL_CASES)',
}.items()}


class ExpandHoistedValues(ast.NodeTransformer):
    def visit_Assign(self, node):
        if len(node.targets)==1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id in replacements:
            return None
        return self.generic_visit(node)
    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load) and node.id in replacements:
            return copy.deepcopy(replacements[node.id])
        return node


assert ast.dump(ast.parse(old_code)) == ast.dump(ExpandHoistedValues().visit(ast.parse(new_code)))
checks.append('all code AST matches baseline after expanding exactly four hoisted values')
comments = lambda code:[t.string for t in tokenize.generate_tokens(io.StringIO(code).readline) if t.type==tokenize.COMMENT]
assert comments(old_code)==comments(new_code)
assert before.metadata==after.metadata and [c.id for c in before.cells]==[c.id for c in after.cells]
for a,b in zip(before.cells,after.cells,strict=True):
    assert {k:v for k,v in a.items() if k!='source'}=={k:v for k,v in b.items() if k!='source'}
checks.append('comments, cell IDs, metadata, outputs and execution counts preserved')


class CountingCase(dict):
    reads = 0
    def __getitem__(self, key):
        if key=='official_instrument_id':
            self.reads += 1
        return super().__getitem__(key)


rows = []
for label,module in (('before',old),('after',new)):
    counted_case = CountingCase(case)
    rows.append(quiet(module.parse_official_volume_rows,content,counted_case))
    metrics[label] = {'target_config_reads':counted_case.reads}
assert rows[0]==rows[1]==case['official_rows']
assert metrics['before']['target_config_reads']==21 and metrics['after']['target_config_reads']==1
checks.append('target contract lookup reduced from 21 to 1 with identical parsed Top 20')

payload = json.loads(content)
invalid = [b'not-json', json.dumps({'o_cursor':None}).encode(), json.dumps({'o_cursor':[]}).encode()]
for field,value in (('INSTRUMENTID','OTHER'),('RANK',True),('RANK',21),('PARTICIPANTABBR1',''),('CJ1',True),('CJ1',-1),('CJ1_CHG',1.5)):
    altered = copy.deepcopy(payload)
    altered['o_cursor'][0][field] = value
    invalid.append(json.dumps(altered).encode())
for contents in invalid:
    errors = []
    for module in (old,new):
        try:
            quiet(module.parse_official_volume_rows,contents,case)
        except Exception as error:
            errors.append((type(error),str(error)))
        else:
            raise AssertionError('invalid fixture accepted')
    assert errors[0]==errors[1]
checks.append('10 invalid response shapes/fields preserve rejection and original exception text')
assert quiet(new.validate_response,content,case)==quiet(old.validate_response,content,case)
checks.append('calibration manifest and frozen digest/content validation remain identical')


def files(root):
    return {p.relative_to(root).as_posix():p.read_bytes() for p in root.rglob('*') if p.is_file()}


with tempfile.TemporaryDirectory(prefix='b01a-invariants-check-') as temporary:
    temporary = pathlib.Path(temporary)
    artifacts = []
    cases = tuple(dict(case,case_id=f'case_{i}',raw_relative_path=f'shfe/position_rank_special_cases/case_{i}') for i in range(3))
    for label,module in (('before',old),('after',new)):
        lake = temporary/label
        raw = lake/'raw'
        root_resolves = []
        original_resolve = pathlib.Path.resolve
        def resolve(path,*args,**kwargs):
            if path==raw:
                root_resolves.append(str(path))
            return original_resolve(path,*args,**kwargs)
        with patch.object(module,'POSITION_RANK_SPECIAL_CASES',cases), patch.object(module.requests,'get',return_value=FakeResponse(content)) as requests, patch.object(pathlib.Path,'resolve',resolve), patch.object(module,'validate_response',wraps=module.validate_response) as validations, patch.object(module,'verify_artifacts',wraps=module.verify_artifacts) as verifies:
            result = CliRunner().invoke(module.main,['--lake-root',str(lake),'--write'])
        assert result.exit_code==0,(label,result.exception,result.output)
        assert requests.call_count==3 and validations.call_count==9 and verifies.call_count==6
        assert 'position_rank_special_cases_ready: true' in result.output
        assert 'committed_cases=3; source_only_cases=0' in result.output
        metrics[label].update(raw_root_resolves=len(root_resolves),request_calls=requests.call_count,response_validations=validations.call_count,artifact_verifications=verifies.call_count)
        artifacts.append(files(lake))
        (SNAPSHOT/f'{label}_three_cases.log').write_text(result.output,encoding='utf8')
        with patch.object(module,'POSITION_RANK_SPECIAL_CASES',cases), patch.object(module.requests,'get',side_effect=AssertionError('existing artifacts must not request')) as requests, patch.object(module,'verify_artifacts',wraps=module.verify_artifacts) as verifies:
            existing = CliRunner().invoke(module.main,['--lake-root',str(lake)])
        assert existing.exit_code==0,(label,existing.exception,existing.output)
        assert requests.call_count==0 and verifies.call_count==3
        assert 'existing_verified_cases=3; committed_cases=0; source_only_cases=0' in existing.output
        assert files(lake)==artifacts[-1]
    assert artifacts[0]==artifacts[1]
    assert metrics['before']['raw_root_resolves']==3 and metrics['after']['raw_root_resolves']==1
checks.append('three-case write preserves bytes, 3 requests and all 9 source/staging/formal validations; raw resolve 3 to 1')
checks.append('existing three-case read verifies once per case, does not request or rewrite')

nbformat.validate(after)
exported,_ = PythonExporter().from_notebook_node(after)
assert PATH.read_bytes()==exported.encode('utf8')
ast.parse(exported)
tree = ast.parse(new_code)
calls = {ast.unparse(n.func) for n in ast.walk(tree) if isinstance(n,ast.Call)}
assert not any(name.endswith(('.to_table','.groupby','.astype','.cast')) for name in calls)
assert not any(name.startswith(('pd.','pa.','ds.')) for name in calls)
checks.append('no DataFrame/Dataset/partition scans; target notebook validation, syntax and byte-exact export pass')
hashes = json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed = [name for name,digest in hashes.items() if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=digest]
expected = {str(PATH.relative_to(ROOT)),str(PATH.with_suffix('.ipynb').relative_to(ROOT))}
assert expected <= set(changed)
export_evidence = json.loads((SNAPSHOT/'export_verification.json').read_text(encoding='utf8'))
assert export_evidence['other_production_files_written']==0
report = dict(checks_passed=len(checks),checks=checks,metrics=metrics,changed_files_observed=changed,
              non_target_changes_observed=[name for name in changed if name not in expected],
              other_production_files_written_by_exporter=0,real_api_calls=0,formal_lake_writes=0)
(SNAPSHOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
