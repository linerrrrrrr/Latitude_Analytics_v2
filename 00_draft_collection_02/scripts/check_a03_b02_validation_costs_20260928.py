"""统计真实执行调用量，不用模拟耗时推算生产性能。"""
import contextlib
import importlib.util
import io
import json
import pathlib
import shutil
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

ROOT=pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT=pathlib.Path(sys.argv[1])
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b02_domestic_spot_basis.py')
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
before=load('raw_cost_before',SNAPSHOT/RELATIVE)
after=load('raw_cost_after',ROOT/RELATIVE)
sys.path.insert(0,str(ROOT/'00_draft_collection_02/tests'))
import test_b03_c02_raw_staging_lifecycle as fixtures

NOW=datetime.now(timezone.utc)
class FrozenDatetime(datetime):
    @classmethod
    def now(cls,tz=None):return NOW

def measure(module,lake,call):
    target=lake/'silver'/module.CALENDAR_TABLE_NAME
    counts={'business_validations':0,'pandas_to_arrow':0,'arrow_to_pandas':0,
            'root_dataset_opens':0,'leaf_dataset_opens':0,'fragments_checked':0,
            'table_materializations':0,'raw_inspections':0,'plan_calls':0,'state_update_input_rows':[]}
    original_dataset=module.ds.dataset
    class DatasetCounter:
        def __init__(self,dataset):self.dataset=dataset
        def __getattr__(self,name):return getattr(self.dataset,name)
        def get_fragments(self,*args,**kwargs):
            for fragment in self.dataset.get_fragments(*args,**kwargs):
                counts['fragments_checked']+=1
                yield fragment
        def to_table(self,*args,**kwargs):
            counts['table_materializations']+=1
            return self.dataset.to_table(*args,**kwargs)
    def dataset(path,*args,**kwargs):
        if pathlib.Path(path)==target:
            counts['root_dataset_opens']+=1
        elif pathlib.Path(path).name.startswith('month='):
            counts['leaf_dataset_opens']+=1
        return DatasetCounter(original_dataset(path,*args,**kwargs))
    def counted(name,original):
        def call(*args,**kwargs):
            counts[name]+=1
            return original(*args,**kwargs)
        return call
    original_state=module.apply_calendar_completion
    def state(frame,*args,**kwargs):
        counts['state_update_input_rows'].append(len(frame))
        return original_state(frame,*args,**kwargs)
    with contextlib.ExitStack() as stack:
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        stack.enter_context(patch.object(module.ds,'dataset',side_effect=dataset))
        stack.enter_context(patch.object(module,'datetime',FrozenDatetime))
        stack.enter_context(patch.object(module.uuid,'uuid4',return_value=SimpleNamespace(hex='cost')))
        business='validate_calendar_table' if hasattr(module,'validate_calendar_table') else 'validate_calendar_frame'
        for name,key in [(business,'business_validations'),('pandas_to_arrow','pandas_to_arrow'),
                         ('arrow_to_pandas','arrow_to_pandas'),('inspect_raw_leaf','raw_inspections'),('plan_raw_grids','plan_calls')]:
            stack.enter_context(patch.object(module,name,side_effect=counted(key,getattr(module,name))))
        stack.enter_context(patch.object(module,'apply_calendar_completion',side_effect=state))
        call()
    return counts

def stored(lake):
    return {p.relative_to(lake).as_posix():p.read_bytes() for p in lake.rglob('*') if p.is_file()}

report={}
with tempfile.TemporaryDirectory(prefix='raw-cost-') as directory:
    temp=pathlib.Path(directory)
    rows=pd.concat([fixtures.pending_calendar_frame(date(2026,m,1)) for m in (5,6,7)],ignore_index=True)
    rows['updated_at']=NOW-timedelta(seconds=5)
    seed=temp/'leaf-seed'
    fixtures.write_exact_calendar(seed,rows)
    days=set(rows['observation_date'])
    outputs=[]
    for module,label in ((before,'before'),(after,'after')):
        lake=temp/('leaf-'+label)
        shutil.copytree(seed,lake)
        counts=measure(module,lake,lambda:module.commit_calendar_partitions(rows,days,lake))
        report.setdefault('three_leaf_commit',{})[label]=counts
        outputs.append(stored(lake))
    assert outputs[0]==outputs[1]
    old,new=report['three_leaf_commit']['before'],report['three_leaf_commit']['after']
    assert (old['business_validations'],new['business_validations'])==(9,3)
    assert (old['pandas_to_arrow'],new['pandas_to_arrow'])==(18,3)
    assert (old['root_dataset_opens'],new['root_dataset_opens'])==(3,0)
    assert (old['fragments_checked'],new['fragments_checked'])==(18,6)

    seed=temp/'main-seed'
    days=[date(2026,5,1),date(2026,5,2),date(2026,6,1),date(2026,7,1)]
    rows=pd.concat([fixtures.pending_calendar_frame(day) for day in days],ignore_index=True)
    rows['updated_at']=NOW-timedelta(seconds=5)
    with contextlib.redirect_stdout(io.StringIO()):
        evidence=after.commit_raw_response(seed/after.RAW_RELATIVE_ROOT,days[0],b'already-complete')
        rows=after.apply_calendar_completion(rows,{days[0]:evidence},'old-run',NOW-timedelta(seconds=2))
        after.commit_raw_response(seed/after.RAW_RELATIVE_ROOT,days[1],b'needs-state-repair')
    fixtures.write_exact_calendar(seed,rows)
    outputs=[]
    for module,label in ((before,'before'),(after,'after')):
        lake=temp/('main-'+label)
        shutil.copytree(seed,lake)
        session=fixtures.FakeSession(fixtures.FakeResponse(200,b'new-source'))
        with patch.object(module,'create_http_session',return_value=session):
            counts=measure(module,lake,lambda:module.main.callback(lake_root=lake,start_date=None,end_date=None,write=True))
        counts['http_calls']=session.get_count
        counts['session_closed']=session.closed
        report.setdefault('one_complete_one_repair_two_fetch',{})[label]=counts
        outputs.append(stored(lake))
    assert outputs[0]==outputs[1]
    old,new=report['one_complete_one_repair_two_fetch']['before'],report['one_complete_one_repair_two_fetch']['after']
    assert (old['plan_calls'],new['plan_calls'])==(3,1)
    assert (old['raw_inspections'],new['raw_inspections'])==(16,8)
    assert (old['root_dataset_opens'],new['root_dataset_opens'])==(6,1)
    assert new['business_validations']==3
    assert old['http_calls']==new['http_calls']==2
    assert new['state_update_input_rows']==[4,1,1]
report['same_formal_file_bytes']=True
report['real_api_calls']=0
report['formal_lake_writes']=0
report['note']='实测调用次数；不是生产耗时预测。保留每个新 raw 的 staging/正式摘要复读，以及每次变更后完整日历叶的单次业务校验。'
(SNAPSHOT/'cost_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
