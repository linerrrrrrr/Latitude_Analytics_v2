"""实际调用次数与契约边界验证；有界临时湖，不调用 API 或写正式湖。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from click.testing import CliRunner

ROOT=pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT=pathlib.Path(sys.argv[1])
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b01_external_market_calendar.py')
sys.path.insert(0,str(ROOT/'00_draft_collection_02/tests'))
import test_b03_metadata_upgrade as fixtures

def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

old=load('external_before_validation_costs',SNAPSHOT/RELATIVE)
new=load('external_after_validation_costs',ROOT/RELATIVE)
NOW=datetime.now(timezone.utc)
class FrozenDatetime(datetime):
    @classmethod
    def now(cls,tz=None):
        return NOW
old.datetime=new.datetime=FrozenDatetime

def quiet(function,*args,**kwargs):
    with contextlib.redirect_stdout(io.StringIO()):
        return function(*args,**kwargs)

def hashes(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*.parquet')}

upstream=fixtures.build_trade_calendar_frame(new)
empty=new.empty_pandas(new.EXTERNAL_MARKET_CALENDAR_SCHEMA)
with patch.object(new,'pandas_to_arrow',wraps=new.pandas_to_arrow) as conversions, patch.object(new,'validate_external_calendar_table',wraps=new.validate_external_calendar_table) as business:
    expected=quiet(new.build_expected_calendar,upstream,empty,NOW)
    assert conversions.call_count==business.call_count==1

digest_cases=[]
for label,frame in (
    ('normal',expected),('empty',empty),('reordered',expected.iloc[::-1]),
    ('non_contiguous_index',expected.iloc[::2]),
):
    table=new.pandas_to_arrow(frame,new.EXTERNAL_MARKET_CALENDAR_SCHEMA)
    assert old.table_digest(frame)==new.table_digest(table),label
    chunked=new.pa.concat_tables([table.slice(0,len(table)//2),table.slice(len(table)//2)])
    assert old.table_digest(frame)==new.table_digest(chunked),label
    digest_cases.append(label)

invalid_cases=[]
for label,field,value in (
    ('negative_count','actual_record_count',-1),
    ('missing_reason','requirement_reason',''),
    ('unknown_entity','entity_code','not-configured'),
    ('bad_completion','is_fetch_completed',True),
    ('legacy_domestic_empty','fetch_result_status','empty_confirmed'),
):
    frame=expected.copy()
    domestic=frame.index[frame.dataset_name.eq('domestic_spot_basis') & frame.is_fetch_required][0]
    frame.loc[domestic,field]=value
    table=new.pandas_to_arrow(frame,new.EXTERNAL_MARKET_CALENDAR_SCHEMA)
    errors=[]
    for module in (old,new):
        try:
            quiet(module.validate_external_calendar_table,table,'fixture')
        except ValueError as error:
            errors.append(str(error))
        else:
            raise AssertionError(label)
    assert errors[0]==errors[1],(label,errors)
    invalid_cases.append(label)

metrics=[]
with tempfile.TemporaryDirectory(prefix='ext-cost-') as directory:
    base=pathlib.Path(directory)
    stored=[]
    for module,label in ((old,'before'),(new,'after')):
        root=base/label
        fixtures.write_partitioned_table(module,upstream,module.TRADE_CALENDAR_SCHEMA,module.UPSTREAM_PARTITION_COLUMNS,root/'silver'/module.UPSTREAM_TABLE_NAME)
        counts=Counter()
        original_dataset=module.ds.dataset
        class CountedDataset:
            def __init__(self,dataset,path):
                self.dataset=dataset
                name=pathlib.Path(path).name
                self.kind='staging' if name.startswith('.a03-b01-s-') else 'upstream' if name==module.UPSTREAM_TABLE_NAME else 'formal'
            def __getattr__(self,name):
                return getattr(self.dataset,name)
            def get_fragments(self,*args,**kwargs):
                counts[self.kind+'_fragment_passes']+=1
                return self.dataset.get_fragments(*args,**kwargs)
            def to_table(self,*args,**kwargs):
                counts[self.kind+'_reads']+=1
                if kwargs.get('filter') is not None:
                    counts[self.kind+'_filtered_reads']+=1
                return self.dataset.to_table(*args,**kwargs)
        def opened(path,*args,**kwargs):
            return CountedDataset(original_dataset(path,*args,**kwargs),path)
        with patch.object(module.ds,'dataset',side_effect=opened), patch.object(module,'pandas_to_arrow',wraps=module.pandas_to_arrow) as conversions, patch.object(module,'validate_external_calendar_table',wraps=module.validate_external_calendar_table) as business, patch.object(module.uuid,'uuid4',return_value=SimpleNamespace(hex='fixed_run')):
            result=CliRunner().invoke(module.main,['--lake-root',str(root),'--write'])
        assert result.exit_code==0,(result.output,result.exception)
        counts['pandas_to_arrow']=conversions.call_count
        counts['business_validations']=business.call_count
        metrics.append(dict(version=label,**counts))
        stored.append(hashes(root))
    assert stored[0]==stored[1]
    assert metrics[0]['staging_reads']==7 and metrics[1]['staging_reads']==1
    assert metrics[0]['formal_reads']==metrics[1]['formal_reads']==1
    assert metrics[1]['business_validations']==1
    assert metrics[1]['pandas_to_arrow']<metrics[0]['pandas_to_arrow']
    assert metrics[1]['upstream_fragment_passes']==metrics[1]['staging_fragment_passes']==metrics[1]['formal_fragment_passes']==1

    # 上游描述 metadata 过期可消费；表身份仍严格匹配。
    for label,identity_bad in (('stale_upstream',False),('wrong_identity',True)):
        root=base/label
        schema=fixtures.schema_with_stale_metadata(new.TRADE_CALENDAR_SCHEMA)
        if identity_bad:
            metadata=dict(schema.metadata)
            metadata[b'table_name']=b'other_table'
            schema=schema.with_metadata(metadata)
        fixtures.write_partitioned_table(new,upstream,new.TRADE_CALENDAR_SCHEMA,new.UPSTREAM_PARTITION_COLUMNS,root/'silver'/new.UPSTREAM_TABLE_NAME,write_schema=schema)
        result=CliRunner().invoke(new.main,['--lake-root',str(root)])
        assert (result.exit_code!=0)==identity_bad,(label,result.output,result.exception)
        if identity_bad:
            assert '表名、主键或分区 metadata' in str(result.exception)
        else:
            assert 'phase=generate; status=completed' in result.output

tree=ast.parse((ROOT/RELATIVE).read_text(encoding='utf8'))
functions={node.name:node for node in tree.body if isinstance(node,ast.FunctionDef)}
assert 'validate_upstream_table' not in functions
assert 'partition_expression' not in functions
assert 'dataset_has_exact_schema_metadata' not in functions
for name in ('changed_partition_keys','commit_partitions'):
    source=ast.unparse(functions[name])
    assert 'pd.Series(' not in source and '.eq(' not in source,name
report=dict(metrics=metrics,digest_cases=digest_cases,unchanged_output_validation_cases=invalid_cases,
            same_parquet_bytes=True,stale_upstream_metadata_allowed=True,wrong_table_identity_rejected=True,
            no_partition_full_frame_masks=True,real_api_calls=0,formal_lake_writes=0)
(SNAPSHOT/'cost_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
