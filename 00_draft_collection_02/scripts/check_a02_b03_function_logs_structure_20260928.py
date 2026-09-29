"""剥离日志后证明业务 AST、注释、单元格状态及非目标文件未改变。"""
import ast
import copy
import hashlib
import io
import json
import pathlib
import sys
import tokenize

root = pathlib.Path(__file__).resolve().parents[2]
snapshot = pathlib.Path(sys.argv[1])
relative = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b03_warehouse_receipt.ipynb')
before = json.loads((snapshot/relative).read_text(encoding='utf8'))
after = json.loads((root/relative).read_text(encoding='utf8'))
source = lambda nb: '\n\n'.join(''.join(c['source']) for c in nb['cells'] if c['cell_type']=='code')
logging_names = set()


class ReplaceReturnName(ast.NodeTransformer):
    def __init__(self, name, value): self.name, self.value = name, value
    def visit_Name(self, node):
        return copy.deepcopy(self.value) if node.id == self.name and isinstance(node.ctx, ast.Load) else node


class BusinessTree(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='click.echo': return None
        return self.generic_visit(node)
    def visit_Assign(self, node):
        if all(isinstance(t,ast.Name) and (t.id.startswith('log_') or t.id in logging_names) for t in node.targets): return None
        return self.generic_visit(node)
    def visit_AugAssign(self, node):
        if isinstance(node.target,ast.Name) and node.target.id.startswith('log_'): return None
        return self.generic_visit(node)
    def visit_If(self, node):
        if any(isinstance(n,ast.Name) and n.id=='log_last_progress_at' for n in ast.walk(node.test)): return None
        return self.generic_visit(node)
    def visit_For(self, node):
        if all(n.id.startswith('log_') for n in ast.walk(node.target) if isinstance(n,ast.Name)): return None
        return self.generic_visit(node)
    def visit_Try(self, node):
        if len(node.handlers)==1 and node.handlers[0].name=='log_error':
            assert isinstance(node.handlers[0].body[-1],ast.Raise) and node.handlers[0].body[-1].exc is None
            assert not node.orelse and not node.finalbody
            return self.generic_visit(node).body
        return self.generic_visit(node)
    def generic_visit(self,node):
        node=super().generic_visit(node)
        for field,value in ast.iter_fields(node):
            if not isinstance(value,list) or not value or not all(isinstance(x,ast.stmt) for x in value): continue
            result=[]
            for stmt in value:
                if isinstance(stmt,ast.Return):
                    while result and isinstance(result[-1],ast.Assign) and len(result[-1].targets)==1 and isinstance(result[-1].targets[0],ast.Name):
                        previous=result[-1];name=previous.targets[0].id
                        if not any(isinstance(n,ast.Name) and n.id==name for n in ast.walk(stmt)):break
                        result.pop();stmt=ReplaceReturnName(name,previous.value).visit(stmt)
                result.append(stmt)
            setattr(node,field,result)
        return node


old_code,new_code=source(before),source(after)
old_tree=BusinessTree().visit(ast.parse(old_code));new_tree=BusinessTree().visit(ast.parse(new_code))
if ast.dump(old_tree)!=ast.dump(new_tree):
    (snapshot/'business_before.py').write_text(ast.unparse(old_tree),encoding='utf8')
    (snapshot/'business_after.py').write_text(ast.unparse(new_tree),encoding='utf8')
    raise AssertionError('Business AST mismatch; see business_before/after.py')
comments=lambda s:[t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type==tokenize.COMMENT]
assert comments(old_code)==comments(new_code)
assert [c['id'] for c in before['cells']]==[c['id'] for c in after['cells']]
for old,new in zip(before['cells'],after['cells'],strict=True):
    assert {k:v for k,v in old.items() if k!='source'}=={k:v for k,v in new.items() if k!='source'},old['id']
assert {k:v for k,v in before.items() if k!='cells'}=={k:v for k,v in after.items() if k!='cells'}
old_functions={n.name:n for n in ast.parse(old_code).body if isinstance(n,ast.FunctionDef)}
new_functions={n.name:n for n in ast.parse(new_code).body if isinstance(n,ast.FunctionDef)}
assert old_functions.keys()==new_functions.keys()
for name in old_functions:
    assert ast.dump(old_functions[name].args)==ast.dump(new_functions[name].args),name
    assert ast.dump(old_functions[name].returns)==ast.dump(new_functions[name].returns),name
hashes=json.loads((snapshot/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,digest in hashes.items() if hashlib.sha256((root/p).read_bytes()).hexdigest()!=digest]
assert set(changed).issubset({relative.as_posix(),relative.with_suffix('.py').as_posix()}),changed
report={'business_ast_unchanged_after_removing_logging_and_inlining_return_captures':True,
        'original_comments_cell_ids_metadata_outputs_counts_preserved':True,
        'function_names_signatures_unchanged':True,
        'changed_production_files':changed}
(snapshot/'structure_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
