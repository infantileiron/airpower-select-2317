import json
from datetime import datetime,timedelta
import pytest
from sqlalchemy import update
from src.models.entities import Run,User
from src.services.analysis import choose,compute_fold,blank_progress,FOLD_COUNT
from src.services import execution
from src.services.execution import claim,run_claimed,record_fold,publish,cancel,execute,recoverable_ids
from src.utils.security import hash_password,token
FEATURES=['pressure','flow','temperature','machines']
ALPHAS=[0.01,0.1,1.0]
SORTED=sorted(set(ALPHAS))
def make_rows(n=80):
 base=datetime(2026,11,1);rows=[]
 for i in range(n):
  rows.append({'hour':(base+timedelta(hours=i)).strftime('%Y-%m-%dT%H:00'),'pressure':round(6.5+0.3*((i*7)%5),4),'flow':8.0+(i%7),'temperature':20.0+(i%5),'machines':2+i%4,'sensor_noise':float((i%3)-1),'kwh':100.0+3*(i%7)+2*(i%4)+0.5*((i*7)%5),'note':''})
 return rows
def new_run(s,rows,features=FEATURES,alphas=ALPHAS):
 r=Run(name='断电续算验证',features=features,alphas=alphas,state='queued',result={'snapshot':rows},progress=blank_progress(alphas),events=[])
 s.add(r);s.commit();return r
def force_expire(s,run_id):
 s.execute(update(Run).where(Run.id==run_id).values(lease_until=0));s.commit()
def norm(x):return json.loads(json.dumps(x))
def test_resume_after_crash_matches_uninterrupted(db,monkeypatch):
 """算完未记进度时中断：接管进程只补未完成折，最终结果与不中断相同。"""
 rows=make_rows();expected=norm(choose(rows,FEATURES,ALPHAS));run=new_run(db,rows)
 epoch=claim(db,run.id,'worker-a');assert epoch==1
 progress=blank_progress(ALPHAS)
 progress['candidates'][0]['folds'][0]=compute_fold(rows,FEATURES,SORTED[0],0)
 assert record_fold(db,run.id,'worker-a',epoch,progress)
 compute_fold(rows,FEATURES,SORTED[0],1)  # 进程A算完第2折但未写回即崩溃
 force_expire(db,run.id)
 calls=[];orig=execution.compute_fold
 def spy(r,f,a,fi):calls.append((a,fi));return orig(r,f,a,fi)
 monkeypatch.setattr(execution,'compute_fold',spy)
 assert execute(db,run.id,'worker-b')
 assert (SORTED[0],0) not in calls,'已记录的折不得重算'
 assert len(calls)==len(ALPHAS)*FOLD_COUNT-1,'只补未完成折'
 final=db.get(Run,run.id)
 assert final.state=='done'
 assert norm({k:v for k,v in final.result.items() if k!='snapshot'})==expected
 kinds=[e['kind'] for e in final.events]
 assert 'takeover' in kinds and 'done' in kinds
 assert any('worker-a' in e['detail'] for e in final.events if e['kind']=='takeover')
def test_stale_worker_write_rejected(db):
 """接管后旧进程迟到写回无效，不污染新进程的结果。"""
 rows=make_rows();run=new_run(db,rows)
 epoch_a=claim(db,run.id,'worker-a')
 force_expire(db,run.id)
 epoch_b=claim(db,run.id,'worker-b');assert epoch_b==epoch_a+1
 stale=blank_progress(ALPHAS)
 stale['candidates'][0]['folds'][0]=compute_fold(rows,FEATURES,SORTED[0],0)
 assert record_fold(db,run.id,'worker-a',epoch_a,stale) is False,'旧代际进度写回必须失效'
 assert publish(db,run.id,'worker-a',epoch_a,{'fake':1},stale) is False,'旧代际发布必须失效'
 assert run_claimed(db,run.id,'worker-b',epoch_b)
 final=db.get(Run,run.id)
 assert final.state=='done' and 'fake' not in final.result
 assert norm({k:v for k,v in final.result.items() if k!='snapshot'})==norm(choose(rows,FEATURES,ALPHAS))
def test_cancel_racing_publish_single_terminal_state(db):
 """取消与完成竞争只保留一个终态；排队中也可取消。"""
 rows=make_rows()
 run=new_run(db,rows);epoch=claim(db,run.id,'worker-a')
 assert cancel(db,run.id,'analyst@demo.local','样本待修订')
 assert publish(db,run.id,'worker-a',epoch,{'x':1},blank_progress(ALPHAS)) is False
 final=db.get(Run,run.id)
 assert final.state=='cancelled' and final.cancel_reason=='样本待修订'
 assert cancel(db,run.id,'analyst@demo.local','重复取消') is False,'终态后不得再取消'
 run2=new_run(db,rows);epoch2=claim(db,run2.id,'worker-a')
 assert run_claimed(db,run2.id,'worker-a',epoch2)
 assert cancel(db,run2.id,'analyst@demo.local','迟到取消') is False
 assert db.get(Run,run2.id).state=='done'
 run3=new_run(db,rows)
 assert cancel(db,run3.id,'analyst@demo.local','排队取消')
 assert db.get(Run,run3.id).state=='cancelled'
 assert recoverable_ids(db)==[],'取消后不得再被恢复接管'
def test_concurrent_claim_and_recovery_boundary(db):
 """多进程并发接管：同一任务只有一个认领成功；租约有效的任务不被恢复扫描重复接管。"""
 rows=make_rows();run=new_run(db,rows)
 epoch=claim(db,run.id,'worker-a');assert epoch==1
 assert claim(db,run.id,'worker-b') is None,'租约有效时第二个进程不得接管'
 assert recoverable_ids(db)==[],'租约有效的running任务不得被恢复'
 force_expire(db,run.id)
 assert recoverable_ids(db)==[run.id],'租约过期后才允许被接管'
 assert claim(db,run.id,'worker-b')==2
 assert claim(db,run.id,'worker-c') is None
def test_cancelled_run_blocked_from_export_and_review(db):
 """取消后不发布结果、不能导出、不能新建评议；viewer不能取消。"""
 from fastapi.testclient import TestClient
 from src.main import app
 rows=make_rows();run=new_run(db,rows)
 assert cancel(db,run.id,'analyst@demo.local','样本待修订')
 db.add(User(email='analyst@demo.local',role='analyst',password=hash_password('x')));db.commit()
 u=db.query(User).filter_by(email='analyst@demo.local').first()
 h={'Authorization':'Bearer '+token(u)}
 with TestClient(app) as c:
  assert c.get('/runs/%d/export'%run.id,headers=h).status_code==409
  r=c.post('/reviews',json={'run_id':run.id,'title':'空载复核','finding':'模型观察到空载耗电偏高','action':'安排安全可控的试点'},headers=h)
  assert r.status_code==409
  assert c.post('/runs/%d/cancel'%run.id,json={'reason':'重复取消'},headers=h).status_code==409
  db.add(User(email='viewer@demo.local',role='viewer',password=hash_password('x')));db.commit()
  v=db.query(User).filter_by(email='viewer@demo.local').first()
  run2=new_run(db,rows)
  assert c.post('/runs/%d/cancel'%run2.id,json={'reason':'x'},headers={'Authorization':'Bearer '+token(v)}).status_code==403
