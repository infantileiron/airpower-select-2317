import time
from datetime import datetime,timezone
from sqlalchemy import update,or_
from src.models.entities import Run
from src.services.analysis import prepare,compute_fold,finalize,blank_progress,FOLD_COUNT
LEASE_SECONDS=30
def event(kind,actor,detail):return {'at':datetime.now(timezone.utc).isoformat(),'kind':kind,'actor':actor,'detail':detail}
def normalize_progress(progress,alphas):
 if progress and progress.get('candidates'):return progress
 return blank_progress(alphas)
def claim(s,run_id,worker_id):
 """原子认领排队任务或接管租约过期任务；成功返回代际epoch，失败返回None。"""
 t=time.time();row=s.get(Run,run_id)
 if not row or row.state not in ('queued','running'):return None
 old_owner,old_state=row.lease_owner,row.state
 stmt=(update(Run).where(Run.id==run_id).where(or_(Run.state=='queued',(Run.state=='running')&(Run.lease_until<t)))
  .values(state='running',lease_owner=worker_id,lease_until=t+LEASE_SECONDS,epoch=Run.epoch+1))
 if s.execute(stmt).rowcount!=1:s.rollback();return None
 s.expire_all();row=s.get(Run,run_id)
 kind='takeover' if old_state=='running' else 'claim'
 detail=('原计算进程 %s 租约过期，接管任务，只补未完成折'%old_owner) if kind=='takeover' else '认领排队任务，开始逐折计算'
 s.execute(update(Run).where(Run.id==run_id,Run.epoch==row.epoch).values(events=list(row.events or [])+[event(kind,worker_id,detail)]))
 s.commit();return row.epoch
def renew(s,run_id,worker_id,epoch):
 """续租；租约被接管或任务已终态时返回False。"""
 stmt=(update(Run).where(Run.id==run_id,Run.lease_owner==worker_id,Run.epoch==epoch,Run.state=='running')
  .values(lease_until=time.time()+LEASE_SECONDS))
 if s.execute(stmt).rowcount!=1:s.rollback();return False
 s.commit();return True
def record_fold(s,run_id,worker_id,epoch,progress):
 """逐折进度条件写回并续租；同折以(候选,折)为键覆盖，重试不重复计入。"""
 stmt=(update(Run).where(Run.id==run_id,Run.lease_owner==worker_id,Run.epoch==epoch,Run.state=='running')
  .values(progress=progress,lease_until=time.time()+LEASE_SECONDS))
 if s.execute(stmt).rowcount!=1:s.rollback();return False
 s.commit();return True
def publish(s,run_id,worker_id,epoch,result,progress):
 """全部折完成后一次条件更新发布done；与取消竞争只保留一个终态。"""
 stmt=(update(Run).where(Run.id==run_id,Run.lease_owner==worker_id,Run.epoch==epoch,Run.state=='running')
  .values(state='done',result=result,progress=progress,lease_until=None))
 if s.execute(stmt).rowcount!=1:s.rollback();return False
 s.expire_all();row=s.get(Run,run_id)
 s.execute(update(Run).where(Run.id==run_id,Run.state=='done').values(events=list(row.events or [])+[event('done',worker_id,'全部折完成，发布完整版本')]))
 s.commit();return True
def fail(s,run_id,worker_id,epoch,message):
 stmt=(update(Run).where(Run.id==run_id,Run.lease_owner==worker_id,Run.epoch==epoch,Run.state=='running')
  .values(state='failed',error=message,lease_until=None))
 if s.execute(stmt).rowcount!=1:s.rollback();return False
 s.expire_all();row=s.get(Run,run_id)
 s.execute(update(Run).where(Run.id==run_id,Run.state=='failed').values(events=list(row.events or [])+[event('failed',worker_id,message)]))
 s.commit();return True
def cancel(s,run_id,actor,reason):
 """取消排队或计算中任务；单条原子更新保证与完成竞争只保留一个终态。"""
 row=s.get(Run,run_id)
 if not row:return None
 stmt=(update(Run).where(Run.id==run_id,Run.state.in_(['queued','running']))
  .values(state='cancelled',cancel_reason=reason,lease_owner=None,lease_until=None))
 if s.execute(stmt).rowcount!=1:s.rollback();return False
 s.expire_all();row=s.get(Run,run_id)
 s.execute(update(Run).where(Run.id==run_id,Run.state=='cancelled').values(events=list(row.events or [])+[event('cancel',actor,reason)]))
 s.commit();return True
def run_claimed(s,run_id,worker_id,epoch):
 """在已持有租约的代际内逐折补算并发布；任何一步失去任务即放弃。"""
 row=s.get(Run,run_id);snapshot=(row.result or {}).get('snapshot') or []
 try:prepare(snapshot,row.features,row.alphas)
 except ValueError as e:fail(s,run_id,worker_id,epoch,str(e));return False
 progress=normalize_progress(row.progress,row.alphas)
 for cand in progress['candidates']:
  for fi in range(FOLD_COUNT):
   if cand['folds'][fi] is not None:continue
   if not renew(s,run_id,worker_id,epoch):return False
   cand['folds'][fi]=compute_fold(snapshot,row.features,cand['alpha'],fi)
   if not record_fold(s,run_id,worker_id,epoch,progress):return False
 result=finalize(snapshot,row.features,row.alphas,progress);result['snapshot']=snapshot
 return publish(s,run_id,worker_id,epoch,result,progress)
def execute(s,run_id,worker_id):
 epoch=claim(s,run_id,worker_id)
 if epoch is None:return False
 return run_claimed(s,run_id,worker_id,epoch)
def recoverable_ids(s):
 t=time.time()
 return [i for (i,) in s.query(Run.id).filter(or_(Run.state=='queued',(Run.state=='running')&(Run.lease_until<t))).all()]
