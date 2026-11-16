import os,time,json,logging,uuid
from pathlib import Path
import redis
from src.config.database import Session
from src.models.entities import Run
from src.services.execution import execute,recoverable_ids
WORKER='worker-'+uuid.uuid4().hex[:8]
r=redis.Redis.from_url(os.environ['REDIS_URL'])
QUEUE='airpower:runs';ENQUEUED='airpower:enqueued'
def enqueue(i):
 if r.sadd(ENQUEUED,i):r.rpush(QUEUE,i)
def recover():
 with Session() as s:ids=recoverable_ids(s)
 for i in ids:enqueue(i)
 if ids:logging.info('recover queued/stale runs %s',ids)
recover();last=time.time()
while True:
 Path('/tmp/heartbeat').touch()
 try:
  if time.time()-last>5:last=time.time();recover()
  item=r.blpop(QUEUE,timeout=3)
  if not item:continue
  rid=int(item[1]);r.srem(ENQUEUED,rid)
  with Session() as s:
   if execute(s,rid,WORKER):
    row=s.get(Run,rid)
    Path('/artifacts/'+str(rid)+'.json').write_text(json.dumps(row.result,ensure_ascii=False))
 except Exception:logging.exception('worker');time.sleep(2)
