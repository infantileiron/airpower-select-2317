import os,sys,tempfile
os.environ['DATABASE_URL']='sqlite:///'+tempfile.mktemp(suffix='.db',prefix='airpower_test_')
os.environ['JWT_SECRET']='test-secret'
os.environ['REDIS_URL']='redis://localhost:6379/9'
os.environ['SEED_DEMO']='false'
sys.path.insert(0,os.path.join(os.path.dirname(__file__),'..'))
import pytest
from src.config.database import Base,engine,Session
@pytest.fixture()
def db():
 Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
 with Session() as s:yield s
