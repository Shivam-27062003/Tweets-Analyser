from __future__ import annotations

import logging
from typing import Optional, Final
import json
from redis.client import Redis

from base import Worker
from constants import IN, COUNT, FNAME


class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379, password='pass',
                       db=0, decode_responses=False)
    self.rds.flushall()
    self.rds.xgroup_create(IN, Worker.GROUP, id="0", mkstream=True)

  def add_file(self, fname: str):
    self.rds.xadd(IN, {FNAME: fname})

  def get_file(self,name: str):
    top_stream = self.rds.xreadgroup(groupname=Worker.GROUP,consumername=name,count=1,streams={IN:'>'})
    if len(top_stream)==0:
      return ()
    file = top_stream[0][1][0][1][FNAME].decode()
    mssd_id = top_stream[0][1][0][0].decode()
    return (file,mssd_id)
  
  def claim_file(self,name: str):
    top_stream = self.rds.xautoclaim(IN,groupname=Worker.GROUP,consumername=name,min_idle_time=2040,count=1)
    list = top_stream[1]
    if len(list)==0:
      return ()
    f_name = list[0][1][FNAME].decode()
    mssg_id = list[0][0].decode()
    return (f_name,mssg_id)

  def pending(self):
    ans = self.rds.xpending(IN,Worker.GROUP)['pending']
    return ans
  
  def redis_process(self,msg_id,word_counts):
    ll = json.dumps(word_counts)
    argss = [COUNT, ll, Worker.GROUP, IN, msg_id]
    self.rds.fcall("sum_up",0, *argss)
    
  def increment(self,Amount,key):
    self.rds.zincrby(COUNT,Amount,key)

  def top(self, n: int) -> list[tuple[bytes, float]]:
    return self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n,
                                     withscores=True)