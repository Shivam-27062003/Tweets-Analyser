from __future__ import annotations

import logging
from typing import Optional, Final

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

  def get_file(self):
    top_stream = self.rds.xreadgroup(groupname=Worker.GROUP,consumername='c',count=1,streams={IN:'>'})
    if len(top_stream)==0:
      return ""
    b_file = top_stream[0][1][0][1][FNAME]
    file = b_file.decode()
    return file

  def increment(self,Amount,key):
    self.rds.zincrby(COUNT,Amount,key)

  def top(self, n: int) -> list[tuple[bytes, float]]:
    return self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n,
                                     withscores=True)