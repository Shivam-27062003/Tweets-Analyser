from __future__ import annotations
import logging
import json
from typing import Optional, Final
from redis.client import Redis
from base import Worker
from constants import IN, COUNT, FNAME


class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379, password='',
                       db=0, decode_responses=False)
    self.rds.flushall()
    self.rds.xgroup_create(IN, Worker.GROUP, id="0", mkstream=True)

  def get_timestamp(self) -> float:
    timestamp = self.rds.time()
    return float(f'{timestamp[0]}.{timestamp[1]}')

  def add_file(self, fname: str) -> None:
    self.rds.xadd(IN, {FNAME: fname})

  def top(self, n: int) -> list[tuple[bytes, float]]:
    return self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n,
                                     withscores=True)

  def get_latency(self) -> list[float]:
    lat = []
    lat_data = self.rds.hgetall("latency")
    for k in sorted(lat_data.keys()):
      v = lat_data[k]
      lat.append(float(v.decode()))
    return lat

  def read(self, worker: Worker) -> Optional[tuple[bytes, dict[bytes, bytes]]]:
      top_stream = self.rds.xreadgroup(groupname=Worker.GROUP,consumername=worker.name,count=1,streams={IN:'>'})
      if len(top_stream)==0:
        top_stream = self.rds.xautoclaim(IN,groupname=Worker.GROUP,consumername=worker.name,min_idle_time=2040,count=1)
        list = top_stream[1]
        if len(list)==0:
          return (None,None)
        data = list[0][1]
        id = list[0][0]
        return (id,data)
      data = top_stream[0][1][0][1]
      id = top_stream[0][1][0][0]
      return (id,data)

  def write(self, id: bytes, wc: dict[str, int]) -> None:
      if id==None:
        return
      ll = json.dumps(wc)
      epoch = int(id.decode().split('-')[0])
      argss = [COUNT, ll, Worker.GROUP, IN, id,epoch]
      self.rds.fcall("add_wc",0, *argss)

  def is_pending(self):
    ans = self.rds.xpending(IN,Worker.GROUP)['pending']
    return ans
