import logging
from typing import Any

import pandas as pd
from base import Worker
from constants import FNAME
from mrds import MyRedis
import sys,random


class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    # Write the code for the worker thread here.
    run = True
    while 1:
      if run:
        ans = rds.get_file(self.name)
        if len(ans)==0:
          run = False
          continue
        file = ans[0]
        mssg_id = ans[1]
        dict = counter(file)
        rds.redis_process(mssg_id,dict)
      else:
        pend = rds.pending()
        if pend==0:
          break
        ans = rds.claim_file(self.name)
        if len(ans)==0:
          continue
        file = ans[0]
        mssg_id = ans[1]
        dict = counter(file)
        rds.redis_process(mssg_id,dict)
    logging.info("Exiting")
      

def counter(file):
  chunks = pd.read_csv(file,chunksize=1000,usecols=['text'])
  dict = {}
  for chunk in chunks:
    list = chunk['text'].tolist()
    for text in list:
      words = text.split(" ")
      for word in words:
        if dict.get(word)==None:
          dict[word] = 1
        else:
          dict[word]+=1
  rand = random.randint(0,5)
  if rand==1:
    exit()
  return dict
        

