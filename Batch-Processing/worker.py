import logging
from typing import Any

import pandas as pd
from base import Worker
from constants import FNAME
from mrds import MyRedis

class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']
    # Write the code for the worker thread here.
    dict = {}
    while 1:
      file = rds.get_file()
      if file=="":
        break
      chunks = pd.read_csv(file,chunksize=1000,usecols=['text'])
      for chunk in chunks:
        list = chunk['text'].tolist()
        for text in  list:
          words = text.split(" ")
          for word in words:
            if dict.get(word)==None:
              dict[word]=1
            else:
              dict[word]=dict[word]+1

    for x in dict:
      rds.increment(dict[x],x)

    logging.info("Exiting")
