import datetime
from typing import Dict
import pandas as pd

class Datastore(object):
    '''A Datastore represents storage for any key-value pair.

        Datastores are general enough to be backed by all kinds of different storage:
        in-memory caches, databases, a remote datastore, flat files on disk, etc.

        The general idea is to wrap a more complicated storage facility in a simple,
        uniform interface, keeping the freedom of using the right tools for the job.
        In particular, a Datastore can aggregate other datastores in interesting ways,
        like sharded (to distribute load) or tiered access (caches before databases).

        While Datastores should be written general enough to accept all sorts of
        values, some implementations will undoubtedly have to be specific (e.g. SQL
        databases where fields should be decomposed into columns), particularly to
        support queries efficiently.

    '''

    # Main API. Datastore mplementations MUST implement these methods.

    def get_latest_row(self,keys:Dict[str,int]) -> pd.Series:
        raise NotImplementedError()
    
    def get_row_by_timestamp(self,keys:Dict[str,int],timestamp:datetime.date,timestamp_column:str) -> pd.Series:
        raise NotImplementedError()

    def put(self, value:pd.Series) -> None:
        raise NotImplementedError