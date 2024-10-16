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

    def get(self, key) -> pd.DataFrame:
        '''Return the object named by key or None if it does not exist.

        None takes the role of default value, so no KeyError exception is raised.

        Args:
            key: Key naming the object to retrieve

        Returns:
            object or None
        '''
        raise NotImplementedError

    def put(self, key, value:pd.DataFrame) -> None:
        '''Stores the object `value` named by `key`.

        How to serialize and store objects is up to the underlying datastore.
        It is recommended to use simple objects (strings, numbers, lists, dicts).

        Args:
            key: Key naming `value`
            value: the object to store.
        '''
        raise NotImplementedError

    def delete(self, key) -> None:
        '''Removes the object named by `key`.

        Args:
            key: Key naming the object to remove.
        '''
        raise NotImplementedError

    def query(self, query) -> pd.DataFrame:
        '''Returns an iterable of objects matching criteria expressed in `query`

        Implementations of query will be the largest differentiating factor
        amongst datastores. All datastores **must** implement query, even using
        query's worst case scenario, see :ref:class:`Query` for details.

        Args:
            query: Query object describing the objects to return.

        Raturns:
            iterable cursor with all objects matching criteria
        '''
        raise NotImplementedError

    # Secondary API. Datastores MAY provide optimized implementations.

    def contains(self, key) -> bool:
        '''Returns whether the object named by `key` exists.

        The default implementation pays the cost of a get. Some datastore
        implementations may optimize this.

        Args:
            key: Key naming the object to check.

        Returns:
            boalean whether the object exists
        '''
        return self.get(key) is not None
