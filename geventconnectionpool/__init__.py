import time
import gevent

from Queue import Full
from gevent.lock import Semaphore
from gevent.event import Event
from sqlalchemy.pool import Pool


class GeventConnectionPool(Pool):
    def __init__(self, creator, recycle=-1, echo=None, use_threadlocal=False, logging_name=None, reset_on_return=True, listeners=None, events=None, _dispatch=None, _dialect=None, max_connections=None, reserve_connections=None, connection_idle_timeout=30, is_full_event='wait'):
        super(GeventConnectionPool, self).__init__(
            creator, recycle=recycle, echo=echo, use_threadlocal=use_threadlocal, logging_name=logging_name, reset_on_return=reset_on_return,
            listeners=listeners, events=events, _dispatch=_dispatch, _dialect=_dialect)

        self.max_connections = max_connections or 5
        self.reserve_connections = reserve_connections or 1
        self.connection_idle_timeout = connection_idle_timeout
        self.is_full_event = is_full_event

        self._lock = Semaphore()
        self._available = list()
        self._inuse = list()
        self._timeouter = gevent.spawn_later(self.connection_idle_timeout, self._timeout)
        self._is_not_full = Event()

    def recreate(self):
        self.logger.info("Pool recreating")
        return self.__class__(
            self._creator, recycle=self._recycle, echo=self.echo, use_threadlocal=self._use_threadlocal, logging_name=self._orig_logging_name, reset_on_return=self._reset_on_return, _dispatch=self.dispatch,
            _dialect=self._dialect, max_connections=self.max_connections, reserve_connections=self.reserve_connections, connection_idle_timeout=self.connection_idle_timeout, is_full_event=self.is_full_event)

    def dispose(self):
        with self._lock:
            while self._available:
                t, conn = self._available.pop()
                conn.close()
        self.logger.info("Pool disposed.")

    def _timeout(self):
        try:
            with self._lock:
                while self._available and len(self._available) + len(self._inuse) > self.reserve_connections and self._available[-1][0] + self.connection_idle_timeout < time.time():
                    t, connection = self._available.pop(-1)
                    connection.close()
                    self.logger.info('closing timed out connection after %0.2f seconds (%s available, %s in use)', time.time() - t, len(self._available), len(self._inuse))
        finally:
            self._timeouter = gevent.spawn_later(self.connection_idle_timeout, self._timeout)

    def _do_get(self):
        while True:
            try:
                with self._lock:
                    if self._available:
                        t, connection = self._available.pop(0)
                        self.logger.debug('pop connection (%s available, %s in use)', len(self._available), len(self._inuse))
                    else:
                        if self.max_connections is not None and len(self._inuse) >= self.max_connections:
                            self._is_not_full.clear()
                            raise Full('max connections of %s reached' % self.max_connections)
                        connection = self._create_connection()
                        self.logger.info('new connection (%s available, %s in use)', len(self._available), len(self._inuse) + 1)
                    self._inuse.append(connection)
            except Full:
                if self.is_full_event == 'wait':
                    self.logger.warning('pool full. waiting for available slot (%s available, %s in use)', len(self._available), len(self._inuse))
                    self._is_not_full.wait()
                else:
                    raise
            else:
                return connection

    def _do_return_conn(self, connection):
        with self._lock:
            self.logger.debug('release connection (%s available, %s in use)', len(self._available), len(self._inuse))
            self._inuse.remove(connection)
            self._available.insert(0, (time.time(), connection))
            self._is_not_full.set()
