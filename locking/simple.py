from concurrent.futures import ThreadPoolExecutor
import time
import etcd


class EtcdLock(object):
    '''
    An Etcd lock object which.
    keeps on locking until released.
    '''

    def __init__(self, etcd_client, lock_name, lock_ttl):
        self.T_pool = ThreadPoolExecutor(max_workers=1)
        self._etcd_client = etcd_client
        self._lock_name = lock_name
        self._lock = etcd.Lock(self._etcd_client, self._lock_name)
        self._lock_ttl = lock_ttl
        self._is_locked = False
        self._sleep_interval = 3

    def _lock_loop(self):

        while self._is_locked:
            time.sleep(self._sleep_interval)
            self._lock.acquire(lock_ttl=self._lock_ttl)
        self._lock.release()

    def acquire(self):
        if self._is_locked:
            return
        while True:
            try:
                self._lock.acquire(blocking=True, lock_ttl=self._lock_ttl)
                break
            except etcd.EtcdLockExpired:
                '''
                We have been wating over the TTL but we need to retry to lock
                '''
                pass
        self._is_locked = True
        self._future = self.T_pool.submit(self._lock_loop)

    def release(self):
        self._is_locked = False
