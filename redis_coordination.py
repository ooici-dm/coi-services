#!/usr/bin/env python


'''
@author David Stuebe <dstuebe@asascience.com>
@author Luke Campbell
@author Swarbhanu Chatterjee

To Run:
@todo - fill this in!
'''


import redis
import random
from uuid import uuid4
import time
import gevent


class RedisCoordination(object):

    def __init__(self, rserver=None, name='', block_size=50, timeout=10):

        self._rserver = rserver # The redis connection

        self._block_size = block_size # the block size of the list to coordinate on

        self._base_name = name # The list name to use in redis

        self._timeout = timeout # the timeout at which point exit is called

        # Setup the internals of this method

        self._list_name = '%s.list' % self._base_name
        self._incr_name = '%s.incr' % self._base_name
        self._backups_set_name = '%s.set' % self._base_name

        self._my_backup_list_name = None
        self._my_backup_lock_name = None

    def __enter__(self ):
        self.timeout = gevent.Timeout(self._timeout)
        self.timeout.start()
        # Start the time out here...
        return self

    def __exit__(self, type, value, traceback):
        # cleanup and deal with any exception thrown during processing

        # push the temprary list back to the main list
        self.timeout.cancel()

        if value is None:
            self._rserver.srem(self._backups_set_name, self._my_backup_list_name)
            self._rserver.delete(self._my_backup_list_name)


    def call_me(self, item):
        self.internal_buffer = item
        # Do fancy stuff with item
        self.safe_lpush_item_rpop_range(item)

    def safe_lpush_item_rpop_range(self, item):
        # Coordination to get the range when the list is full goes here
        

        current_length = self._rserver.lpush(self._list_name, item)

        salt = random.normalvariate(mu=0,sigma=1)
        

        packet_block = None
        if current_length % self._block_size == 0:

            self._my_backup_list_name = '%s.%s.list' % (self._base_name, self._rserver.incr(self._incr_name))
            self._my_backup_lock_name = '%s.lock' % self._my_backup_list_name

            with self._rserver.pipeline() as pipe:

                while True:
                    try:
                        pipe.watch(self._list_name)
                        pipe.multi()
                        for i in xrange(self._block_size):
                            pipe.rpoplpush(self._list_name, self._my_backup_list_name)

                        # slow up the connection
                        if salt > 2:
                            gevent.sleep(0.2)
                        pipe.sadd(self._backups_set_name, self._my_backup_list_name)
                        pipe.setex(self._my_backup_lock_name, self._timeout*3, True)

                        packet_block = pipe.execute()[:-2]

                        self._rserver.delete(self._my_backup_lock_name)
                        break

                    except redis.WatchError:
                        print 'Watch Error'

                        continue

        else:
            # Check for backup lists that have expired!

            locks = self._rserver.smembers(self._backups_set_name)

            for lock_key in locks:
                timed_out =self._rserver.get(lock_key)

                if timed_out is None:
                    list_key = lock_key[:-5]
                    result = self._rserver.srem(self._backups_set_name, list_key)

                    if result is 1:
                        # Its our list to clean up

                        packet_block = self._rserver.lrange(list_key, 0, -1)


                    else:
                        # Someone else got there first!
                        pass

                
        return packet_block


if __name__ == "__main__":
    #-------------- This is a process --------------#
    #


    dsets = 1 # number of datasets

    dset_max_size = 15 # Number of elements to put in the list before aggregation and chop


    data_points = dset_max_size * 1000


    COMPARESET = 'compareset'

    rserver = redis.StrictRedis(host='localhost', port=6379, db=0)


    for i in xrange(data_points):

        new_data = str(uuid4())
        dataset_name = 'dataset_%d' % random.uniform(1, dsets)

        # For comparison purposes - to make sure we got everything
        new_vals = rserver.sadd(COMPARESET, new_data)


        with RedisCoordination(rserver=rserver, name=dataset_name, block_size=dset_max_size, timeout=15 ) as coordinator:

            packet_block = coordinator.safe_lpush_item_rpop_range(new_data)

            ###  Raise an exception here to test failure!

            if packet_block:

                for datum in packet_block:
                    result = rserver.srem(COMPARESET,datum)

                    assert result == 1






