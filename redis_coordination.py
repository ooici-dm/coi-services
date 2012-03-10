#!/usr/bin/env python


'''
@author David Stuebe <dstuebe@asascience.com>
@author Luke Campbell
@author Swarbhanu Chatterjee

To Run:
@todo - fill this in!
'''


import gevent

import redis
import random
from uuid import uuid4
import time


class RedisCoordination(object):

    def __init__(self, rserver=None, name='', block_size=50, timeout=10, id=None):

        self._id = id

        self._rserver = rserver # The redis connection

        self._block_size = block_size # the block size of the list to coordinate on

        self._base_name = name # The list name to use in redis

        self._timeout = timeout # the timeout at which point exit is called
        assert timeout >= 1, 'Timeout must be greater than 1.'
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

        if value is None and self._my_backup_list_name is not None:
            print '(ID:%s) EXIT: Cleaning up my backup list from set: %s' % (self._id, self._my_backup_list_name)

            result = self._rserver.srem(self._backups_set_name, self._my_backup_list_name)
            assert result is True, 'Failed to remove my list name from the set'

            result = self._rserver.delete(self._my_backup_list_name)
            assert result is True, 'Failed to remove my backup list'

            result = self._rserver.delete(self._my_backup_lock_name)
            assert result is True, 'Failed to remove my lock'



    def safe_lpush_item_rpop_range(self, item):
        # Coordination to get the range when the list is full goes here
        

        current_length = self._rserver.lpush(self._list_name, item)
        #print '(ID:%s) content_length: %d' % (self._id, current_length)

        salt = random.normalvariate(mu=0,sigma=1)


        packet_block = None
        if current_length % self._block_size == 0:
            queue_name = str(uuid4())

            self._my_backup_list_name = '%s.%s.list' % (self._base_name, queue_name)
            self._my_backup_lock_name = '%s.%s.lock' % (self._base_name, queue_name)

            with self._rserver.pipeline() as pipe:

                while True:
                    try:
                        pipe.watch(self._list_name)
                        pipe.multi()
                        for i in xrange(self._block_size):
                            pipe.rpoplpush(self._list_name, self._my_backup_list_name)

                        # slow up the connection
                        #if salt > 2:
                        #    gevent.sleep(0.6)
                        pipe.setex(self._my_backup_lock_name, int(self._timeout*2), True)
                        pipe.sadd(self._backups_set_name, self._my_backup_list_name)


                        packet_block = pipe.execute()[:-2]
                        if len(packet_block) != self._block_size:
                            print '(ID:%s) Error: packet size does not match block size.' % self._id
                        break

                    except redis.WatchError:
                        print '(ID:%s) Error: Watch Pipe 1' % self._id
                        continue
                    #except redis.exceptions.ResponseError:
                    #    print 'I got this'
                    #    continue

        else:
            # Check for backup lists that have expired!

            backup_lists = self._rserver.smembers(self._backups_set_name)

            for backup_list in backup_lists:
                backup_lock = '%s.lock' % backup_list[:-5]
                list_is_locked =self._rserver.exists(backup_lock)

                print '(ID:%s) List %s is %s locked %s' % (self._id, backup_list, list_is_locked, backup_lock)

                if not list_is_locked:
                    with self._rserver.pipeline() as pipe:
                        try:
                            pipe.watch(self._backups_set_name)
                            pipe.multi()
                            pipe.srem(self._backups_set_name, backup_list)
                            result = pipe.execute()

                        except redis.WatchError:
                            print '(ID:%s) Error: Watche Pipe 2... passing' % self._id
                            result = [False,]

                    print '(ID:%s) Removed backup list name "%s" from backup set. Result: "%s"' % (self._id, backup_list, result)
                    if result[0] is True:
                        # Its our list to clean up
                        self._my_backup_list_name = backup_list
                        self._my_backup_lock_name = backup_lock
                        packet_block = self._rserver.lrange(backup_list, 0, -1)

                        self._rserver.setex(self._my_backup_lock_name, int(self._timeout*2), 'True')
                        self._rserver.sadd(self._backups_set_name, self._my_backup_list_name)

                        print '(ID:%s) Got backup list name: %s; contents: %s' % (self._id, self._my_backup_list_name, packet_block)


                    else:
                        # Someone else got there first!
                        pass



                
        return packet_block

"""
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


"""



