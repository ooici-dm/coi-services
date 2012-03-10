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

    def __init__(self, rserver=None, name='', block_size=50, timeout=10, packet=None, log_id=None):

        self._item = packet # A packet - the set of which we are trying to coordinate

        self._id = log_id

        self._rserver = rserver # The redis connection

        self._block_size = block_size # the block size of the list to coordinate on

        self._base_name = name # The list name to use in redis

        self._timeout = timeout # the timeout at which point exit is called
        # Setup the internals of this method

        self._list_name = '%s.list' % self._base_name
        self._backups_set_name = '%s.set' % self._base_name

        self._my_backup_list_name = None

        self._packet_block = []



    def __enter__(self ):

        self._safe_lpush_item_rpop_range()

        self.timeout = gevent.Timeout(self._timeout)
        self.timeout.start()
        # Start the time out here...
        return self

    def __exit__(self, type, value, traceback):
        # cleanup and deal with any exception thrown during processing

        # push the temprary list back to the main list
        self.timeout.cancel()

        if self._my_backup_list_name is not None:
            if value is None:
                # No exception raised
                #print '(ID:%s) EXIT: Cleaning up my backup list from set: %s' % (self._id, self._my_backup_list_name)
                self._rserver.delete(self._my_backup_list_name)

            else:
                # Exception raised - tell others about the backup list and abort!
                print '(ID:%s) EXIT: Raise exception %s; Saving backup list name: %s' % (self._id, value, self._my_backup_list_name)
                self._rserver.sadd(self._backups_set_name, self._my_backup_list_name)

    def __iter__(self):
        for item in self._packet_block:
            yield item


    def _safe_lpush_item_rpop_range(self):
        # Coordination to get the range when the list is full goes here

        #########
        # An error or network interruption occurring within the following block of code can cause problems!
        # =====================================
        current_length = self._rserver.lpush(self._list_name, self._item)
        if current_length % self._block_size == 0:
            queue_name = str(uuid4())[:13]
            self._my_backup_list_name = '%s.%s.list' % (self._base_name, queue_name)

            with self._rserver.pipeline() as pipe:

                pipe.multi()
                for i in xrange(self._block_size): 
                    pipe.rpoplpush(self._list_name, self._my_backup_list_name)

                self._packet_block = pipe.execute()
        # =====================================



        else:
            # Check for backup lists that need to be handled

            self._my_backup_list_name = self._rserver.spop(self._backups_set_name)

            if self._my_backup_list_name:
                self._packet_block = self._rserver.lrange(self._my_backup_list_name, 0, -1)

                print '(ID:%s) Got backup list name: %s; contents: %s' % (self._id, self._my_backup_list_name, self._packet_block)

