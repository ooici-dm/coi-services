#!/usr/bin/env python

"""
@author David Stuebe <dstuebe@asascience.com>
@author Luke Campbell
@author Swarbhanu Chatterjee

Runs a single instance of a redis coordination test using a fixed name and known coordination set. Run in a distributed
test using subprocess_redis.py

To Run:
bin/python subprocess_redis.py
"""



from gevent.monkey import patch_all

patch_all()
import gevent
import redis
import uuid
from redis_coordination import RedisCoordination
import random


COMPARE_SET = 'compareset' # Key name for the set which holds the truth about all data in the test

def proc(points, block_size, timeout, flag):

    # Create a redis connection
    conn = redis.StrictRedis('localhost', db=0)


    for i in xrange(points):

        # Create some data for the test
        datum = str(uuid.uuid4())

        # Add it to redis as the authoritative set of data for the test
        conn.sadd(COMPARE_SET, datum)

        # Use the Redis Coordination as a WITH statement inside a try except block so that we can intentionally raise
        # exceptions as part of the test harness
        try:

            # Create the with statement passing in the redis connection and other paramters
            # name should be a known string shared between instances such as a stream_id or dataset_id
            with RedisCoordination(rserver=conn, block_size=block_size, name='foo', timeout=timeout, packet=datum) as coordinator:

                # The RedisCoordination class inherits from list. If a block size list of values is available for processing
                # they will be returned in the coordinator class which you can treat as a list.

                ###
                # Do processing of the data here!
                ###

                # Generate a random value to trip an exception for test purposes
                salt = random.normalvariate(mu=0,sigma=1)
                if salt < -2.0:
                    raise RuntimeError('Error raised at random as part of the test harness')
                #elif salt > 2.0:
                #    time.sleep(1.0)
                # a timeout here is just another kind of exception... but it makes the test really slow


                for item in coordinator:

                    # As part of the test harness remove each value in the coordinator from the set of Truth in redis
                    # if the test succeeds redis will be empty when the test is complete.
                    result = conn.srem(COMPARE_SET, item)
                    if not result:
                        raise KeyError('ERROR IN COMPARISON!!!! Tried to remove data that was not there!')

        except RuntimeError:
            # Keep running the test......
            pass


    if flag:
        # Run a few more block sizes without raising errors to make sure that any previous test failures can be cleaned up.

        for i in xrange(block_size*2):
            datum = str(uuid.uuid4())
            conn.sadd(COMPARE_SET, datum)

            with RedisCoordination(rserver=conn, block_size=block_size, name='foo', timeout=timeout, packet=datum) as coordinator:
                for item in coordinator:
                    result = conn.srem(COMPARE_SET, item)
                    if not result:
                        raise KeyError('ERROR IN COMPARISON!!!! Tried to remove data that was not there!')



if __name__ == '__main__':
    import sys
    block_size = 15
    data_size = block_size * 800
    flag = None
    if len(sys.argv) > 1:
        flag = sys.argv[1]
        print 'Got Flag: "%s"' % flag

    g = gevent.Greenlet(proc, points=data_size, block_size=block_size, timeout=0.4, flag=flag)
    g.start()

    gevent.joinall([g,])
    
        
    