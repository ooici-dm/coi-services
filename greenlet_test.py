#!/usr/bin/env python

from gevent.monkey import patch_all

patch_all()
import gevent
import redis
import uuid
from redis_coordination import RedisCoordination
import random
import time
import os


COMPARE_SET = 'compareset' # Key name for the set which holds the truth about all data added

def proc(id, points, block_size, timeout, flag):

    conn = redis.StrictRedis('localhost', db=0)

    for i in xrange(points):
        datum = str(uuid.uuid4())
        
        conn.sadd(COMPARE_SET, datum)

        salt = random.normalvariate(mu=0,sigma=1)

        try:
            with RedisCoordination(rserver=conn, block_size=block_size, name='foo', timeout=timeout, packet=datum, log_id=id) as coordinator:

                for item in coordinator:
                    #if salt > 2.0:
                    #    time.sleep(1.0)
                    # a timeout here is just another kind of exception...

                    if salt < -2.0:
                        raise RuntimeError('I suck')

                    result = conn.srem(COMPARE_SET, item)
                    if not result:
                        print 'ERROR IN COMPARISON!!!! Tried to remove data that was not there!'

        except:
            # Keep running the test......
            pass


    if flag:
        # Run a few more block sizes to make sure that any backup lists can be cleaned up.

        for i in xrange(block_size*2):
            datum = str(uuid.uuid4())

            conn.sadd(COMPARE_SET, datum)


            with RedisCoordination(rserver=conn, block_size=block_size, name='foo', timeout=timeout, packet=datum) as coordinator:
                for item in coordinator:

                    result = conn.srem(COMPARE_SET, item)
                    if not result:
                        print 'ERROR IN COMPARISON!!!! Tried to remove data that was not there!'



if __name__ == '__main__':
    import sys
    block_size = 15
    data_size = block_size * 800
    flag = None
    if len(sys.argv) > 1:
        flag = sys.argv[1]
        print 'Got Flag: "%s"' % flag

    g = gevent.Greenlet(proc, id=os.getpid(), points=data_size, block_size=block_size, timeout=0.4, flag=flag)
    g.start()

    gevent.joinall([g,])
    
        
    