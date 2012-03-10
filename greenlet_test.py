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

def atomic_verify(conn, packet_block):
    for datum in packet_block:
        result = conn.srem('compareset', datum)

        if not result:

            print 'ERROR IN COMPARISON!!!! Tried to remove data that was not there!'

def proc(id, points, block_size, timeout, flag):

    conn = redis.StrictRedis('localhost', db=0)

    for i in xrange(points):
        datum = str(uuid.uuid4())
        
        conn.sadd('compareset', datum)

        salt = random.normalvariate(mu=0,sigma=1)

        try:
            with RedisCoordination(rserver=conn, block_size=block_size, name='foo', timeout=timeout, id=id) as coordinator:
                packet_block = coordinator.safe_lpush_item_rpop_range(datum)



                if packet_block:
                    if salt > 2.0:
                        time.sleep(1.0)

                    if salt < -2.0:
                        raise RuntimeError('I suck')

                    #gevent.Greenlet(atomic_verify,conn=conn,packet_block=packet_block).start()
                    atomic_verify(conn=conn, packet_block=packet_block)

        except:
            # Keep running...
            pass


    if flag:

        time.sleep(6)

        for i in xrange(block_size*20):
            datum = str(uuid.uuid4())

            conn.sadd('compareset', datum)


            with RedisCoordination(rserver=conn, block_size=block_size, name='foo', timeout=timeout) as coordinator:
                packet_block = coordinator.safe_lpush_item_rpop_range(datum)
                print 'backup cleanup: %s' % packet_block
                if packet_block:
                    atomic_verify(conn=conn, packet_block=packet_block)



if __name__ == '__main__':
    import sys
    block_size = 5 # prime number
    data_size = block_size * 400
    flag = None
    if len(sys.argv) > 1:
        flag = sys.argv[1]
        print 'Got Flag: "%s"' % flag

    g = gevent.Greenlet(proc, id=os.getpid(), points=data_size, block_size=block_size, timeout=1.0, flag=flag)
    g.start()

    gevent.joinall([g,])
    
        
    