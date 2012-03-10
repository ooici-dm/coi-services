
#from gevent.monkey import patch_all

#patch_all()
#import gevent
import redis
import uuid
from redis_coordination import RedisCoordination
import random
import time
import subprocess

def atomic_verify(conn, packet_block):
    for datum in packet_block:
        result = conn.srem('compareset', datum)


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
                    #if salt > 2.0:
                    #    gevent.sleep(.7)

                    #if salt < -2.0:
                    #    raise RuntimeError('Shit happened')

                    #gevent.Greenlet(atomic_verify,conn=conn,packet_block=packet_block).start()

        except:
            # Keep running...
            pass


    if flag:

        time.sleep(6)

        for i in xrange(block_size):
            datum = str(uuid.uuid4())

            conn.sadd('compareset', datum)


            with RedisCoordination(rserver=conn, block_size=block_size, name='foo', timeout=timeout) as coordinator:
                packet_block = coordinator.safe_lpush_item_rpop_range(datum)
                print 'backup cleanup: %s' % packet_block
                if packet_block:
                    gevent.Greenlet(atomic_verify,conn=conn,packet_block=packet_block).start()



if __name__ == '__main__':
    

    greenlet_pool = list()
    block_size = 15 # prime number
    data_size = block_size * 40
    pool_size = 4
    for i in xrange(pool_size):
        print 'starting greenlet'
        g = gevent.Greenlet(proc, id=i, points=data_size, block_size=block_size, timeout=2, flag= i==0)
        g.start()
        greenlet_pool.append(g)
    gevent.sleep(2)
    poor_bastard = random.randint(0,pool_size-1)
    #greenlet_pool[poor_bastard].kill()
    print 'joining'
    gevent.joinall(greenlet_pool)
    print 'Joined!!!'
    
    
        
    