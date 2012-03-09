import gevent
import redis
import uuid
from redis_coordination import RedisCoordination
import random

def atomic_verify(conn, packet_block):
    for datum in packet_block:
        result = conn.srem('compareset', datum)


def proc(conn, points, block_size, timeout):
    
    for i in xrange(points):
        datum = str(uuid.uuid4())
        
        conn.sadd('compareset', datum)
        
        
        with RedisCoordination(rserver=conn, block_size=block_size, timeout=timeout) as coordinator:
            packet_block = coordinator.safe_lpush_item_rpop_range(datum)
            if packet_block:
                gevent.Greenlet(atomic_verify,conn=conn,packet_block=packet_block).start()

            

if __name__ == '__main__':
    
    conn = redis.StrictRedis('localhost', db=0)
    greenlet_pool = list()
    block_size = 7 # prime number
    data_size = block_size * 20
    pool_size = 5
    for i in xrange(pool_size):
        print 'starting greenlet'
        g = gevent.Greenlet(proc,conn=conn, points=data_size, block_size=block_size, timeout=10)
        g.start()
        greenlet_pool.append(g)
    gevent.sleep(2)
    poor_bastard = random.randint(0,pool_size)
    greenlet_pool[poor_bastard].kill()
    print 'joining'
    gevent.joinall(greenlet_pool)
    
    
    
        
    