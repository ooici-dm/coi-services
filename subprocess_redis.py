#!/usr/bin/env python

"""
@author David Stuebe <dstuebe@asascience.com>
@author Luke Campbell
@author Swarbhanu Chatterjee

Runs pool_size # of test cases of the redis_coordinator_test at one time demonstrating distributed state between them.

To Run:
bin/python subprocess_redis.py
"""

import subprocess

if __name__ == '__main__':



    subprocess_pool = list()
    open_files = list()

    pool_size = 20
    for i in xrange(pool_size):
        print 'starting subprocess %d' % i
        job = 'bin/python'

        arg = 'redis_coordinator_test.py'

        proc = [job, arg]
        if i==1:
            proc.append('1')
        f = open('/tmp/g%s'%i, 'w')
        out = f.fileno()
        open_files.append(f)

        subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))



    for process in subprocess_pool:

        process.wait()
    for f in open_files:
        f.close()


    print "TADA!"
