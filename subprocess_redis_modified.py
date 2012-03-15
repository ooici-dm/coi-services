#!/usr/bin/env python

"""
@author David Stuebe <dstuebe@asascience.com>
@author Luke Campbell
@author Swarbhanu Chatterjee

Runs pool_size # of test cases of the redis_coordinator_test at one time demonstrating distributed state between them.

To Run:
bin/python subprocess_redis_modified.py
"""

import os
import subprocess
import time

if __name__ == '__main__':

    subprocess_pool = list()
    open_files = list()

    #------------------------------------------------------------------------
    # To launch services
    #------------------------------------------------------------------------

    print 'starting subprocess (all services: r2dm.yml)'
    job = 'bin/pycc'

    arg = '--rel res/deploy/r2dm.yml'

    proc = [job, arg]

    f = open('/tmp/g0', 'w')
    out = f.fileno()
    open_files.append(f)

    subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))

    time.sleep(5.0)

    #------------------------------------------------------------------------
    # To launch streams
    #------------------------------------------------------------------------

    print 'starting subprocess (redis_stream_launcher)'
    job = 'bin/pycc'

    arg = '--rel res/deploy/examples/redis_stream_launcher.yml'

    proc = [job, arg]

    f = open('/tmp/g1', 'w')
    out = f.fileno()
    open_files.append(f)

    subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))

    time.sleep(5.0)

    #------------------------------------------------------------------------
    # To launch redis coordination publishers
    #------------------------------------------------------------------------

    counter = 2
    pool_size = 2
    for i in xrange(counter,counter + pool_size):
        print 'starting subprocess (redis_publisher) %d' % i
        job = 'bin/pycc'

        arg = '--rel res/deploy/examples/redis_publisher.yml'

        proc = [job, arg]

        f = open('/tmp/g%s'%i, 'w')
        out = f.fileno()
        open_files.append(f)

        subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))

    #------------------------------------------------------------------------
    # To launch redis coordination consumers (transforms)
    #------------------------------------------------------------------------

    counter += pool_size
    print ("counter: %d" % counter)

    pool_size = 2
    for i in xrange(counter, counter + pool_size):
        print 'starting subprocess (redis_transform) %d' % i
        job = 'bin/pycc'

        arg = '--rel res/deploy/examples/redis_transform.yml'

        proc = [job, arg]

        f = open('/tmp/g%s'%i, 'w')
        out = f.fileno()
        open_files.append(f)

        subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))

    #------------------------------------------------------------------------
    # Wait for processes to end and then kill them
    #------------------------------------------------------------------------

    for process in subprocess_pool:

        process.wait()
    for f in open_files:
        f.close()

#    for process in subprocess_pool:
#        process.kill()

    print "TADA!"
