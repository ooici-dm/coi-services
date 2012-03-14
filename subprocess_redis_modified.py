#!/usr/bin/env python

"""
@author David Stuebe <dstuebe@asascience.com>
@author Luke Campbell
@author Swarbhanu Chatterjee

Runs pool_size # of test cases of the redis_coordinator_test at one time demonstrating distributed state between them.

To Run:
bin/python subprocess_redis_modified.py
"""

import subprocess

if __name__ == '__main__':


    subprocess_pool = list()
    open_files = list()
    start =0

    #------------------------------------------------------------------------
    # To launch services
    #------------------------------------------------------------------------

    pool_size = 1
    for i in xrange(pool_size):
        print 'starting subprocess (all services: r2dm.yml) %d' % i
        job = 'bin/pycc'

        arg = '--rel res/deploy/r2dm.yml'

        proc = [job, arg]
        if i==1:
            proc.append('1')
        f = open('/tmp/g%s'%i, 'w')
        out = f.fileno()
        open_files.append(f)

        subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))

        start = i + 1

    #------------------------------------------------------------------------
    # To launch streams
    #------------------------------------------------------------------------

    pool_size = 1
    for i in xrange(start, start + pool_size):
        print 'starting subprocess (redis_stream_launcher) %d' % i
        job = 'bin/pycc'

        arg = '--rel res/deploy/examples/redis_stream_launcher.yml'

        proc = [job, arg]
        if i==1:
            proc.append('1')
        f = open('/tmp/g%s'%i, 'w')
        out = f.fileno()
        open_files.append(f)

        subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))

        start = i + 1

    #------------------------------------------------------------------------
    # To launch redis coordination publishers
    #------------------------------------------------------------------------

    pool_size = 20
    for i in xrange(start,start + pool_size):
        print 'starting subprocess (redis_publisher) %d' % i
        job = 'bin/pycc'

        arg = '--rel res/deploy/examples/redis_publisher.yml'

        proc = [job, arg]
        if i==1:
            proc.append('1')
        f = open('/tmp/g%s'%i, 'w')
        out = f.fileno()
        open_files.append(f)

        subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))

        start = i + 1


#
#    #------------------------------------------------------------------------
#    # To launch redis coordination consumers (transforms)
#    #------------------------------------------------------------------------
#
#
#    pool_size = 20
#    for i in xrange(start + 1 , pool_size):
#        print 'starting subprocess (redis_transform) %d' % i
#        job = 'bin/pycc'
#
#        arg = '--rel res/deploy/examples/redis_transform.yml'
#
#        proc = [job, arg]
#        if i==1:
#            proc.append('1')
#        f = open('/tmp/g%s'%i, 'w')
#        out = f.fileno()
#        open_files.append(f)
#
#        subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))
#
#        start = i

    #------------------------------------------------------------------------
    # Launch all the above processes
    #------------------------------------------------------------------------

    for process in subprocess_pool:

        process.wait()
    for f in open_files:
        f.close()


    print "TADA!"
