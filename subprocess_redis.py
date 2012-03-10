#!/usr/bin/env python

import redis

import subprocess


if __name__ == '__main__':



    subprocess_pool = list()
    open_files = list()

    pool_size = 20
    for i in xrange(pool_size):
        print 'starting subprocess %d' % i
        job = 'bin/python'

        arg = 'greenlet_test.py'

        proc = [job, arg]
        if i==1:
            proc.append('1')
        f = open('/tmp/g%s'%i, 'w')
        out = f.fileno()
        open_files.append(f)

        subprocess_pool.append(subprocess.Popen(proc, shell=False, stdout=out, stderr=out))#=subprocess.PIPE, stderr=subprocess.PIPE ))



    for process in subprocess_pool:

        process.wait()
    for f in open_files:
        f.close()


    print "TADA!"
