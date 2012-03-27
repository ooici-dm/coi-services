#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>

To Run:
@todo - fill this in!
'''


import redis
import random
from uuid import uuid4
import time

rserver = redis.StrictRedis(host='localhost', port=6379, db=0)

TESTSET = 'testset'
COMPARESET = 'compareset'



dsets = 3 # number of datasets

dset_max_size = 15 # Number of elements to put in the list before aggregation and chop


data_points = dset_max_size * 100

for i in xrange(data_points):


    #### ====== TEST CODE - DO NOT TOUCH
    # Simulate data coming from a message for a particular dataset
    new_data = str(uuid4())
    dataset_name = 'dataset_%d' % random.uniform(1, dsets)

    ### For comparison add the new data to the comparison set
    new_vals = rserver.sadd(COMPARESET, new_data)
    assert new_vals == 1, 'That UUID already existed!'
    #### ======


    # Put the new data into redis - modify if you like....
    current_length = rserver.rpush(dataset_name, new_data)
    print 'Current length: %d' % current_length



    # if we have accumulated enough values, do some processing
    #if current_length > 0 and current_length % dset_max_size == 0:
    if current_length % dset_max_size == 0:

        # Get the supplements from Redis that we are to process
        start = current_length - dset_max_size
        stop = current_length - 1
        print 'Getting Range: %d, %d; from dataset: %s' % (start, stop, dataset_name)
        data = rserver.lrange(dataset_name, start, stop)
        assert len(data)==dset_max_size, 'Got data length %d, expected: %d' % (len(data), dset_max_size)

        ### Do processing here blocking stuff that takes a while...

        #...... stuff will go here
        
        ###  ====== TEST CODE - DO NOT TOUCH
        # for now add it to a set so that we can tell if we have gotten this data before

        normal = random.normalvariate(0,1)
        if normal  > 2:
            time.sleep(0.1)
        elif normal > 3:
            continue

        new_values = rserver.sadd(TESTSET, *data)
        assert new_values == dset_max_size, 'Added the wrong number of values: new vals=%d, expected=%d' % (new_values, dset_max_size)
        ###  ======




        ### Once processing is complete, remove the supplements that we processed from REDIS
        # Now remove the data that we have processed from the list
        # Modify as needed
        print "keeping only greater than index: %d" % (stop + 1)
        result = rserver.ltrim(dataset_name, stop + 1, -1)
        assert result, 'Failed to clear the list'


    time.sleep(.01)


