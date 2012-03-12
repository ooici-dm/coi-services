#!/usr/bin/env python

'''
@author David Stuebe <dstuebe@asascience.com>
@file ion/processes/data/ctd_stream_publisher.py
@description A simple example process which publishes prototype ctd data

To Run:
bin/pycc --rel res/deploy/r2dm.yml
pid = cc.spawn_process(name='ctd_test',module='ion.processes.data.ctd_stream_publisher',cls='SimpleCtdPublisher')

'''
from gevent.greenlet import Greenlet
from pyon.ion.endpoint import StreamPublisherRegistrar
from pyon.ion.process import StandaloneProcess
from pyon.public import log
from redis_coordination import RedisCoordination
from pyon.ion.transform import TransformDataProcess
from prototype.sci_data.stream_defs import ctd_stream_packet, SBE37_CDM_stream_definition, ctd_stream_definition
import redis
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from prototype.sci_data.constructor_apis import PointSupplementConstructor
from pyon.service.service import BaseService
import gevent
import time
import random
import uuid


class RedisCoordinationTransform(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input for an integer and adds 1 to it. If the transform
    has an output_stream it will publish the output on the output stream.

    This transform appends transform work in 'FS.TEMP/transform_output'
    '''
    def __init__(self, *args, **kwargs):
        super(RedisCoordinationTransform,self).__init__()


    def on_start(self):
        # with redis coordination....

        self.COMPARE_SET = 'compareset'

        self.conn = redis.StrictRedis('localhost', db=0)



    def process(self, packet):


        salt = random.normalvariate(mu=0,sigma=1)

        try:
            with RedisCoordination(rserver=self.conn, block_size=None, name='foo', timeout=None, packet=packet, log_id=None) as coordinator:

                for item in coordinator:
                    #if salt > 2.0:
                    #    time.sleep(1.0)
                    # a timeout here is just another kind of exception...

                    if salt < -2.0:
                        raise Exception('I suck')

                    result = self.conn.srem(self.COMPARE_SET, item)

                    print("RedisCoordinationTransform removed from %s, the item, %s" % (self.COMPARE_SET, item))

                    if not result:
                        print 'ERROR IN COMPARISON!!!! Tried to remove data that was not there!'

        except:
            # Keep running the test......
            pass



class RedisCoordinationPublisher(StandaloneProcess):
    def __init__(self, *args, **kwargs):
        super(StandaloneProcess, self).__init__(*args,**kwargs)
        #@todo Init stuff

    outgoing_stream_def = SBE37_CDM_stream_definition()


    def on_start(self):
        '''
        Creates a publisher for each stream_id passed in as publish_streams
        Creates an attribute with the name matching the stream name which corresponds to the publisher
        ex: say we have publish_streams:{'output': my_output_stream_id }
          then the instance has an attribute output which corresponds to the publisher for the stream
          in my_output_stream_id
        '''

        self.conn = redis.StrictRedis('localhost', db=0)
        self.COMPARE_SET = 'compareset'

        # Get the stream(s)
        stream_id = self.CFG.get_safe('process.stream_id',{})

        self.greenlet_queue = []

        self.stream_publisher_registrar = StreamPublisherRegistrar(process=self,node=self.container.node)
        # Needed to get the originator's stream_id
        self.stream_id= stream_id


        self.publisher = self.stream_publisher_registrar.create_publisher(stream_id=stream_id)


        g = Greenlet(self._trigger_func, stream_id)
        log.debug('Starting publisher thread for simple ctd data.')
        g.start()
        self.greenlet_queue.append(g)

    def on_quit(self):
        for greenlet in self.greenlet_queue:
            greenlet.kill()
        super(RedisCoordinationPublisher,self).on_quit()

    def _trigger_func(self, stream_id):

        while True:

            datum = str(uuid.uuid4())

            self.conn.sadd(self.COMPARE_SET, datum)

            log.warn('RedisCoordinationPublisher sending: %s\n' % datum)
            self.publisher.publish(datum)

            time.sleep(2.0)
