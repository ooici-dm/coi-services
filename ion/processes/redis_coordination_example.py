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
import time, hashlib
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


        host = self.CFG.get_safe('server.redis.host','localhost')
        db = self.CFG.get_safe('server.redis.db',0)

        log.warn("CFG: %s" % self.CFG)
        log.warn("host: %s" % host)
        log.warn("db: %s" % db)

#        host = '192.168.100.49'
#        db = 0

        self.conn = redis.StrictRedis(host, db=db)



    def process(self, packet):


        salt = random.normalvariate(mu=0,sigma=1)

        log.warn("Got packet: %s" % packet)

        name = packet['name']
        data = packet['value']

        try:
            with RedisCoordination(rserver=self.conn, block_size=5, name=name, timeout=10, packet=data) as coordinator:

                for item in coordinator:
                    #if salt > 2.0:
                    #    time.sleep(1.0)
                    # a timeout here is just another kind of exception...

                    if salt < -2.0:
                        raise Exception('I suck')

                    result = self.conn.srem(self.COMPARE_SET, item)

                    log.warn("RedisCoordinationTransform removed from %s, the item, %s" % (self.COMPARE_SET, item))

                    if not result:
                        print 'ERROR IN COMPARISON!!!! Tried to remove data that was not there!'

        except Exception as exc:
            # Keep running the test......
            log.warn("Got an exception:: %s" %  exc.message)


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

        host = self.CFG.get_safe('server.redis.host','localhost')
        db = self.CFG.get_safe('server.redis.db',0)

#        host = '192.168.100.49'
#        db = 0


        self.conn = redis.StrictRedis(host, db=db)
        self.COMPARE_SET = 'compareset'

        # Get the stream(s)
        stream_id = self.CFG.get_safe('process.stream_id',{})

        self.greenlet_queue = []

        self.stream_publisher_registrar = StreamPublisherRegistrar(process=self,node=self.container.node)

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

            packet_dict = {'name' : stream_id, 'value' : datum}

            log.warn('RedisCoordinationPublisher sending: %s\n' % packet_dict)
            self.publisher.publish(packet_dict)

#            time.sleep(2.0)