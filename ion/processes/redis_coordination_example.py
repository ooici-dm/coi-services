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
from pyon.ion.process import StandaloneProcess
from prototype.sci_data.stream_defs import ctd_stream_packet, SBE37_CDM_stream_definition, ctd_stream_definition
import redis
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.objects import ProcessDefinition, StreamQuery
from pyon.service.service import BaseService


class RedisCoordinationExample(TransformDataProcess):
    ''' A basic transform that receives input through a subscription,
    parses the input for an integer and adds 1 to it. If the transform
    has an output_stream it will publish the output on the output stream.

    This transform appends transform work in 'FS.TEMP/transform_output'
    '''
    def __init__(self, *args, **kwargs):
        super(RedisCoordinationExample,self).__init__()

        self.dataset_name = 'dataset1'


    def process(self):
        # with redis coordination....

        dsets = 1 # number of datasets

        dset_max_size = 15 # Number of elements to put in the list before aggregation and chop


        data_points = dset_max_size * 1000


        COMPARESET = 'compareset'

        rserver = redis.StrictRedis(host='localhost', port=6379, db=0)

        #-----------------------
        # Set up subscribers
        #-----------------------

        pubsub_cli = PubsubManagementServiceClient(node=self.container.node)

        stream = pubsub_cli.create_stream()

        subscription_id = pubsub_cli.create_subscription(
            query=StreamQuery([stream]),
            exchange_name='redis_queue',
            name='Subscription',
            description='Description'
        )

        def message_received(message, headers):

            log.warn("message received: %s" % message )

            with RedisCoordination(rserver=rserver, name=self.dataset_name, block_size=dset_max_size, timeout=15 ) as coordinator:

                packet_block = coordinator.safe_lpush_item_rpop_range(new_data)

                ###  Raise an exception here to test failure!

                if packet_block:

                    for datum in packet_block:
                        result = rserver.srem(COMPARESET,datum)

                        assert result == 1

        subscriber_registrar = StreamSubscriberRegistrar(process=self.container, node=self.container.node)
        subscriber = subscriber_registrar.create_subscriber(exchange_name='redis_queue', callback=message_received)

        # Start subscribers
        subscriber.start()

        # Activate subscriptions
        pubsub_cli.activate_subscription(subscription_id)

        #-------------------------------------------
        # Set up publisher
        #-------------------------------------------

        redis_publisher =  RedisDataPublisher()

        for i in xrange(data_points):

            new_data = str(uuid4())

            # For comparison purposes - to make sure we got everything
            new_vals = rserver.sadd(COMPARESET, new_data)

            redis_publisher._publish(new_data)

        subscriber.stop()

class RedisDataPublisher(StandaloneProcess):
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

        # Get the stream(s)
        stream_id = self.CFG.get_safe('process.stream_id',{})

        self.greenlet_queue = []


        self.stream_publisher_registrar = StreamPublisherRegistrar(process=self,node=self.container.node)
        # Needed to get the originator's stream_id
        self.stream_id= stream_id


        self.publisher = self.stream_publisher_registrar.create_publisher(stream_id=stream_id)

    def on_quit(self):

        super(RedisDataPublisher,self).on_quit()


    def _publish(self, msg):

        log.warn("publishing message: %s" % msg)

        self.publisher.publish(msg)
