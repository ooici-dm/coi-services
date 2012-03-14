from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.ion.process import StandaloneProcess
from pyon.public import log, RT
from interface.objects import ProcessDefinition
import random

class RedisPublisher(StandaloneProcess):

    def __init__(self):
        pass
    def on_start(self):

        process_dispatcher = ProcessDispatcherServiceClient(node=self.container.node)
        rr_cli = ResourceRegistryServiceClient(node=self.container.node)


        producer_definition = ProcessDefinition()
        producer_definition.executable = {
            'module':'ion.processes.redis_coordination_example',
            'class':'RedisCoordinationPublisher'
        }

        producer_procdef_id = process_dispatcher.create_process_definition(process_definition=producer_definition)


        #------------------------------------------------------------------------
        # Find streams in the system and choose one at random to publish on
        #------------------------------------------------------------------------

        streams =  rr_cli.find_resources(RT.Stream)

        log.warn("redis_publisher found streams")

        stream_ids = []

        for stream_dict in streams[1]:

            log.warn("type of streams: %s" % type(stream_dict))

            log.warn("streams.id: %s" % stream_dict['id'])

            stream_ids.append(stream_dict['id'])

        # choose a random stream:

        log.warn("num of stream_ids: %d" % len(stream_ids))

        stream_id = stream_ids[random.randint(0,len(stream_ids)-1)]
        log.warn("chose the random stream, %s, to publish on!" % stream_id)

        #------------------------------------------------------------------------
        # publish!
        #------------------------------------------------------------------------

        configuration = {
            'process':{
                'stream_id':stream_id,
                }
        }

        ctd_pid = process_dispatcher.schedule_process(process_definition_id=producer_procdef_id, configuration=configuration)

        log.warn("post schedule process. ctd_pid: %s" % ctd_pid)