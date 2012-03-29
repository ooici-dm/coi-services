from interface.services.cei.iprocess_dispatcher_service import ProcessDispatcherServiceClient
from interface.services.dm.itransform_management_service import TransformManagementServiceClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.ion.process import StandaloneProcess
from pyon.public import log, RT
from pyon.core.exception import BadRequest
from interface.objects import ProcessDefinition
import time, hashlib

class RedisTransform(StandaloneProcess):

    def __init__(self):
        pass
    def on_start(self):

        log.warn("came here!")

#        process_dispatcher = ProcessDispatcherServiceClient(node=self.container.node)
#        transform_management_service = TransformManagementServiceClient(node=self.container.node)

        rr_cli = ResourceRegistryServiceClient(node=self.container.node)
        pubsub_cli = PubsubManagementServiceClient(node = self.container.node)

        #----------------------------------------------------------------------------------
        # Find the exchange subscription in the system and activate it
        #----------------------------------------------------------------------------------
#        subscription, _ =  rr_cli.find_resources(RT.Subscription, id_only=False)

        subscriptions, subscriptions_info =  rr_cli.find_resources(RT.Subscription)

        subscription_id =  subscriptions_info[0]['id']
        subscription = subscriptions[0]

        pubsub_cli.activate_subscription(subscription_id)


#        #----------------------------------------------------------------------------------
#        # Create a process definition for the transforms
#        #----------------------------------------------------------------------------------
#
#        transform_definition = ProcessDefinition()
#        transform_definition.executable = {
#            'module':'ion.processes.redis_coordination_example',
#            'class':'RedisCoordinationTransform'
#        }
#
#        redis_transform_procdef_id = process_dispatcher.create_process_definition(process_definition=transform_definition)
#
#        #----------------------------------------------------------------------------------
#        # Create the transforms
#        #----------------------------------------------------------------------------------
#
#        name = 'redis_transform' + hashlib.sha1(str(time.time())).hexdigest().upper()[:8]
#
#        transform_id = transform_management_service.create_transform(
#            name=name,
#            in_subscription_id=subscription_id,
#            out_streams=None,
#            process_definition_id = redis_transform_procdef_id,
#            # no configuration needed at this time...
#        )
#
#        # start the transforms - for a test case it makes sense to do it before starting the producer but it is not required
#
#        try:
#            transform_management_service.activate_transform(transform_id=transform_id)
#        except BadRequest:
#            log.debug("Subscription is already active. A previous transform may have already activated this subscription.")


        configuration={}
        configuration['process'] = {
            'name':'redis_transform',
            'type':'stream_process',
            'listen_name':subscription.exchange_name,
            'transform_id':'1234'
        }


        pid = self.container.spawn_process('redis_transform', 'ion.processes.redis_coordination_example', 'RedisCoordinationTransform', configuration)









