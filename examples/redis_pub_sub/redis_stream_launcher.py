from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from pyon.ion.process import StandaloneProcess
from pyon.public import log
from interface.objects import ExchangeQuery

class RedisStreamLauncher(StandaloneProcess):

    def __init__(self):

        self.stream_ids = []

    def on_start(self):
        pubsub_cli = PubsubManagementServiceClient(node=self.container.node)

        #----------------------------------------
        # Create streams
        #----------------------------------------

        for i in xrange(0,10):

            stream_id = pubsub_cli.create_stream(name="redis_stream_%s" % i,
                description="A stream on which redis coordination publisher can publish its packets")

            self.stream_ids.append(stream_id)

        log.warn("RedisStreamLauncher created the stream_ids:")
        log.warn(self.stream_ids)

        #----------------------------------------
        # Create exchange query subscription
        #----------------------------------------

        #----------------------------------------------------------------------------------
        # Create a subscriber. Get the datum from a the published stream
        #----------------------------------------------------------------------------------

        # listen to a stream_id....

        exchange_name = "redis_coordination_queue"
        query = ExchangeQuery()

        subscription_id = pubsub_cli.create_subscription(query = query,
            exchange_name = exchange_name,
            name = "RedisSubscription",
            description = "Redis Subscription Description")

        log.warn("Exchange subscription created: %s" % subscription_id)

