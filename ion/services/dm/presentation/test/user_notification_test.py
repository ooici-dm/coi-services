'''
@author Bill Bollenbacher
@file ion/services/dm/presentation/test/user_notification_test.py
@description Unit and Integration test implementations for the user notification service class.
'''
from interface.services.coi.iidentity_management_service import IdentityManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iuser_notification_service import UserNotificationServiceClient
from ion.services.dm.presentation.user_notification_service import UserNotificationService
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from pyon.public import IonObject, RT, PRED
from nose.plugins.attrib import attr
import unittest


@attr('UNIT',group='DM')
class UserNotificationTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('user_notification')
        self.user_notification = UserNotificationService()
        self.user_notification.clients = mock_clients

        self.mock_rr_client = self.user_notification.clients.resource_registry
        self.notification_object = IonObject(RT.NotificationRequest, name="notification")

    def test_create_one_user_notification(self):
        # mocks
        self.mock_rr_client.create.return_value = ('notification_id','rev')
        self.mock_rr_client.read.return_value = ('user_1_info')

        # execution
        notification_id = self.user_notification.create_notification(self.notification_object, 'user_1')


        # assertions
        self.assertEquals(notification_id,'notification_id')
        self.assertTrue(self.mock_rr_client.create.called)

    def test_update_user_notification(self):
        # mocks

        # execution

        # assertions
        pass
    
    def test_delete_user_notification(self):
        # mocks

        # execution

        # assertions
        pass


@attr('INT', group='DM')
#@unittest.skip('not working')
class UserNotificationIntTest(IonIntegrationTestCase):
    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.unsc = UserNotificationServiceClient(node=self.container.node)
        self.rrc = ResourceRegistryServiceClient(node=self.container.node)
        self.imc = IdentityManagementServiceClient(node=self.container.node)
        
    def test_create_one_user_notification(self):
        user_object = IonObject(RT.UserIdentity, name="user1")
        user_id, _ = self.rrc.create(user_object)
        notification_object = IonObject(RT.NotificationRequest, name="notification")
        self.unsc.create_notification(notification_object, user_id)

    def test_create_two_user_notifications(self):
        user_identty_object = IonObject(RT.UserIdentity, name="user1")
        user_id = self.imc.create_user_identity(user_identty_object)
        user_info_object = IonObject(RT.UserInfo, {"name":"user1_info", "contact":{"email":'user1_email@someplace.com'}})
        self.imc.create_user_info(user_id, user_info_object)
        notification_object = IonObject(RT.NotificationRequest, name="notification1")
        self.unsc.create_notification(notification_object, user_id)
        notification_object = IonObject(RT.NotificationRequest, name="notification2")
        self.unsc.create_notification(notification_object, user_id)

