#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file 
@date 04/17/12 09:07
@description DESCRIPTION
'''
from mock import Mock, patch
import elasticpy
from nose.plugins.attrib import attr
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iindex_management_service import IndexManagementServiceClient
from ion.services.dm.inventory.index_management_service import IndexManagementService



@attr('UNIT',group='dm')
class IndexManagementUnitTest(PyonTestCase):
    def setUp(self):
        mock_clients = self._create_service_mock('index_management')
        self.index_management = IndexManagementService()
        self.index_management.clients = mock_clients
        
        self.rr_create = mock_clients.resource_registry.create
        self.rr_read   = mock_clients.resource_registry.read
        self.rr_update = mock_clients.resource_registry.update
        self.rr_delete = mock_clients.resource_registry.delete
        self.rr_find_resources = mock_clients.resource_registry.find_resources


    def test_create_index(self):
        '''
        test_create_index
        Unit test for basic creation of an index
        '''
        
        # Mocks
        self.rr_find_resources.return_value = ([],[])
        self.rr_create.return_value = 'mock_index_id', 'Garbage'

        # Patch elasticpy for the Unit Tests
        with patch('elasticpy.ElasticSearch') as MockSearch:

            instance = MockSearch.return_value
            mock_river_create = Mock()
            mock_index_create = Mock()
            instance.river_couchdb_create = mock_river_create
            instance.index_create = mock_index_create

            # Execution
            retval = self.index_management.create_index('test_index', IndexManagementService.SIMPLE_INDEX)

            # Assertions
            self.assertFalse(mock_river_create.called, 'A river should not have been created.')
            self.assertTrue(mock_index_create.called, 'An index should have been created.')
            self.assertTrue(retval=='mock_index_id', 'The return value is incorrect. [%s]' % retval)

            self.assertTrue(self.rr_create.called, 'The resource should be created.')


    def test_read_index(self):
        pass

    def test_delete_index(self):
        pass

    def test_list_indexes(self):
        pass

    def test_find_indexes(self):
        pass

    def test_query(self):
        pass




@attr('INT',group='dm')
class IndexManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(IndexManagementIntTest,self).setUp()
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        ims_cli = IndexManagementServiceClient()
        rr_cli = ResourceRegistryServiceClient()

    def test_create_index(self):
        pass

    def test_read_index(self):
        pass

    def test_delete_index(self):
        pass

    def test_list_indexes(self):
        pass

    def test_find_indexes(self):
        pass

    def test_query(self):
        pass

