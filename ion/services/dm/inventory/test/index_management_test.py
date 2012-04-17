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
from interface.objects import Index
from pyon.core.exception import NotFound
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.iindex_management_service import IndexManagementServiceClient
from ion.services.dm.inventory.index_management_service import IndexManagementService

from ion.services.dm.inventory.index_management_service import elasticsearch_host, elasticsearch_port

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
        # mocks
        return_obj = dict(mock='mock')
        self.rr_read.return_value = return_obj

        # execution
        retval = self.index_management.read_index('mock_index_id')

        # assertions
        self.assertEquals(return_obj, retval, 'The resource should be returned.')



    def test_delete_index(self):
        # Mocks
        mock_index = Index(index_name='mocked',index_type=IndexManagementService.SIMPLE_INDEX)
        self.rr_read.return_value = mock_index

        with patch('elasticpy.ElasticSearch') as MockSearch:
            instance = MockSearch.return_value
            mock_river_delete = Mock()
            mock_index_delete = Mock()
            instance.river_couchdb_delete = mock_river_delete
            instance.index_delete = mock_index_delete

            # Execution
            self.index_management.delete_index('mock_index_id')

            # Assertions
            self.rr_delete.assert_called_once_with('mock_index_id')
            mock_index_delete.assert_called_once_with('mocked')

    def test_list_indexes(self):
        pass

    def test_find_indexes(self):
        pass

    def test_query(self):
        pass




@attr('INT',group='dm')
class IndexManagementIntTest(IonIntegrationTestCase):

    def setUp(self):
        from urllib2 import HTTPError

        super(IndexManagementIntTest,self).setUp()
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.ims_cli = IndexManagementServiceClient()
        self.rr_cli = ResourceRegistryServiceClient()
        try:
            elasticpy.ElasticSearch().index_delete('test_index')
        except HTTPError:
            pass #Means the index isn't there

    def test_create_simple_index(self):
        import urllib2
        import json

        index_id = self.ims_cli.create_index('test_index', IndexManagementService.SIMPLE_INDEX)

        index_res = self.rr_cli.read(index_id)
        self.assertTrue(index_res.index_name == 'test_index', "Improperly formed resource.")
        self.assertTrue(index_res.index_type == 'simple_index', "Improperly formed resource. [%s]" % index_res.index_type)

        indices = elasticpy.ElasticSearch().index_list()
        self.assertTrue("test_index" in indices, "Index failed to be created in ElasticSearch.")

        # Cleanup
        elasticpy.ElasticSearch().index_delete('test_index')

    def test_read_index(self):
        index_res = Index(name='test_index',index_name='test_index',index_type=IndexManagementService.SIMPLE_INDEX)
        index_id, _ = self.rr_cli.create(index_res)

        retval = self.ims_cli.read_index(index_id)
        self.assertEquals( (retval.name, retval.index_name, retval.index_type), 
                (index_res.name, index_res.index_name, index_res.index_type), "Improperly retrieved resource.")



    def test_delete_index(self):
        index_res = Index(name='test_index',index_name='test_index',index_type='simple_index')
        index_id, _ = self.rr_cli.create(index_res)

        elasticpy.ElasticSearch().index_create('test_index')


        self.ims_cli.delete_index(index_id)

        with self.assertRaises(NotFound):
            self.rr_cli.read(index_id)

        indices = elasticpy.ElasticSearch().index_list()

        self.assertFalse("test_index" in indices, "Index not deleted from ElasticSearch")



    def test_list_indexes(self):
        pass

    def test_find_indexes(self):
        pass

    def test_query(self):
        pass

