#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file 
@date 04/18/12 11:56
@description DESCRIPTION
'''
from interface.objects import Catalog, Index
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.icatalog_management_service import CatalogManagementServiceClient
from ion.services.dm.presentation.catalog_management_service import CatalogManagementService
from pyon.ion.resource import PRED
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr


@attr('UNIT', group='dm')
class CatalogManagementUnitTest(PyonTestCase):
    def setUp(self):
        super(CatalogManagementUnitTest, self).setUp()

        mock_clients = self._create_service_mock('catalog_management')
        self.catalog_management = CatalogManagementService()
        self.catalog_management.clients = mock_clients

        self.rr_create = mock_clients.resource_registry.create
        self.rr_read = mock_clients.resource_registry.read
        self.rr_update = mock_clients.resource_registry.update
        self.rr_delete = mock_clients.resource_registry.delete
        self.rr_find_assoc = mock_clients.resource_registry.find_associations
        self.rr_find_res = mock_clients.resource_registry.find_resources

    def test_create_catalog(self):
        # Mocks
        self.rr_find_res.return_value = ([],[])
        self.rr_create.return_value = 'resource_id','revision'

        # Execution
        catalog_id = self.catalog_management.create_catalog('mock_catalog')

        # Assertions
        self.assertTrue(catalog_id == 'resource_id', "Improper creation of resource.")
        self.assertTrue(self.rr_create.called)

    def test_update_catalog(self):
        # Mocks
        catalog_res = Catalog()

        # Execution
        self.catalog_management.update_catalog(catalog_res)

        # Assertions
        self.assertTrue(self.rr_update.called, "Improper update of resource")

    def test_read_catalog(self):
        # Mocks
        catalog_res = dict(check=True)
        self.rr_read.return_value = catalog_res

        # Execution
        retval = self.catalog_management.read_catalog('catalog_id')

        # Assertions
        self.rr_read.assert_called_once_with('catalog_id', '')
        self.assertTrue(retval['check'])

    def test_delete_catalog(self):
        # Mocks

        # Execution
        self.catalog_management.delete_catalog('catalog_id')

        # Assertions
        self.rr_delete.assert_called_once_with('catalog_id')
        

    def test_add_indexes(self):

        # Ending result should be
        # available_fields: [1,2,3,4,5,6,7]
        # catalog_fields: [3,4,5]

        catalog_res = Catalog()
        catalog_res.available_fields = [1]
        catalog_res.catalog_fields = [2,3,4,5]

        index_res_1 = Index()
        index_res_1.options.attribute_match = [3,4]
        index_res_1.options.geo_fields = [2,5]

        index_res_2 = Index()
        index_res_2.options.attribute_match = [3,4]
        index_res_2.options.wildcard = [6,7]
        index_res_2.options.geo_fields = [5]

        reads = [catalog_res, index_res_1, index_res_2]
        def mock_read(*args, **kwargs):
            return reads.pop(0)

        def mock_update(res):
            available_fields = res.available_fields
            catalog_fields   = res.catalog_fields
            self.assertTrue(available_fields == [1,2,3,4,5,6,7], "Mismatch: %s" % available_fields)
            self.assertTrue(catalog_fields == [3,4,5] , "Mismatch: %s" % catalog_fields)
        self.rr_find_assoc.return_value = [index_res_1, index_res_2], [0,1]
        self.rr_read.side_effect = mock_read
        self.rr_update.side_effect = mock_update

        # Execution
        self.catalog_management.add_indexes('catalog_id', [1,2])

        # Assertions



    def test_list_indexes(self):
        # Mocks
        self.rr_find_assoc.return_value = ([1,2,3],[1,2,3])

        # Execution
        retval = self.catalog_management.list_indexes('catalog_id')

        # Assertions
        self.rr_find_assoc.assert_called_once_with('catalog_id',PRED.hasIndex,'',None,True)
        self.assertTrue(retval == [1,2,3], "Return mismatch.")

@attr('INT', group='dm')
class CatalogManagementIntTest(IonIntegrationTestCase):
    def setUp(self):
        super(CatalogManagementIntTest,self).setUp()

        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2dm.yml')

        self.cms_cli = CatalogManagementServiceClient()
        self.rr_cli  = ResourceRegistryServiceClient()

    def test_create_catalog(self):
        pass

    def test_update_catalog(self):
        pass

    def test_read_catalog(self):
        pass

    def test_delete_catalog(self):
        pass

    def test_add_indexes(self):
        pass

    def test_list_indexes(self):
        pass
