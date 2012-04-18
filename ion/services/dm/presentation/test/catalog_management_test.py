#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file 
@date 04/18/12 11:56
@description DESCRIPTION
'''
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.services.dm.icatalog_management_service import CatalogManagementServiceClient
from ion.services.dm.presentation.catalog_management_service import CatalogManagementService
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

@attr('UNIT', group='dm')
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
