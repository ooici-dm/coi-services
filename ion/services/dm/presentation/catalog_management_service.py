#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/presentation/catalog_management_service.py
@description Catalog Management Service
'''

from pyon.public import PRED
from pyon.util.log import log
from pyon.core.exception import BadRequest
from interface.objects import Catalog, Index
from interface.services.dm.icatalog_management_service import BaseCatalogManagementService


class CatalogManagementService(BaseCatalogManagementService):
    """
    Catalog Management Service
    Manages and presents catalog resources
    """


    def create_catalog(self, catalog_name=''):
        """A catalog is a new data set that aggregates and presents datasets in a specific way.
        @param catalog_name    str
        @retval catalog_id    str
        """
        # Ensure unique catalog names 
        res, _ = self.clients.resource_registry.find_resources(name=catalog_name, id_only=True)
        if len(res) > 0:
            raise BadRequest('The catalog resource with name: %s, already exists.' % catalog_name)

        catalog_res = Catalog(name=catalog_name)

        catalog_id = self.clients.resource_registry.create(catalog_res)

        return catalog_id

    def update_catalog(self, catalog=None):
        """@todo document this interface!!!

        @param catalog    Catalog
        @retval success    bool
        """
        pass

    def read_catalog(self, catalog_id=''):
        """Read catalog resources

        @param catalog_id    str
        @retval catalog    Catalog
        """
        pass

    def delete_catalog(self, catalog_id=''):
        """@todo document this interface!!!

        @param catalog_id    str
        @retval success    bool
        """
        pass

    def add_index(self, catalog_id='', index_ids=None):
        """Add an index to the specified catalog

        @param index_ids    list
        @retval success    bool
        """
        for index_id in index_ids:
            index_res = self.clients.resource_registry.read(index_id)
            #==========================================================
            # Parse index_res for fields and aggregate into the catalog
            #----------------------------------------------------------

            self.clients.resource_registry.create_association(subject=catalog_id, predicate=PRED.hasIndex,object=index_id)
            

        return True
            

    def list_indexes(self, catalog_id='', id_only=True):
        """List the indexes for the specified catalog

        @param catalog_id    str
        @retval success    list
        """
        index_ids, assocs = self.clients.resource_registry.find_associations(subject=catalog_id, predicate=PRED.hasIndex, id_only=True)
        return index_ids

