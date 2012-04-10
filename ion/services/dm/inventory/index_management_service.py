#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/inventory/index_management_service.py
@description Micro implementation of Index Management using ElasticSearch to index resources.

'''
from pyon.core.exception import NotFound
from pyon.public import log
from interface.objects import Index
import json
import urllib2
from interface.services.dm.iindex_management_service import BaseIndexManagementService
from pyon.public import CFG

elasticsearch_host = 'localhost'
elasticsearch_port = '9200'

class IndexManagementService(BaseIndexManagementService):

    """
    class docstring
    """

    def create_index(self, index_name='', index_type=''):
        '''
        @param index_name Name of the index/couchdb database
        @param index_type Type of the index

        Creates an elastic search index and river to couchdb based on the index_name

        > ims_cli = IndexManagementServiceClient()
        > from pyon.core.bootstrap import get_sys_name
        > sys_name = get_sys_name()
        > index_name = '%s_resources' % sys_name
        > index_id = ims_cli.create_index(index_name,index_name)
        '''
        import elasticpy as ep
        index_res = Index(index_name=index_name, index_type=index_type)
        index_id, _ = self.clients.resource_registry.create(index_res)

        couchdb_host = CFG.get_safe('server.couchdb.host','localhost')
        couchdb_port = CFG.get_safe('server.couchdb.port','5984')


        search = ep.ElasticSearch()
        search.river_couchdb_create(
            index_name=index_name,
            index_type=index_type,
            couchdb_db=index_name,
            couchdb_host=couchdb_host,
            couchdb_port=couchdb_port
        )

        return index_id


    def update_index(self, index=None):
        raise NotImplemented("update_index is not yet implemented")

    def read_index(self, index_id=''):
        index_resources = self.clients.resource_registry.read(index_id)
        return index_resources

    def delete_index(self, index_id=''):
        import elasticpy as ep

        index = self.clients.resource_registry.read(index_id)
        search = ep.ElasticSearch()
        search.river_couchdb_delete(index.index_name)
        search.index_delete(index.index_name)

        self.clients.resource_registry.delete(index_id)
        return True


    def list_indexes(self):
        indices, _ = self.clients.resource_registry.find_resources(restype=Index, id_only=True)
        return indices

    def find_indexes(self, filters=None):
        pass

    def query(self, index_id='', query_string=''):
        '''
        Docs
        '''
        import elasticpy
        index = self.clients.resource_registry.read(index_id)

        engine = elasticpy.ElasticSearch()
        query = elasticpy.ElasticQuery().query_string(query=query_string)

        results = engine.search_advanced(index.index_name, index.index_type, query)

        try:
            if results['hits']['total'] > 0:
                res_list = list([hit['_id'] for hit in results['hits']['hits']])
            else:
                raise NotFound("The query had no results.")

        except AttributeError:
            raise NotFound("The query had no results.")

        return res_list
