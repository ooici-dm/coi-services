#!/usr/bin/env python
'''
@author Luke Campbell <LCampbell@ASAScience.com>
@file ion/services/dm/inventory/index_management_service.py
@description Micro implementation of Index Management using ElasticSearch to index resources.

'''
from pyon.ion.resource import RT
from pyon.public import CFG
from pyon.core.bootstrap import get_sys_name
from pyon.core.exception import NotFound, BadRequest
from interface.objects import Index
import json
import urllib2
from interface.services.dm.iindex_management_service import BaseIndexManagementService
import elasticpy as ep
from pyon.util.log import log


elasticsearch_host = 'localhost'
elasticsearch_port = '9200'

class IndexManagementService(BaseIndexManagementService):
    """
    class docstring
    """
    COUCHDB_RIVER_INDEX=1
    SIMPLE_INDEX=2
    ADVANCED_INDEX=3


    def create_index(self, index_name='', index_type='', options=None, datastore_name=''):
        '''
        '''
        if not index_type:
            index_type = IndexManagementService.SIMPLE_INDEX


        # Handle the name uniqueness factor
        res, _ = self.clients.resource_registry.find_resources(name=index_name, id_only=True)
        if len(res)>0:
            raise BadRequest('The index resource with name: %s, already exists.' % index_name)
            
        es = ep.ElasticSearch()
        index_res = Index()
        index_res.index_name = index_name
       
        if index_type == IndexManagementService.COUCHDB_RIVER_INDEX:
            #-------------------------
            # Create the couchdb river
            #-------------------------
            es.river_couchdb_create(
                index_name=index_name,
                index_type=index_name,
                couchdb_db= '_'.join([get_sys_name(), datastore_name or index_name]),
                river_name=index_name,
                couchdb_host=CFG.server.couchdb.host,
                couchdb_port=CFG.server.couchdb.port
            )
            index_res.index_type = 'couchdb_river'

        elif index_type == IndexManagementService.SIMPLE_INDEX:
            #--------------------------------------
            # Create a simple index
            #--------------------------------------
            es.index_create(index_name, index_name)
            index_res.index_type = 'simple_index'


        elif index_type == IndexManagementService.ADVANCED_INDEX:
            #--------------------------------------
            # Create an advanced index with options
            #--------------------------------------

            shards = None
            replicas = None
            if options and options.has_key('itype'):
                itype = options['itype']
            else:
                itype = index_name
            if options and options.has_key('shards'):
                shards = options['shards']
            if options and options.has_key('replicas'):
                replicas = options['replicas']


            es.index_create(
                index_name=index_name,
                index_type=itype,
                shards = shards or 5,
                replicas = replicas or 1
            )
            index_res.index_type = 'advanced_index'
            index_res.options = options

        index_id, _ = self.clients.resource_registry.create(index_res)
        return index_id

    def update_index(self, index=None):
        raise NotImplemented("update_index is not yet implemented")

    def read_index(self, index_id=''):
        index_resources = self.clients.resource_registry.read(index_id)
        return index_resources

    def delete_index(self, index_id=''):

        index_res = self.clients.resource_registry.read(index_id)

        if index_res.index_type == IndexManagementService.COUCHDB_RIVER_INDEX:
            ep.ElasticSearch().river_couchdb_delete(index_res.index_name)

        ep.ElasticSearch().index_delete(index_res.index_name)

        log.debug('Deleted index %s from ElasticSearch' % index_res.index_name)

        self.clients.resource_registry.delete(index_id)
        return True


    def list_indexes(self):
        indices, _ = self.clients.resource_registry.find_resources(restype=RT.Index, id_only=True)
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

    def _map_resource_index(self, index_name, resource_id):
        '''
        Maps resources from CouchDB into an elastic search index
        '''
        pass

    def _resource_bfs(self, resource_id):
        '''
        Breadth First Search of resources based on given resource
        '''
        marked = [resource_id]
        queue = [resource_id]
        while len(queue) > 0:
            t = queue.pop(0)
            marked.append(t)
            res_ids, assocs = self.clients.resource_registry.find_objects(subject=t, id_only=True)
            for res in res_ids:
                if not res in marked:
                    marked.append(res)
                    queue.append(res)
        return marked
