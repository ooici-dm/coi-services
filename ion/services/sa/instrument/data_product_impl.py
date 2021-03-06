#!/usr/bin/env python

"""
@package  ion.services.sa.instrument.data_product_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT


from ion.services.sa.resource_impl.resource_impl import ResourceImpl

class DataProductImpl(ResourceImpl):
    """
    @brief Resource management for DataProduct resources
    """

    def _primary_object_name(self):
        return RT.DataProduct

    def _primary_object_label(self):
        return "data_product"

    def link_data_producer(self, data_product_id='', data_producer_id=''):
        return self._link_resources(data_product_id, PRED.hasDataProducer, data_producer_id)

    def unlink_data_producer(self, data_product_id='', data_producer_id=''):
        return self._unlink_resources(data_product_id, PRED.hasDataProducer, data_producer_id)

    def link_data_set(self, data_product_id='', data_set_id=''):
        return self._link_resources(data_product_id, PRED.hasDataset, data_set_id)

    def unlink_data_set(self, data_product_id='', data_set_id=''):
        return self._unlink_resources(data_product_id, PRED.hasDataset, data_set_id)

    def link_stream(self, data_product_id='', stream_id=''):
        return self._link_resources(data_product_id, PRED.hasStream, stream_id)

    def unlink_stream(self, data_product_id='', stream_id=''):
        return self._unlink_resources(data_product_id, PRED.hasStream, stream_id)

    def link_parent(self, data_product_id='', parent_data_product_id=''):
        return self._link_resources(data_product_id, PRED.hasParent, parent_data_product_id)

    def unlink_parent(self, data_product_id='', parent_data_product_id=''):
        return self._unlink_resources(data_product_id, PRED.hasParent, parent_data_product_id)

    def find_having_data_producer(self, data_producer_id):
        return self._find_having(PRED.hasDataProducer, data_producer_id)

    def find_stemming_data_producer(self, data_product_id):
        return self._find_stemming(data_product_id, PRED.hasDataProducer, RT.DataProducer)

    def find_having_data_set(self, data_set_id):
        return self._find_having(PRED.hasDataset, data_set_id)

    def find_stemming_data_set(self, data_product_id):
        return self._find_stemming(data_product_id, PRED.hasDataset, RT.DataSet)

    def find_having_stream(self, stream_id):
        return self._find_having(PRED.hasStream, stream_id)

    def find_stemming_stream(self, data_product_id):
        return self._find_stemming(data_product_id, PRED.hasStream, RT.DataSet)

    def find_having_parent(self, parent_data_product_id):
        return self._find_having(PRED.hasParent, parent_data_product_id)

    def find_stemming_parent(self, data_product_id):
        return self._find_stemming(data_product_id, PRED.hasParent, RT.DataProduct)

