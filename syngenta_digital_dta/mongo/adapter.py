from functools import lru_cache

from pymongo import MongoClient, operations

from syngenta_digital_dta.common.base_adapter import BaseAdapter
from syngenta_digital_dta.common import dict_merger
from syngenta_digital_dta.common import schema_mapper


class MongoAdapter(BaseAdapter):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__collection = self.__connect(**kwargs)
        self.__model_schema_file = kwargs['model_schema_file']
        self.__model_schema = kwargs['model_schema']
        self.__model_identifier = kwargs['model_identifier']
        self.__allowed_queries = [
            'find',
            'find_one',
            'aggregate',
            'aggregate_raw_batches',
            'find_raw_batches',
            'count_documents',
            'estimated_document_count',
            'distinct',
            'list_indexes'
        ]

    @lru_cache(maxsize=128)
    def __connect(self, **kwargs):
        client = MongoClient(kwargs['endpoint'], username=kwargs['user'], password=kwargs['password'])
        db = client[kwargs['database']]
        return db[kwargs['collection']]

    def create(self, **kwargs):
        data = schema_mapper.map_to_schema(kwargs['data'], self.__model_schema_file, self.__model_schema)
        data['_id'] = data[self.__model_identifier]
        self.__collection.insert_one(data)
        super().publish('create', data, **kwargs)
        return data

    def __map_documents(self, **kwargs):
        items = []
        for item in kwargs['data']:
            item = schema_mapper.map_to_schema(item, self.__model_schema_file, self.__model_schema)
            item['_id'] = item[self.__model_identifier]
            items.append(item)
        return items

    def batch_create(self, **kwargs):
        items = self.__map_documents(**kwargs)
        insert_result = self.__collection.insert_many(items, **kwargs.get('params', {}))
        super().publish('batch_create', items, **kwargs)
        return insert_result

    def batch_upsert(self, **kwargs):
        data = kwargs['data']
        batch_size = kwargs.get('batch_size', 25)

        if not isinstance(data, list):
            raise Exception('Batched data must be contained within a list')

        batched_data = (data[pos:pos + batch_size] for pos in range(0, len(data), batch_size))
        results = []
        for items in batched_data:
            bulk_operations = [
                operations.ReplaceOne(filter={'_id': item[self.__model_identifier]}, replacement=item, upsert=True) for item in items
            ]
            batch_results = self.__collection.bulk_write(bulk_operations, **kwargs.get('params', {}))
            results.append(batch_results)
            super().publish('batch_upsert', items, **kwargs)

        return results

    def read(self, **kwargs):
        if kwargs.get('operation') == 'query':
            return self.find(**kwargs)
        return self.find_one(**kwargs)

    def query(self, **kwargs):
        if kwargs['operation'] not in self.__allowed_queries:
            raise Exception(
                'query method is for read-only operations; please use another function for destructive operations')
        query = getattr(self.__collection, kwargs['operation'])
        return query(kwargs['query'])

    def find_one(self, **kwargs):
        return self.__collection.find_one(kwargs['query'])

    def find(self, **kwargs):
        results = self.__collection.find(kwargs['query'], **kwargs.get('params', {}))
        return list(results)

    def count(self, **kwargs):
        return self.__collection.count_documents(kwargs.get('query', {}), **kwargs.get('params', {}))

    def update(self, **kwargs):
        original_data = self.find_one(**kwargs)
        if not original_data:
            raise Exception(f'no document found by query: {kwargs["query"]}')
        merged_data = dict_merger.merge(original_data, kwargs['data'], **kwargs)
        updated_data = schema_mapper.map_to_schema(merged_data, self.__model_schema_file, self.__model_schema)
        self.__collection.replace_one(kwargs['query'], updated_data, upsert=False)
        super().publish('update', updated_data, **kwargs)
        return updated_data

    def upsert(self, **kwargs):
        original_data = self.find_one(**kwargs)
        if original_data:
            merged_data = dict_merger.merge(original_data, kwargs['data'], **kwargs)
        else:
            merged_data = kwargs['data']
        data = schema_mapper.map_to_schema(merged_data, self.__model_schema_file, self.__model_schema)
        data['_id'] = data[self.__model_identifier]
        self.__collection.replace_one(kwargs['query'], data, upsert=True)
        super().publish('upsert', data, **kwargs)
        return data

    def delete(self, **kwargs):
        data = self.find_one(**kwargs)
        result = self.__collection.delete_one(kwargs['query'])
        super().publish('delete', data, **kwargs)
        return result

    def batch_delete(self, **kwargs):
        items = self.__map_documents(**kwargs)
        bulk_operations = []
        for item in items:
            bulk_operations.append(operations.DeleteOne(filter={'_id': item['_id']}))

        results = self.__collection.bulk_write(bulk_operations, **kwargs.get('params', {}))
        super().publish('batch_delete', items, **kwargs)
        return results
