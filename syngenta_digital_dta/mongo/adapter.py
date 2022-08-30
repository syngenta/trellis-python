from functools import lru_cache

from pymongo import MongoClient

from syngenta_digital_dta.common.base_adapter import BaseAdapter
from syngenta_digital_dta.common import dict_merger
from syngenta_digital_dta.common import schema_mapper


class MongoAdapter(BaseAdapter):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.endpoint = kwargs['endpoint']
        self.database = kwargs['database']
        self.collection = kwargs['collection']
        self.user = kwargs['user']
        self.password = kwargs['password']
        self.model_schema_file = kwargs['model_schema_file']
        self.model_schema = kwargs['model_schema']
        self.model_identifier = kwargs['model_identifier']
        self.model_version_key = kwargs['model_version_key']
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
        self.connection = self.__connect()

    @lru_cache(maxsize=128)
    def __connect(self):
        client = MongoClient(self.endpoint, username=self.user, password=self.password)
        db = client[self.database]
        return db[self.collection]

    def create(self, **kwargs):
        data = schema_mapper.map_to_schema(kwargs['data'], self.model_schema_file, self.model_schema)
        data['_id'] = data[self.model_identifier]
        self.connection.insert_one(data)
        super().publish('create', data, **kwargs)
        return data

    def batch_create(self, **kwargs):
        items = []
        for item in kwargs['data']:
            item = schema_mapper.map_to_schema(item, self.model_schema_file, self.model_schema)
            item['_id'] = item[self.model_identifier]
            items.append(item)
        self.connection.insert_many(items)
        super().publish('batch_create', items, **kwargs)
        return items

    def read(self, **kwargs):
        if kwargs.get('operation') == 'query':
            return self.find(**kwargs)
        return self.find_one(**kwargs)

    def query(self, **kwargs):
        if kwargs['operation'] not in self.__allowed_queries:
            raise Exception('query method is for read-only operations; please use another function for destructive operations')
        query = getattr(self.connection, kwargs['operation'])
        return query(kwargs['query'])

    def find_one(self, **kwargs):
        return self.connection.find_one(kwargs['query'])

    def find(self, **kwargs):
        results = self.connection.find(kwargs['query'], **kwargs.get('params', {}))
        return list(results)

    def update(self, **kwargs):
        original_data = self.find_one(**kwargs)
        if not original_data:
            raise Exception(f'no document found by query: {kwargs["query"]}')
        merged_data = dict_merger.merge(original_data, kwargs['data'], **kwargs)
        updated_data = schema_mapper.map_to_schema(merged_data, self.model_schema_file, self.model_schema)
        self.connection.replace_one(kwargs['query'], updated_data, upsert=False)
        super().publish('update', updated_data, **kwargs)
        return updated_data

    def upsert(self, **kwargs):
        original_data = self.find_one(**kwargs)
        if original_data:
            merged_data = dict_merger.merge(original_data, kwargs['data'], **kwargs)
        else:
            merged_data = kwargs['data']
        data = schema_mapper.map_to_schema(merged_data, self.model_schema_file, self.model_schema)
        data['_id'] = data[self.model_identifier]
        self.connection.replace_one(kwargs['query'], data, upsert=True)
        super().publish('upsert', data, **kwargs)
        return data

    def delete(self, **kwargs):
        data = self.find_one(**kwargs)
        result = self.connection.delete_one(kwargs['query'])
        super().publish('delete', data, **kwargs)
        return result
