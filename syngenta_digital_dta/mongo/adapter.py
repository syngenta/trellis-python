from functools import lru_cache

from pymongo import MongoClient

from syngenta_digital_dta.common.base_adapter import BaseAdapter
# from syngenta_digital_dta.common import dict_merger
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

    def read(self, **kwargs):
        if kwargs.get('operation') == 'query':
            return self.find(**kwargs)
        return self.find_one(**kwargs)

    def find_one(self, **kwargs):
        return self.connection.find_one(kwargs['query'])

    def find(self, **kwargs):
        results = self.connection.find(kwargs['query'])
        return list(results)

    def update(self, **kwargs):
        print(kwargs)

    def delete(self, **kwargs):
        data = self.find_one(**kwargs)
        result = self.connection.delete_one(kwargs['query'])
        super().publish('delete', data, **kwargs)
        return result
