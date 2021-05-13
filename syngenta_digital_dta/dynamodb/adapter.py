from functools import lru_cache

import boto3
from boto3.dynamodb.conditions import Attr

from syngenta_digital_dta.common import schema_mapper
from syngenta_digital_dta.common import dict_merger
from syngenta_digital_dta.common import publisher


class DynamodbAdapter:

    def __init__(self, **kwargs):
        self.table = self._get_dynamo_table(kwargs['table'], kwargs.get('endpoint'))
        self.model_schema_file = kwargs['model_schema_file']
        self.model_schema = kwargs['model_schema']
        self.model_identifier = kwargs['model_identifier']
        self.model_version_key = kwargs['model_version_key']
        self.author_identifier = kwargs.get('author_identifier')
        self.sns_attributes = kwargs.get('sns_attributes')
        self.publisher = publisher
        self.sns_arn = kwargs.get('sns_arn')

    @lru_cache(maxsize=128)
    def _get_dynamo_table(self, table, endpoint=None):
        return boto3.resource('dynamodb', endpoint_url=endpoint).Table(table)

    def create(self, **kwargs):
        if kwargs.get('operation') == 'overwrite':
            return self.overwrite(**kwargs)
        return self.insert(**kwargs)

    def read(self, **kwargs):
        if kwargs.get('operation') == 'query':
            return self.query(**kwargs)
        if kwargs.get('operation') == 'scan':
            return self.scan(**kwargs)
        return self.get(**kwargs)

    def scan(self, **kwargs):
        return self.table.scan(**kwargs.get('query', {})).get('Items', [])

    def get(self, **kwargs):
        return self.table.get_item(**kwargs.get('query', {})).get('Item', {})

    def query(self, **kwargs):
        return self.table.query(**kwargs.get('query', {})).get('Items', [])

    def overwrite(self, **kwargs):
        overwrite_item = schema_mapper.map_to_schema(kwargs['data'], self.model_schema_file, self.model_schema)
        self.table.put_item(Item=overwrite_item)
        self._publish('create', overwrite_item)
        return overwrite_item

    def insert(self, **kwargs):
        new_item = schema_mapper.map_to_schema(kwargs['data'], self.model_schema_file, self.model_schema)
        self.table.put_item(Item=new_item, ConditionExpression=Attr(self.model_identifier).not_exists())
        self._publish('create', new_item)
        return new_item

    def delete(self, **kwargs):
        kwargs['query']['ReturnValues'] = 'ALL_OLD'
        result = self.table.delete_item(**kwargs['query']).get('Attributes', {})
        self._publish('delete', result)
        return result

    def update(self, **kwargs):
        original_data = self._get_original_data(**kwargs)
        merged_data = dict_merger.merge(original_data, kwargs['data'], **kwargs)
        updated_data = schema_mapper.map_to_schema(merged_data, self.model_schema_file, self.model_schema)
        self.table.put_item(Item=updated_data, ConditionExpression=Attr(self.model_version_key).eq(original_data[self.model_version_key]))
        self._publish('update', updated_data)
        return updated_data

    def _get_original_data(self, **kwargs):
        if kwargs['operation'] == 'get':
            original_data = self.get(**kwargs)
        else:
            original_data = self.query(**kwargs)[0]
        if not original_data:
            raise Exception('update: no data found to update')
        return original_data

    def _publish(self, operation, data):
        self.publisher.publish(
            sns_arn=self.sns_arn,
            model_identifier=self.model_identifier,
            model_schema=self.model_schema,
            author_identifier=self.author_identifier,
            data=data,
            operation=operation
        )
