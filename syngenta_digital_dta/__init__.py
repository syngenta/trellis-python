from syngenta_digital_dta.dynamodb.adapter import DynamodbAdapter
from syngenta_digital_dta.postgres.adapter import PostgresAdapter
from syngenta_digital_dta.elasticsearch.adapter import ElasticsearchAdapter


def adapter(**kwargs):
    if kwargs['engine'] == 'dynamodb':
        return DynamodbAdapter(**kwargs)
    if kwargs['engine'] == 'redshift':
        return PostgresAdapter(**kwargs)
    if kwargs['engine'] == 'postgres':
        return PostgresAdapter(**kwargs)
    if kwargs['engine'] == 'elasticsearch':
        return ElasticsearchAdapter(**kwargs)
    raise Exception('engine {} not supported; contribute to get it supported :)'.format(kwargs['engine']))
