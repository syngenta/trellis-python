from syngenta_digital_dta.dynamodb import DynamodbAdapter
from syngenta_digital_dta.postgres import PostgresAdapter

def adapter(**kwargs):
    if kwargs['engine'] == 'dynamodb':
        return DynamodbAdapter(**kwargs)
    if kwargs['engine'] == 'redshift':
        return PostgresAdapter(**kwargs)
    if kwargs['engine'] == 'postgres':
        return PostgresAdapter(**kwargs)
    raise Exception('engine {} not supported; contribute to get it supported :)'.format(kwargs['engine']))
