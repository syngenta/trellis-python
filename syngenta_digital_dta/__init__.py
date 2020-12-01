from syngenta_digital_dta.dynamodb import DynamodbAdapter

def adapter(**kwargs):
    supported_engines = ['dynamodb']
    if kwargs['engine'] not in supported_engines:
        raise Exception('engine {} not supported; contribute to get it supported :)'.format(kwargs['engine']))
    if kwargs['engine'] == 'dynamodb':
        return DynamodbAdapter(**kwargs)
