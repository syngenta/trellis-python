from syngenta_digital_dta.dynamodb import DynamodbAdapter
from syngenta_digital_dta.redshift import RedShiftAdapter

def adapter(**kwargs):
    supported_engines = ['dynamodb', 'redshift']
    if kwargs['engine'] not in supported_engines:
        raise Exception('engine {} not supported; contribute to get it supported :)'.format(kwargs['engine']))
    if kwargs['engine'] == 'dynamodb':
        return DynamodbAdapter(**kwargs)
    if kwargs['engine'] == 'redshift':
        return RedShiftAdapter(**kwargs)
    return None
