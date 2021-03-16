import boto3
import simplejson as json

from syngenta_digital_dta.common import logger


def _check_max_custom_attributes(default_attributes, custom_attributes):
    default_keys = default_attributes.keys()
    custom_keys = custom_attributes.keys()
    if len(default_keys) + len(custom_keys) > 10:
        raise Exception('too many custom attributes provided; by default {} are provided, you provided {}: together they are greater than 10'.format(len(default_keys), len(custom_keys)))


def _format_custom_attributes(default_attributes, custom_attributes):
    _check_max_custom_attributes(default_attributes, custom_attributes)
    for key, value in custom_attributes.items():
        data_type = 'Number'
        if isinstance(value, str):
            data_type = 'String'
        default_attributes[key] = {
            'DataType': data_type,
            'StringValue': value
        }
    return default_attributes


def _get_default_attributes(**kwargs):
    defaults = {
        'model_schema': {
            'DataType': 'String',
            'StringValue': kwargs['model_schema']
        },
        'model_identifier': {
            'DataType': 'String',
            'StringValue': kwargs['model_identifier']
        },
        'operation': {
            'DataType': 'String',
            'StringValue': kwargs['operation']
        }
    }
    if kwargs.get('author_identifier'):
        defaults['author_identifier'] = {
            'DataType': 'String',
            'StringValue': kwargs['author_identifier']
        }
    return defaults


def publish(**kwargs):
    if not kwargs.get('sns_arn') or not kwargs.get('data'):
        return
    try:
        publisher = boto3.client('sns', region_name=kwargs.get('region'))
        attributes = _get_default_attributes(**kwargs)
        message_attributes = _format_custom_attributes(attributes, kwargs.get('sns_attributes', {}))
        publisher.publish(
            TopicArn=kwargs['sns_arn'],
            Message=json.dumps(kwargs['data']),
            MessageAttributes=message_attributes
        )
    except Exception as e:
        logger.log(level='WARN', log={'error':'publish_sns_error: {}'.format(e)})
