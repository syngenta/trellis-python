import boto3
import simplejson as json


def _format_custom_attributes(attributes, sns_attributes):
    for key, value in sns_attributes.items():
        data_type = 'Number'
        if isinstance(value, str):
            data_type = 'String'
        attributes[key] = {
            'DataType': data_type,
            'StringValue': value
        }
    return attributes


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
        publisher = boto3.client('sns')
        attributes = _get_default_attributes(**kwargs)
        message_attributes = _format_custom_attributes(attributes, kwargs.get('sns_attributes', {}))
        publisher.publish(
            TopicArn=kwargs['sns_arn'],
            Message=json.dumps(kwargs['data']),
            MessageAttributes=message_attributes
        )
    except Exception as e:
        print('publish_sns_error: {}'.format(e))
