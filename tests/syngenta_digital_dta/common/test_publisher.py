import unittest
import warnings

from syngenta_digital_dta.common import publisher

class PublisherTest(unittest.TestCase):

    def setUp(self, *args, **keywargs):
        warnings.simplefilter("ignore", ResourceWarning)
        self.maxDiff = None
        self.mock_sns_arn = 'arn:aws:sns:us-east-2:111111111111:unittest-mock-sns-topic'

    def test_publish_basic(self):
        publisher.publish(
            sns_arn=self.mock_sns_arn,
            data={'key': 'value'},
            model_schema='unit-test',
            model_identifier='unit_test_id',
            region='us-east-2',
            operation='create'
        )

    def test_get_default_attributes(self):
        results = publisher._get_default_attributes(
            model_schema='unit-test',
            model_identifier='unit_test_id',
            author_identifier='author_id',
            operation='create'
        )
        self.assertDictEqual(results, {
            'model_schema': {
                'DataType': 'String',
                'StringValue': 'unit-test'
            },
            'model_identifier': {
                'DataType': 'String',
                'StringValue': 'unit_test_id'
            },
            'operation': {
                'DataType': 'String',
                'StringValue': 'create'
            },
            'author_identifier': {
                'DataType': 'String',
                'StringValue': 'author_id'
            }
        })

    def test_format_custom_attributes_string(self):
        attributes = {
            'model_schema': {
                'DataType': 'String',
                'StringValue': 'unit-test'
            },
            'model_identifier': {
                'DataType': 'String',
                'StringValue': 'unit_test_id'
            },
            'operation': {
                'DataType': 'String',
                'StringValue': 'create'
            },
            'author_identifier': {
                'DataType': 'String',
                'StringValue': 'author_id'
            }
        }
        custom_attributes = {
            'new_attribute': 'new_value'
        }
        results = publisher._format_custom_attributes(attributes, custom_attributes)
        self.assertDictEqual(results, {
            'model_schema': {
                'DataType': 'String',
                'StringValue': 'unit-test'
            },
            'model_identifier': {
                'DataType': 'String',
                'StringValue': 'unit_test_id'
            },
            'operation': {
                'DataType': 'String',
                'StringValue': 'create'
            },
            'author_identifier': {
                'DataType': 'String',
                'StringValue': 'author_id'
            },
            'new_attribute': {
                'DataType': 'String',
                'StringValue': 'new_value'
            }
        })

    def test_format_custom_attributes_number(self):
        attributes = {
            'model_schema': {
                'DataType': 'String',
                'StringValue': 'unit-test'
            },
            'model_identifier': {
                'DataType': 'String',
                'StringValue': 'unit_test_id'
            },
            'operation': {
                'DataType': 'String',
                'StringValue': 'create'
            },
            'author_identifier': {
                'DataType': 'String',
                'StringValue': 'author_id'
            }
        }
        custom_attributes = {
            'new_attribute': 10
        }
        results = publisher._format_custom_attributes(attributes, custom_attributes)
        self.assertDictEqual(results, {
            'model_schema': {
                'DataType': 'String',
                'StringValue': 'unit-test'
            },
            'model_identifier': {
                'DataType': 'String',
                'StringValue': 'unit_test_id'
            },
            'operation': {
                'DataType': 'String',
                'StringValue': 'create'
            },
            'author_identifier': {
                'DataType': 'String',
                'StringValue': 'author_id'
            },
            'new_attribute': {
                'DataType': 'Number',
                'StringValue': 10
            }
        })

    def test_format_custom_attributes_error(self):
        attributes = {
            'model_schema': {
                'DataType': 'String',
                'StringValue': 'unit-test'
            },
            'model_identifier': {
                'DataType': 'String',
                'StringValue': 'unit_test_id'
            },
            'operation': {
                'DataType': 'String',
                'StringValue': 'create'
            },
            'author_identifier': {
                'DataType': 'String',
                'StringValue': 'author_id'
            }
        }
        custom_attributes = {
            'new_attribute5': 10,
            'new_attribute6': 10,
            'new_attribute7': 10,
            'new_attribute8': 10,
            'new_attribute9': 10,
            'new_attribute10': 10,
            'new_attribute11': 10
        }
        try:
            results = publisher._format_custom_attributes(attributes, custom_attributes)
        except Exception as e:
            self.assertEqual('{}'.format(e), 'too many custom attributes provided; by default 4 are provided, you provided 7: together they are greater than 10')
