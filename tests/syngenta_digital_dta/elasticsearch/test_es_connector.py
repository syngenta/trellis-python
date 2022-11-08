import unittest
import warnings
from collections import namedtuple
from unittest import mock

from syngenta_digital_dta.elasticsearch.es_connector import ESConnector
from tests.syngenta_digital_dta.elasticsearch.mocks import MockESAdapter


class ESConnectorTest(unittest.TestCase):

    def setUp(self, *args, **kwargs):
        warnings.simplefilter('ignore', ResourceWarning)
        self.maxDiff = None

    def test_class_port(self):
        mock_adapter = MockESAdapter()
        connector = ESConnector(mock_adapter)
        self.assertEqual(connector.port, mock_adapter.port)

    def test_class_port_nonlocalhost(self):
        mock_adapter = MockESAdapter(endpoint='dev.aws.com')
        connector = ESConnector(mock_adapter)
        self.assertEqual(connector.port, mock_adapter.port)

    def test_class_port_user_pass(self):
        mock_adapter = MockESAdapter(endpoint='dev.aws.com', user='root', password='root',
                                     authentication='user-password')
        connector = ESConnector(mock_adapter)
        self.assertEqual(connector.user, mock_adapter.user)
        self.assertEqual(connector.password, mock_adapter.password)

    @mock.patch('syngenta_digital_dta.elasticsearch.es_connector.Elasticsearch')
    def test_elasticsearch_constructor(self, mock_elasticsearch):
        cls = namedtuple('cls', ['endpoint', 'port', 'authentication', 'user', 'password'])

        kwargs = cls(
            endpoint='endpoint',
            port=1234,
            authentication='user-password',
            user='user',
            password='password'
        )

        esc = ESConnector(kwargs)
        esc.connect()

        mock_elasticsearch.assert_called_with(
            hosts=[
                {
                    'host': 'endpoint',
                    'port': 1234
                }
            ],
            use_ssl=True,
            verify_certs=True,
            connection_class=mock.ANY,
            timeout=30,
            request_timeout=30,
            http_auth=('user', 'password')
        )
