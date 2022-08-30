# pipenv run python -m unittest tests/syngenta_digital_dta/mongo/test_mongo.py
import unittest
import warnings

import syngenta_digital_dta

from tests.syngenta_digital_dta.mongo import mock_data


class MongoAdapterTest(unittest.TestCase):

    def setUp(self, *args, **keywargs):
        warnings.simplefilter('ignore', ResourceWarning)
        self.maxDiff = None
        self.adapter = syngenta_digital_dta.adapter(
            engine='mongo',
            database='unit',
            collection='test',
            user='root',
            password='Lq4nKg&&TRhHv%7z',
            endpoint='mongodb://localhost:27017/',
            model_schema='test-mongo-model',
            model_schema_file='tests/openapi.yml',
            model_identifier='test_id',
            model_version_key='modified'
        )

    def test_init(self):
        self.assertIsInstance(self.adapter, syngenta_digital_dta.mongo.adapter.MongoAdapter)

    def test_create_succeed(self):
        data = mock_data.get_standard()
        result = self.adapter.create(data=data)
        result.pop('_id')
        self.assertDictEqual(result, data)
        self.adapter.delete(query={'test_id': result['test_id']})

    def test_batch_create_succeed(self):
        data = mock_data.get_items()
        result = self.adapter.batch_create(data=data)
        for item in result:
            item.pop('_id')
        self.assertListEqual(result, data)
        for item in result:
            self.adapter.delete(query={'test_id': item['test_id']})

    def test_create_fail_non_unique(self):
        data = mock_data.get_standard()
        data['test_id'] = 'fail-non-unique'
        self.adapter.create(data=data)
        try:
            self.adapter.create(data=data)
            self.assertTrue(False)
        except:
            self.assertTrue(True)
        self.adapter.delete(query={'test_id': data['test_id']})  # clean up

    def test_read(self):
        data = mock_data.get_standard()
        self.adapter.create(data=data)
        result = self.adapter.read(query={'test_id': data['test_id']})
        result.pop('_id')
        self.assertDictEqual(result, data)
        self.adapter.delete(query={'test_id': data['test_id']})  # clean up

    def test_query_allowed(self):
        data = mock_data.get_standard()
        self.adapter.create(data=data)
        result = self.adapter.query(operation='find', query={'test_id': data['test_id']})
        self.assertTrue(len(list(result)) == 1)
        self.adapter.delete(query={'test_id': data['test_id']})  # clean up

    def test_not_query_allowed(self):
        data = mock_data.get_standard()
        self.adapter.create(data=data)
        try:
            result = self.adapter.query(operation='find_one_and_delete', query={'test_id': data['test_id']})
            self.assertTrue(False)
        except Exception as error:
            self.assertTrue('query method is for read-only operations' in repr(error))

    def test_read_many(self):
        count = 0
        while (count < 3):
            data = mock_data.get_standard()
            data['test_query_id'] = 'some-query'
            self.adapter.create(data=data)
            count += 1
        results = self.adapter.read(query={'test_query_id': 'some-query'}, operation='query')
        for result in results:
            self.adapter.delete(query={'test_id': result['test_id']})  # clean up
        self.assertTrue(len(results) >= 3)

    def test_read_many_pagination(self):
        count = 0
        while count < 10:
            data = mock_data.get_standard()
            data['test_query_id'] = 'some-query'
            self.adapter.create(data=data)
            count += 1
        results = self.adapter.read(query={'test_query_id': 'some-query'}, operation='query', params={'skip': 5, 'limit': 5})
        for result in results:
            self.adapter.delete(query={'test_id': result['test_id']})  # clean up
        self.assertEqual(len(results), 5)

    def test_update_success(self):
        data = mock_data.get_standard()
        self.adapter.create(data=data)
        data['array_number'] = [4, 5, 6]
        result = self.adapter.update(query={'test_id': data['test_id']}, data=data, update_list_operation='replace')
        self.assertDictEqual(result, data)
        self.adapter.delete(query={'test_id': data['test_id']})  # clean up

    def test_update_fail(self):
        data = mock_data.get_standard()
        try:
            result = self.adapter.update(query={'test_id': data['test_id']}, data=data)
            self.assertTrue(False)
        except Exception as error:
            self.assertTrue('no document found by query:' in repr(error))

    def test_upsert(self):
        data = mock_data.get_standard()
        result = self.adapter.upsert(query={'test_id': data['test_id']}, data=data)
        result.pop('_id')
        self.assertDictEqual(result, data)
        self.adapter.delete(query={'test_id': data['test_id']})  # clean up

    def test_delete(self):
        data = mock_data.get_standard()
        self.adapter.create(data=data)
        result = self.adapter.delete(query={'test_id': data['test_id']})
        self.assertTrue(result.deleted_count == 1)
