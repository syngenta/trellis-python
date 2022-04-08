import os
from unittest import TestCase, mock

import syngenta_digital_dta
from syngenta_digital_dta.common import base_adapter


class TestFileSystemAdapter(TestCase):
    def setUp(self) -> None:
        self.adapter = syngenta_digital_dta.adapter(
            engine='file_system',
            sns_arn='test_sns_arn',
            sns_attributes={}
        )

    @mock.patch.object(base_adapter.publisher, 'publish')    
    def test_operations(self, mock_publish):
        destination_path = 'tmp/test.txt'
        file_object = 'This is a test\n'
        self.adapter.create(
            destination_path=destination_path,
            file_object=file_object
        )
        self.assertEqual(True, os.path.exists(destination_path))

        mock_publish.assert_called_with(
            endpoint=None,
            arn='test_sns_arn',
            attributes={'operation': {'DataType': 'String', 'StringValue': 'create'}},
            data={'file_path': 'tmp/test.txt'},
            fifo_group_id=None,
            fifo_duplication_id=None
        )
        
        results = self.adapter.read(file_path=destination_path)
        self.assertEqual(results, file_object)

        updated_object = 'Updating the test\n'
        self.adapter.update(
            destination_path=destination_path,
            file_object=updated_object
        )

        results = self.adapter.read(file_path=destination_path)
        self.assertEqual(results, updated_object)

        age = self.adapter.get_age(path=destination_path)
        self.assertEqual(float, type(age))

        self.adapter.delete(path='tmp', recursive=True)
        self.assertEqual(False, os.path.exists(destination_path))
