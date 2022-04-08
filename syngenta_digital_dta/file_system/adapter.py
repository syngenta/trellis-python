import os
import shutil
import requests

import jsonpickle

from syngenta_digital_dta.common.base_adapter import BaseAdapter


class FileSystemAdapter(BaseAdapter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sns_attributes = kwargs.get('sns_attributes')
        self.sns_arn = kwargs.get('sns_arn')

    def create(self, **kwargs):
        body = self.__set_body(**kwargs)
        destination_path = kwargs['destination_path']
        publish = kwargs.get('publish', True)

        self.__create_destination_directory(destination_path)

        with open(destination_path, 'w') as file:
            file.write(body)

        if publish:
            data = {'file_path': destination_path}
            super().publish('create', data)

    def read(self, **kwargs):
        file_path = kwargs['file_path']
        json = kwargs.get('json', False)

        with open(file_path, 'r') as file:
            body = file.read()

        if json:
            return jsonpickle.decode(body)
        return body

    def update(self, **kwargs):
        destination_path = kwargs['destination_path']

        if not os.path.isfile(destination_path):
            raise FileNotFoundError(f'No such file: {destination_path}')

        self.create(**kwargs)

    def delete(self, **kwargs):
        path = kwargs['path']
        recursive = kwargs.get('recursive', False)

        if os.path.isdir(path) and recursive:
            shutil.rmtree(path)
        elif os.path.isdir(path):
            raise DeleteDirectoryException(f'Set recursive to True to delete contents in folder at {path}')
        else:
            os.remove(path)

    def get_age(self, **kwargs):
        path = kwargs['path']
        return os.stat(path).st_ctime

    def __set_body(self, **kwargs):
        if kwargs.get('file_object'):
            return kwargs['file_object']
        if kwargs.get('file_path'):
            return self.read(file_path=kwargs['file_path'])
        if kwargs.get('http_link') or kwargs.get('s3_link'):
            return self.__download_file(**kwargs)
        raise FileObjectExpception('No file_object, file_path, http_link, or s3_link provided')

    def __create_destination_directory(self, destination_path):
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    def __download_file(self, **kwargs):
        link = kwargs.get('http_link', kwargs['s3_link'])
        headers = kwargs.get('headers')

        response = requests.request('GET', link, headers=headers)
        return response.content


class DeleteDirectoryException(Exception):
    pass


class FileObjectExpception(Exception):
    pass
