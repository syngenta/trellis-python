import abc
from functools import lru_cache

import psycopg2


# class SQLConnector(abc.ABC):
#     def __init__(self, **kwargs):
#         self.host = kwargs['endpoint']
#         self.database = kwargs['database']
#         self.table = kwargs['table']
#         self.user = kwargs['user']
#         self.password = kwargs['password']
#         self.port = kwargs.get('port', 5439)
#         self.connection = None
#         self.cursor = None
#
#     @lru_cache(maxsize=128)
#     def connect(self):
#         try:
#             if not self.connection:
#                 print('connect')
#                 self.connection = psycopg2.connect(
#                     dbname=self.database,
#                     host=self.host,
#                     port=self.port,
#                     user=self.user,
#                     password=self.password
#                 )
#                 self.cursor = self.connection.cursor()
#         except Exception as error:
#             print(error)
#             raise error
#
#     def __del__(self):
#         try:
#             self.cursor.close()
#             self.connection.close()
#             print('closed')
#         except Exception as error:
#             print(error)
__connections = {}

def sql_connection():
    def wraper(cls):
        key = 
