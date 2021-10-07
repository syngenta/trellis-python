import typing

from syngenta_digital_dta.postgres.sql_connector import SQLConnector


def sql_connection(func: typing.Callable):
    __connections = {}

    def decorator(obj):
        if __connections.get(obj.database):
            if obj.connection:
                if obj.connection.closed or obj.cursor.closed:
                    __connections[obj.database] = SQLConnector(obj)
                return func(obj, __connections[obj.database])

        __connections[obj.database] = SQLConnector(obj)
        return func(obj, __connections[obj.database])

    return decorator
