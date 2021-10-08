from syngenta_digital_dta.postgres.sql_connector import SQLConnector
import typing


def sql_connection(func: typing.Callable) -> typing.Callable:
    __connections = {}

    def decorator(obj: typing.Union["PostgresAdapter", "RedshiftAdapter"]):

        # reuse the existing connection if it isn't closed
        if __connections.get(obj.database):
            if __connections[obj.database].connection:
                if not __connections[obj.database].connection.closed:
                    return func(obj, __connections[obj.database])

        __connections[obj.database] = SQLConnector(obj)
        return func(obj, __connections[obj.database])

    return decorator
