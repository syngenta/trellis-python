from syngenta_digital_dta.postgres.sql_connector import SQLConnector


def sql_connection(func):
    __connections = {}
    def decorator(obj):

        if not __connections.get(obj.database):
            __connections[obj.database] = SQLConnector(obj)

        try:
            return func(obj, __connections[obj.database])
        except Exception as err:
            if 'already closed' in str(err):
                __connections[obj.database] = SQLConnector(obj)
                return func(obj, __connections[obj.database])
            else:
                raise err

    return decorator
