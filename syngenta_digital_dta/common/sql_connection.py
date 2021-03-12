from syngenta_digital_dta.common.sql_connector import SQLConnector


def sql_connection(func):
    __connections = {}
    def decorator(self_obj):
        if not __connections.get(self_obj.database):
            __connections[self_obj.database] = SQLConnector(self_obj)
        if self_obj.auto_connect:
            __connections[self_obj.database].connect()
        return func(self_obj, __connections[self_obj.database])
    return decorator
