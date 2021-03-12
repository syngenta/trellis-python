from syngenta_digital_dta.common.sql_connection import sql_connection

class RedShiftAdapter():
    def __init__(self, **kwargs):
        self.endpoint = kwargs['endpoint']
        self.database = kwargs['database']
        self.table = kwargs['table']
        self.user = kwargs['user']
        self.password = kwargs['password']
        self.port = kwargs.get('port', 5439)
        self.autoconnect = kwargs.get('autoconnect', False)
        self.autocommit = kwargs.get('autocommit', False)
        self.connection = None
        self.cursor = None

    @sql_connection
    def connect(self, connector):
        self.connection = connector.connect()
        self.cursor = self.connection.cursor()

    def disconnect(self):
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            if self.connection:
                self.connection.close()
                self.connection = None
        except Exception as error:
            print(error)

    def commit(self):
        self.connection.commit()

    def __del__(self):
        if self.autocommit:
            self.commit()
        if self.autoconnect:
            self.disconnect()
