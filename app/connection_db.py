class Connection:
    def __init__(self, data_connection:dict, replicated_connection = False) -> None:
        self.replicated_connection = replicated_connection

        self.host = data_connection.get("HOST")
        self.port = data_connection.get("PORT")
        self.user = data_connection.get("USER")
        self.password = data_connection.get("PASSWORD")
        self.database = data_connection.get("DATABASE")
        self.table = data_connection.get("TABLE")
        self.data = data_connection.get("DATA", False)
        self.dbms = data_connection.get("DBMS")


    @property
    def host(self) -> str:
        return self._host

    @host.setter
    def host(self, host):
        self._host = host

    
    @property
    def port(self) -> str:
        return self._port

    @port.setter
    def port(self, port):
        self._port = port

    
    @property
    def user(self) -> str:
        return self._user

    @user.setter
    def user(self, user):
        self._user = user

    
    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, password):
        self._password = password

    
    @property
    def database(self) -> list:
        return self._database

    @database.setter
    def database(self, database):
        self._database = database

    
    @property
    def table(self) -> list:
        return self._table

    @table.setter
    def table(self, table):
        self._table = table

    
    @property
    def data(self) -> bool:
        return self._data

    @data.setter
    def data(self, data):
        self._data = data
    

    @property
    def dbms(self) -> str:
        return self._dbms

    @dbms.setter
    def dbms(self, dbms):
        self._dbms = dbms