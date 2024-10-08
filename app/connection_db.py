from app.database import Database
class Connection:
    def __init__(self, data_connection:dict, replicated_connection = False) -> None:
        self.replicated_connection = replicated_connection
        self.table_associated_to_database = {}

        self.host = data_connection.get("HOST")
        self.port = data_connection.get("PORT")
        self.user = data_connection.get("USER")
        self.password = data_connection.get("PASSWORD")
        self.database = data_connection.get("DATABASE")
        self.data = data_connection.get("DATA", False)
        self.dbms = data_connection.get("DBMS")


    @property
    def host(self) -> str:
        return self._host

    @host.setter
    def host(self, host:str):
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
    def user(self, user:str):
        self._user = user

    
    @property
    def password(self) -> str:
        return self._password

    @password.setter
    def password(self, password:str):
        self._password = password

    
    @property
    def database(self) -> list:
        return self._database

    @database.setter
    def database(self, database:list):
        self._database = database

    
    @property
    def data(self) -> bool:
        return self._data

    @data.setter
    def data(self, data:bool):
        self._data = data
    

    @property
    def dbms(self) -> str:
        return self._dbms

    @dbms.setter
    def dbms(self, dbms:str):
        if isinstance(dbms, str):
            dbms = dbms.lower()
        self._dbms = dbms


    def find_existing_databases(self):
        database_connection = self.__create_connection()
        databases_to_ignore = [
            "information_schema", "mysql", "performance_schema", "sys"
        ]
        databases_found = []
        database_informed = True if len(self.database) != 0 else False

        for database in database_connection.find_databases():
            database = database[0]

            if database in databases_to_ignore or database_informed and database not in self.database:
                continue
            elif not database_informed or database in self.database:
                databases_found.append(database)
                continue

            raise ValueError(f"Database {database} not found")

        self.database = databases_found
        
        if len(self.database) == 0:
            raise ValueError("Not found databases")


    def find_tables_from_database(self, database):
        database_connection = self.__create_connection(database)
        tables = []

        for table in database_connection.show_tables():
            tables.append(table[0])
        
        if len(tables) == 0:
            raise ValueError("Not found tables")

        self.table_associated_to_database[database] = tables


    def verify_if_database_exists(self, database):
        try:
            self.__create_connection(database)
            return True
        except Exception:
            return False


    def create_database(self, database):
        try:
            database_connection = self.__create_connection()
            database_connection.create_database(database)
            return True
        except Exception:
            return False
        

    def create_table(self, database, script_creation):
        try:
            database_connection = self.__create_connection(database)
            database_connection.create_table(f"""{script_creation}""")
            return True
        except Exception:
            return False
        

    def add_column(self, database, table, column):
        try:
            database_connection = self.__create_connection(database)
            database_connection.add_column(table, column)
            return True
        except Exception:
            return False


    def modify_constraint(self, database, table, constraint):
        try:
            database_connection = self.__create_connection(database)
            database_connection.modify_constraint(table, constraint)
            return True
        except Exception:
            return False


    def drop_column(self, database, table, column):
        try:
            database_connection = self.__create_connection(database)
            database_connection.drop_column(table, column)
            return True
        except Exception:
            return False
        

    def find_constraint_for_table(self, database, table, column):
        try:
            database_connection = self.__create_connection(database)
            return database_connection.describe_constraint_for_column(table, column)
        except Exception:
            return False
        

    def find_structure_table(self, database, table):
        try:
            database_connection = self.__create_connection(database)
            return database_connection.find_table(table)
        except Exception:
            return False

    
    def show_create_table(self, database, table):
        try:
            database_connection = self.__create_connection(database)
            return database_connection.show_create_table(table)[0][1]
        except Exception:
            return False


    def __create_connection(self, database = None):
        return Database(
            self.dbms, self.host, self.port, self.user, self.password, database
        )