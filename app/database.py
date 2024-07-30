import logging

from sqlalchemy import create_engine, text

class Database:
    def __init__(self, dbms, host, port, user, password, database = None) -> None:
        self.connection = None
        driver = self.__verify_driver(dbms)
        
        self.database_url = f"{dbms}+{driver}://{user}:{password}@{host}:{port}"
        if database:
            self.database_url = f"{self.database_url}/{database}"
        
        self.create_connection()


    def __verify_driver(self, dbms):
        '''
        Verify if driver is available for the given DBMS.
        '''
        available_drivers = {
            "mysql": "pymysql",
        }

        if dbms not in available_drivers:
            raise ValueError(f"Driver not available to DBMS ${dbms}")
        
        return available_drivers[dbms]


    def create_connection(self):
        try:
            engine = create_engine(self.database_url)
            self.connection = engine.connect()

        except Exception as e:
            raise ValueError(f"Error creating connection: {e}")
        

    def find_databases(self):
        try:
            sql = text("SHOW DATABASES")
            return self.connection.execute(sql)
        except Exception as e:
            raise ValueError(f"Error searching databases: {e}")
        

    def show_tables(self):
        try:
            sql = text("SHOW TABLES")
            return self.connection.execute(sql)
        except Exception as e:
            raise ValueError(f"Error searching databases: {e}")