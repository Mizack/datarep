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
        
    
    def create_database(self, database_name):
        try:
            sql = text(f"CREATE DATABASE {database_name}")
            return self.connection.execute(sql)
        except Exception as e:
            raise ValueError(f"Error searching databases: {e}")
                
    
    def create_table(self, script_creation):
        try:
            sql = text(script_creation)
            return self.connection.execute(sql)
        except Exception as e:
            raise ValueError(f"Error creating databases: {e}")
                
    
    def add_column(self, table, column):
        try:
            sql = text(f"ALTER TABLE {table} ADD COLUMN {column}")
            return self.connection.execute(sql)
        except Exception as e:
            raise ValueError(f"Error creating databases: {e}")
        
    
    def find_table(self, table):
        try:
            sql = text(f"DESCRIBE {table}")
            resultado = self.connection.execute(sql)
            
            return resultado.fetchall()
        except Exception as e:
            raise ValueError(f"Error searching table: {e}")
        
    
    def show_create_table(self, table):
        try:
            sql = text(f"SHOW CREATE TABLE {table}")
            resultado = self.connection.execute(sql)
            
            return resultado.fetchall()
        except Exception as e:
            raise ValueError(f"Error searching table: {e}")