import json
import os

from app.connection_db import Connection

class Replicator:
    def __init__(self, config_file = "connections.json") -> None:
        self.config_file = None
        self.file_path = None
        self.replicated_connection = None
        self.other_connections = []

        self.__validate_config_file(config_file)


    @property
    def config_file(self) -> dict:
        return self._config_file

    @config_file.setter
    def config_file(self, config_file):
        self._config_file = config_file
    

    def __validate_config_file(self, config_file):
        '''
        Verify if config file exists and install it.
        '''
        self.__verify_if_config_file_exists(config_file)
        self.__load_configs_databases()
        self.__check_json_fields()
        
    
    def __verify_if_config_file_exists(self, config_file):
        current_directory = os.getcwd()
        self.file_path = f"{current_directory}\config\{config_file}"

        if not os.path.exists(self.file_path):
            raise ValueError("Configuration file not found")
        

    def __load_configs_databases(self):
        try:
            file_open = open(self.file_path)
            self.config_file = json.load(file_open)
        except Exception as e:
            raise ValueError(f"Configuration file is invalid {e}")
        
    
    def __check_json_fields(self):
        self.replicated_connection = Connection(self.config_file.get("replicated_connection"))
        others_connections = self.config_file.get("other_connections")

        if not isinstance(others_connections, list):
            raise ValueError("The other connections weren't sent correctly")
        
        for connection in others_connections:
            self.other_connections.append(Connection(connection))


    def run(self):
        try:
            self.__find_databases_to_replicate()
            self.__associate_tables_to_databases()
            self.__replicate_databases_and_tables_to_others_connections()

        except Exception as e:
            raise ValueError(f"Error creating connection: {e}")
    

    def __find_databases_to_replicate(self):
        self.replicated_connection.find_existing_databases()

    
    def __associate_tables_to_databases(self):
        '''
        Verify which tables are in database and associate it in a dict.
        '''
        for database in self.replicated_connection.database:
            self.replicated_connection.find_tables_from_database(database)


    def __replicate_databases_and_tables_to_others_connections(self):
        for connection in self.other_connections:
            connection.dbms = self.replicated_connection.dbms

            for database in self.replicated_connection.table_associated_to_database.keys():
                self.__replicate_databases(connection, database)
                self.__replicate_tables(connection, database)
                

    def __replicate_databases(self, connection:Connection, database:str):
        if not connection.verify_if_database_exists(database):
            connection.create_database(database)


    def __replicate_tables(self, connection:Connection, database:str):
        for table in self.replicated_connection.table_associated_to_database.get(database):
            base_structure_table = self.replicated_connection.find_structure_table(database, table)
            structure_table = connection.find_structure_table(database, table)

            if structure_table == False:
                self.__replicate_table(connection, database, table)
                continue

            print(base_structure_table)
            print(structure_table)

    def __replicate_table(self, connection:Connection, database:str, table:str):
        original_table_structure = self.replicated_connection.show_create_table(database, table)
        if not connection.create_table(database, original_table_structure):
            print("ERRO")