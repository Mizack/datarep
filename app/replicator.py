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
        return