import json
import os

from app.connection_db import Connection

class Replicator:
    def __init__(self, config_file = "connections.json") -> None:
        self.config_file = config_file
        self.__validate_config_file()


    @property
    def config_file(self):
        return self._config_file

    @config_file.setter
    def config_file(self, config_file):
        self._config_file = config_file


    def run(self):
        return
    

    def __validate_config_file(self):
        '''
        Verify if config file exists and install it.
        '''
        self.__verify_if_config_file_exists()
        
    
    def __verify_if_config_file_exists(self):
        current_directory = os.getcwd()
        file_path = f"{current_directory}\config\{self.config_file}"

        if not os.path.exists(file_path):
            raise ValueError("Configuration file not found")
        
    
