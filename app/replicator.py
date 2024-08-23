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
            self.__sort_tables_by_dependencies()
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


    def __sort_tables_by_dependencies(self):
        '''
        Sort tables by your foreign keys.
        '''
        foreign_keys = self.__get_foreign_keys()

        for database in foreign_keys:
            sorted_tables = []
            for table, fks in list(foreign_keys[database].items()):
                if not fks:
                    sorted_tables.insert(0, table)
                else:
                    sorted_tables.append(table)
            
            self.replicated_connection.table_associated_to_database[database] = sorted_tables

    
    def __get_foreign_keys(self):
        foreign_keys = {}
        
        for database in self.replicated_connection.table_associated_to_database:
            foreign_keys[database] = {}
            for table in self.replicated_connection.table_associated_to_database[database]:
                table_structure = self.replicated_connection.show_create_table(database, table)
                table_foreign_keys = []

                for line in table_structure.split('\n'):
                    if 'FOREIGN KEY' in line:
                        fk_table = line.split('REFERENCES ')[1].split(' ')[0].strip('`')
                        table_foreign_keys.append(fk_table)
                
                foreign_keys[database][table] = table_foreign_keys
        
        return foreign_keys


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

            base_structure_table = {column[0:]: column for column in base_structure_table}
            structure_table = {column[0:]: column for column in structure_table}

            self.__add_missing_columns(database, table, base_structure_table, structure_table, connection)
            self.__remove_remaining_columns(database, table, base_structure_table, structure_table, connection)


    def __replicate_table(self, connection:Connection, database:str, table:str):
        original_table_structure = self.replicated_connection.show_create_table(database, table)
        if not connection.create_table(database, original_table_structure):
            print("ERRO")


    def __add_missing_columns(self, database, table, base_structure_table:dict, structure_table:dict, connection:Connection):
        missing_columns = set(base_structure_table.keys()) - set(structure_table.keys())
        columns_to_add_constraints = []

        for column in missing_columns:
            null_column = "" if column[2] == "YES" else "NOT NULL"
            default = "" if column[4] == None else f"DEFAULT '{column[4]}'"
            extra = "" if not column[5] else f"{column[5]}"

            describe_column = f"{column[0]} {column[1]} {default} {null_column} {extra}"
            connection.add_column(database, table, describe_column)

            if column[3]:
                columns_to_add_constraints.append(column[0])
        
        self.__replicate_constraints(database, table, connection, columns_to_add_constraints)
        

    def __replicate_constraints(self, database, table, connection:Connection, columns_to_add_constraints):
        for column in columns_to_add_constraints:
            describe_constraint = self.replicated_connection.find_constraint_for_table(database, table, column)
            syntax_constraint = self.__generate_syntax_constraint(describe_constraint)
            self.__add_constraint(connection, database, table, syntax_constraint)


    def __generate_syntax_constraint(self, constraint_description:list):
        constraint_type = constraint_description[0][7]
        syntax_constraint = False

        if constraint_type == "FOREIGN KEY":
            syntax_constraint = f"""
                ADD CONSTRAINT {constraint_description[0][0]}
                FOREIGN KEY ({constraint_description[0][2]}) REFERENCES {constraint_description[0][3]}({constraint_description[0][4]})
                ON DELETE {constraint_description[0][6]}
                ON UPDATE {constraint_description[0][5]}
            """
        elif constraint_type == "PRIMARY KEY":
            keys = []
            for pk in constraint_description:
                keys.append(pk[2])
            syntax_constraint = f"ADD PRIMARY KEY ( {', '.join(keys)})"
        elif constraint_type == "UNIQUE":
            keys = []
            for unique in constraint_description:
                keys.append(unique[2])
            syntax_constraint = f"ADD CONSTRAINT {constraint_description[0][2]} UNIQUE ({', '.join(keys)})"

        return syntax_constraint
    

    def __add_constraint(self, connection:Connection, database, table, column):
        connection.modify_constraint(database, table, column)


    def __remove_remaining_columns(self, database, table, base_structure_table:dict, structure_table:dict, connection:Connection):
        remaining_columns = set(structure_table.keys()) - set(base_structure_table.keys())

        for column in remaining_columns:
            connection.drop_column(database, table, column[0])
            if column[3]:
                describe_constraint = self.replicated_connection.find_constraint_for_table(database, table, column)
                self.__drop_constraint(connection, database, table, describe_constraint)
    

    def __drop_constraint(self, connection:Connection, database, table, constraint_description):
        constraint_type = constraint_description[7]
        syntax_constraint = False

        if constraint_type == "FOREIGN KEY":
            syntax_constraint = f"""
                DROP FOREIGN KEY {constraint_description[0]}
            """
        elif constraint_type == "PRIMARY KEY":
            syntax_constraint = """DROP PRIMARY KEY"""
        elif constraint_type == "UNIQUE":
            syntax_constraint = f"""
                DROP INDEX {constraint_description[0]}
            """

        connection.modify_constraint(database, table, syntax_constraint)