import yaml
from data_importer import *

class DatabaseConnection:
    def __init__(self, config_path):
        self.config_path = config_path

    def load_details(self):
        with open(self.config_path, 'r') as f:
            data = yaml.full_load(f)
        self.database_type = data.get('database_type')
        self.database = data.get('database')
        self.collection = data.get('collection')
        self.username = data.get('username')
        self.password = data.get('password')
        self.host = data.get('host')
        self.port = data.get('port')
        self.schema = data.get('schema')
        self.table_name = data.get('table')
        self.full_load = data.get('full_load')
        self.start = data.get('start_date')
        self.end = data.get('end_date')
        self.frequency = data.get('frequency')
        self.output = data.get('output')
        return self.database_type

