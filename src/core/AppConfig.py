import json
import os
from logging import exception

class AppConfig:

    def __init__(self):
        self.config_file = None
        self.config_data = None

    def load_configuration(self, config_file_path = None):
        if config_file_path is not None:
            self.config_file = config_file_path
        else:
            self.config_file = os.environ.get("APP_CONFIG") + "/appconfig.json"
        try:
            with open(self.config_file, encoding='utf-8-sig', errors='ignore') as json_config_file:
                self.config_data = json.load(json_config_file, strict=False)
        except exception as e:
            print(e)
        return self.config_data
