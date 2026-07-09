
import json
import asyncio
class JSONStorage:

    def __init__(self, path:str):
        self.file_path = path
        self.lock = asyncio.Lock()

    def get_data(self):
        with open(self.file_path, "r") as f:
            return json.load(f)
    
    def write_data(self, data:dict):
        with open(self.file_path, "w") as f:
            json.dump(data, f, indent=4)