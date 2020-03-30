import utils
import json


class JsonIterator:
    def __init__(self, json_file_path, file_name, index_name, doc_type):
        self.json_obj = json.loads(utils.json_file_reader(json_file_path, file_name))
        self.counter = 0
        self.max_elements = len(self.json_obj)
        self.index_name = index_name
        self.doc_type = doc_type

    def __iter__(self): return self

    def next(self):  # __next__ in Python 3
        if self.max_elements <= self.counter:
            raise StopIteration
        else:
            return_counter = self.counter
            self.counter += 1
            result = {
                '_op_type': 'index',
                '_index': self.index_name,
                '_type': self.doc_type,
                '_source': dict(self.json_obj[return_counter])
            }
            return result

