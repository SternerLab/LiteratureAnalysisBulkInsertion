import json


def json_file_writer(path, file_name, data):
    with open(path + file_name, 'w') as outfile:
        json.dump(data, outfile)


def json_file_reader(path, file_name):
    with open(path + file_name) as json_file:
        data = json.load(json_file)
    return data
