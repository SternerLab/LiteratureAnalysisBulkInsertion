from elasticsearch_connection import ElasticsearchConnection
import elasticsearch
import elasticsearch.helpers
import json
import multiprocessing
import utils
from json_iterator import JsonIterator


def create_new_index(mapping_json, es_index_name, elasticsearch_client):
    with open(mapping_json, 'r') as fh:
        mappings = json.load(fh).values()[0]
    elasticsearch_client.indices.create(index=es_index_name, body=mappings)


def insert_bulk_data_parallely(elasticsearch_client, iterator, index_name, thread_count=multiprocessing.cpu_count() - 2,
                               chunk_size=500,
                               max_chunk_bytes=104857600, queue_size=4):
    for success, info in elasticsearch.helpers.parallel_bulk(elasticsearch_client, iterator, thread_count, chunk_size,
                                                             max_chunk_bytes,
                                                             queue_size, index=index_name, raise_on_error=True):

        if not success:
            print('Doc failed', info)


def insert_bulk_data(elasticsearch_client, iterator, index_name):
    for success, info in elasticsearch.helpers.bulk(client=elasticsearch_client, actions=iterator, index=index_name):

        if not success:
            print('Doc failed', info)


def init():
    dir_path = "./Data"
    extension = "json"
    files_to_proceed = utils.get_all_files(dir_path, extension)
    ES_AUTH_USER = ''
    ES_AUTH_PASSWORD = ''
    ES_HOST = 'localhost:9200'
    db_connection = ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    elasticsearch_client = db_connection.get_elasticsearch_client()
    create_new_index("./mapping.json", "bulk_insertion_index", elasticsearch_client)
    index_name, doc_type = "bulk_insertion_index", "_doc"
    for file in files_to_proceed:
        json_iterator = JsonIterator("", file, index_name, doc_type)
        insert_bulk_data_parallely(elasticsearch_client, json_iterator, index_name)


if __name__ == "__main__":
    init()
