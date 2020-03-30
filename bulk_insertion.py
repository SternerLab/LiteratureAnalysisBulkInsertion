import logging
import os
import time

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


def insert_bulk_data_parallely(elasticsearch_client, iterator, index_name, i,
                               thread_count=multiprocessing.cpu_count() - 2,
                               chunk_size=500,
                               max_chunk_bytes=104857600, queue_size=4):
    for success, info in elasticsearch.helpers.parallel_bulk(elasticsearch_client, iterator, thread_count, chunk_size,
                                                             max_chunk_bytes,
                                                             queue_size, index=index_name, raise_on_error=True):

        if not success:
            print('Doc failed', info)
        else:
            logging.info("Batch {} inserted into index".format(i))


def insert_bulk_data(elasticsearch_client, iterator, index_name):
    for success, info in elasticsearch.helpers.bulk(client=elasticsearch_client, actions=iterator, index=index_name):

        if not success:
            print('Doc failed', info)
        else:
            logging.debug()


def init():
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        filename='./Logs/batch_insertion.log',
                        level=logging.INFO)

    logger = logging.getLogger('BulkInsertion')
    logger.debug('Started')
    index_name, doc_type = "bulk__insertion_index", "_doc"
    # dir_path = r"D:\ASU_Part_time\LiteratureAnalysis\TermvectorResultJsonData\\"
    dir_path = r"./Data//"
    extension = "json"
    files_to_proceed = utils.get_all_files(dir_path, extension)
    print(files_to_proceed)
    ES_AUTH_USER = 'ketan'
    ES_AUTH_PASSWORD = 'hk7PDr0I4toBA%e'
    ES_HOST = 'http://diging-elastic.asu.edu/elastic'
    # ES_AUTH_USER = ''
    # ES_AUTH_PASSWORD = ''
    # ES_HOST = 'localhost:9200'
    db_connection = ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    elasticsearch_client = db_connection.get_elasticsearch_client()

    create_new_index("./mapping.json", index_name, elasticsearch_client)

    for i, file in enumerate(files_to_proceed):
        json_iterator = JsonIterator("", file, index_name, doc_type)
        insert_bulk_data_parallely(elasticsearch_client, json_iterator, index_name, i)
        if os.path.isfile(file):
            os.remove(file)


if __name__ == "__main__":
    start_time = time.time()
    init()
    print "Time Taken===>", time.time() - start_time
