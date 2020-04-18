import json
import logging
import multiprocessing
import sys
import time

import elasticsearch
import elasticsearch.helpers

import utils
from elasticsearch_connection import ElasticsearchConnection
from json_iterator import JsonIterator


def create_new_index(mapping_json, es_index_name, elasticsearch_client):
    with open(mapping_json, 'r') as fh:
        mappings = json.load(fh)["ketan_beckett_jstor_ngrams_term_vectors"]
    elasticsearch_client.indices.create(index=es_index_name, body=mappings)


def insert_bulk_data_parallely(elasticsearch_client, iterator, index_name, file_number,
                               thread_count=multiprocessing.cpu_count(),
                               chunk_size=5000,
                               max_chunk_bytes=500 * 1024 * 1024, queue_size=20):
    for success, info in elasticsearch.helpers.parallel_bulk(elasticsearch_client, iterator, thread_count, chunk_size,
                                                             max_chunk_bytes,
                                                             queue_size, index=index_name, raise_on_error=True):

        if not success:
            print('Doc failed', info)
            logging.info("Batch {} insertion failed".format(file_number))
        else:
            print("Batch {} inserted into index".format(file_number))
            logging.info("Batch {} inserted into index".format(file_number))


def insert_bulk_data(elasticsearch_client, iterator, index_name):
    for success, info in elasticsearch.helpers.bulk(client=elasticsearch_client, actions=iterator, index=index_name):

        if not success:
            print('Doc failed', info)
        else:
            logging.debug()


def init(ES_AUTH_USER, ES_AUTH_PASSWORD, ES_HOST, dir_path, index_name, doc_type):
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        filename='./Logs/batch_insertion.log',
                        level=logging.INFO)

    logger = logging.getLogger('BulkInsertion')
    logger.debug('Started')
    extension = "json"
    files_to_proceed = utils.get_all_files(dir_path, extension)

    print("Inserting {} number of files in index: {}".format(len(files_to_proceed), index_name))

    db_connection = ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    elasticsearch_client = db_connection.get_elasticsearch_client()

    # create_new_index("./mapping.json", index_name, elasticsearch_client)
    files_to_proceed = ["./mterms.json"]
    for i, file in enumerate(files_to_proceed):
        try:
            json_iterator = JsonIterator("", file, index_name, doc_type)
            insert_bulk_data_parallely(elasticsearch_client, json_iterator, index_name, i + 36)
        except Exception as e:
            logging.info(
                "{} and due to this couldn't insert records from file: {}".format(e, file))
            print "Failed to insert records from file: {}".format(e, file)
        break


if __name__ == "__main__":
    ES_AUTH_USER = sys.argv[1]
    ES_AUTH_PASSWORD = sys.argv[2]
    ES_HOST = sys.argv[3]
    dir_path = sys.argv[4]
    dir_path = r"D:\ASU_Part_time\LiteratureAnalysis\FullTermvectorResultJsonData\\"

    index_name = sys.argv[5]
    doc_type = sys.argv[6]
    start_time = time.time()
    init(ES_AUTH_USER, ES_AUTH_PASSWORD, ES_HOST, dir_path, index_name, doc_type)
    print("Time Taken===>", time.time() - start_time)
