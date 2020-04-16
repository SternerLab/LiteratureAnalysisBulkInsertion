#!/usr/bin/env python2
import json
import logging
import multiprocessing
import os
import pprint
import sys
import time
from multiprocessing import Process, Manager
import elasticsearch.helpers
import elasticsearch_connection
import utils

sys.setrecursionlimit(10000)

INDEX_NAME = "beckett_jstor_ngrams_all"
DOC_TYPE = "article"


def get_cpu_count():
    cpu_count = multiprocessing.cpu_count()
    if cpu_count >= 30:
        cpu_count = 30
    elif cpu_count >= 10:
        cpu_count = 8
    elif cpu_count >= 8:
        cpu_count = 6
    elif cpu_count == 4:
        cpu_count = 3
    else:
        cpu_count = 2
    return cpu_count


def term_process(term_list, term, term_dict):
    term_count = term_dict["term_freq"]
    term_word_list = term.split()
    return_dict = {"term": " ".join(term_word_list), "term_count": term_count, "gram": len(term_word_list)}
    term_list.append(return_dict)


def term_vector_processing(term_vector, doc_id, index):
    try:
        cpu_count = get_cpu_count()
        pool = multiprocessing.Pool(cpu_count)
        term_list = Manager().list()
        terms = term_vector["term_vectors"]["plain_text"]["terms"]
        for term in terms:
            pool.apply_async(term_process, args=(term_list, term, terms[term]))

        pool.close()
        pool.join()
        return_term_vector = {"field_statistics": term_vector["term_vectors"]["plain_text"]["field_statistics"],
                              "terms": list(term_list)}
        return return_term_vector
    except KeyError as e:
        logging.error("{}:  for doc_id :{} and index: {}".format(e, doc_id, index))
        return {}


def process_doc(document, shared_doc_dict, index, terms, doc_id):
    temp_dict = shared_doc_dict[index]

    temp_dict["year"] = document["_source"]["article"]["article-meta"]["year"]
    temp_dict["year"] = document["_source"]["article"]["article-meta"]["year"]
    temp_dict["article-id"] = document["_source"]["article"]["article-meta"]["article-id"]
    temp_dict["journal-id"] = document["_source"]["article"]["journal-meta"]["journal-id"]
    temp_dict["journal-title"] = document["_source"]["article"]["journal-meta"]["journal-title"]
    temp_dict["term_vectors"] = term_vector_processing(terms, doc_id, index)
    shared_doc_dict[index] = temp_dict


def get_scanner(elasticsearch_client, size):
    query = {
        'size': size,
        'query': {
            'match_all': {}
        }
    }
    scanner = elasticsearch.helpers.scan(
        client=elasticsearch_client,
        scroll='2m',
        query=query,
        index=INDEX_NAME)
    return scanner


def process(elasticsearch_client, data_directory, initial_offset):
    manager = Manager()
    offset = initial_offset
    # TODO: Processing limit number of records each time
    limit = 2
    result_template = {}
    for i in range(limit):
        result_template[i] = {}

    count = initial_offset // limit
    fetched_docs = []
    fetched_ids = []
    scanner = get_scanner(elasticsearch_client, limit)

    limit_counter = 0
    mTerms_counter = 0
    for doc in scanner:
        print "==================================================================================="
        if limit_counter < limit:
            print "Fetching docs for batch: {} and id: {}".format(count, doc["_id"])
            fetched_docs.append(doc)
            fetched_ids.append(doc["_id"])
            limit_counter += 1
        else:
            print(fetched_ids)
            print("Starting batch: {}".format(count))
            batch_start_time = time.time()
            time.sleep(1)
            try:
                mTerms = elasticsearch_client.mtermvectors(index=INDEX_NAME, doc_type=DOC_TYPE, ids=fetched_ids,
                                                           offsets=False,
                                                           fields=["plain_text"],
                                                           positions=False, payloads=False, term_statistics=True,
                                                           field_statistics=True)
                mTerms_counter = 0
            except Exception as e:
                if mTerms_counter == 0:
                    logging.info("{} m_vectors failed 3 times and stopping the script".format(e))
                    break
                time.sleep(3)
                mTerms_counter += 1
                continue

            processes = []
            shared_doc_dict = manager.dict(result_template)
            for index, document in enumerate(fetched_docs):
                processes.append(Process(target=process_doc,
                                         args=(
                                             document, shared_doc_dict, index, mTerms["docs"][index], document["_id"])))

                processes[index].start()

            offset += limit
            for i in range(len(fetched_docs)):
                try:
                    processes[i].join()
                except Exception as e:
                    logging.info(
                        "{} couldn't find process as it wasn't started due to some error".format(e))

            utils.json_file_writer(os.path.join(data_directory, "result_{}.json".format(count)), "",
                                   json.dumps(list(shared_doc_dict.values())))
            logging.info(
                "Batch {} completed and has {} records in it and result saved in file: {}. It contains {} records".format(
                    count, len(
                        list(shared_doc_dict.values())),
                    os.path.join(data_directory, "result_{}.json".format(count)),
                    len(list(shared_doc_dict.values()))))
            print("batch {} completed in {}".format(count, time.time() - batch_start_time))
            time.sleep(1)
            count += 1
            limit_counter = 0
            fetched_docs = []
            fetched_ids = []


def init(ES_AUTH_USER, ES_AUTH_PASSWORD, ES_HOST, data_directory, initial_offset):
    start_time = time.time()
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        filename='./Logs/data_preparation.log',
                        level=logging.INFO)

    logger = logging.getLogger('LiteratureAnalysis')
    logger.info('Started')
    db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    elasticsearch_client = db_connection.get_elasticsearch_client()
    process(elasticsearch_client, data_directory, initial_offset)
    print "Time Taken===>", time.time() - start_time
    logger.info("Time Taken===> {}".format(time.time() - start_time))
    logger.info('Finished')


if __name__ == "__main__":
    ES_AUTH_USER = sys.argv[1]
    ES_AUTH_PASSWORD = sys.argv[2]
    ES_HOST = sys.argv[3]
    data_directory = sys.argv[4]
    initial_offset = sys.argv[5]
    init(ES_AUTH_USER, ES_AUTH_PASSWORD, ES_HOST, r"D:\ASU_Part_time\LiteratureAnalysis\FullTermvectorResultJsonData\\",
         int(initial_offset))
