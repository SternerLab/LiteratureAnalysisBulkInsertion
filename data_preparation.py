#!/usr/bin/env python2
import json
import logging
import multiprocessing
import os
import sys
import time
from multiprocessing import Process, Manager

import elasticsearch_connection
import utils

sys.setrecursionlimit(10000)


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
    gram = len(term.split(" "))
    term_count = term_dict["term_freq"]
    return_dict = {"term": term.strip(), "term_count": term_count, "gram": gram}
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


def process(elasticsearch_client, index_name, doc_type, data_directory, initial_offset):
    manager = Manager()
    is_done = False
    offset = initial_offset
    # TODO: Processing limit number of records each time
    limit = 200

    result_template = {}
    for i in range(limit):
        result_template[i] = {}

    count = initial_offset
    while not is_done:
        try:
            fetched_docs = elasticsearch_client.search(index=index_name, doc_type=doc_type, size=limit,
                                                       from_=offset)
        except Exception as e:
            logging.info(
                "{} Couldn't get records trying again for limit:{} and offset:{}".format(e, limit, offset))
            continue
        fetched_docs = fetched_docs["hits"]["hits"]
        processes = []
        print count
        shared_doc_dict = manager.dict(result_template)
        for index, doc in enumerate(fetched_docs):
            try:
                terms = elasticsearch_client.termvectors(index=index_name, id=doc["_id"], offsets=False,
                                                         fields=["plain_text"],
                                                         positions=False, payloads=False)
                if "term_vectors" not in terms:
                    terms = elasticsearch_client.termvectors(index=index_name, id=doc["_id"], offsets=False,
                                                             fields=["plain_text"],
                                                             positions=False, payloads=False)
            except Exception as e:
                logging.info(
                    "{} Couldn't finish this doc index {} and id {} due to some error".format(e, index, doc["_id"]))
                logging.info(
                    "Skipping this doc index {} and id {} due to connection error".format(e, index, doc["_id"]))
                continue
            processes.append(Process(target=process_doc,
                                     args=(
                                         doc, shared_doc_dict, index, terms, doc["_id"])))

            processes[index].start()

        offset += limit
        for i in range(limit):
            try:
                processes[i].join()
            except Exception as e:
                logging.info(
                    "{} couldn't find process as it wasn't started due to some error".format(e))

        if len(fetched_docs) < limit:
            logging.info(
                "This is last batch.")
            is_done = True

        count += 1
        print("batch {} completed".format(count))
        utils.json_file_writer(os.path.join(data_directory, "result_{}.json".format(count)), "",
                               json.dumps(list(shared_doc_dict.values())))
        logging.info(
            "Batch {} completed and has {} records in it and result saved in file: {}. It contains {} records".format(
                count, len(
                    list(shared_doc_dict.values())), os.path.join(data_directory, "result_{}.json".format(count)),
                len(list(shared_doc_dict.values()))))
        # if count == 2:
        #     is_done = True


def init(initial_offset):
    start_time = time.time()
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        filename='./Logs/data_preparation.log',
                        level=logging.INFO)

    logger = logging.getLogger('LiteratureAnalysis')
    logger.info('Started')
    ES_AUTH_USER = 'ketan'
    ES_AUTH_PASSWORD = 'hk7PDr0I4toBA%e'
    ES_HOST = 'http://diging-elastic.asu.edu/elastic'
    INDEX_NAME = "beckett_jstor_ngrams_part"
    DOC_TYPE = "article"
    # data_directory = r"D:\ASU_Part_time\LiteratureAnalysis\TermvectorResultJsonData\\"
    data_directory = r"Data\\"
    db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)

    elasticsearch_client = db_connection.get_elasticsearch_client()

    process(elasticsearch_client, INDEX_NAME, DOC_TYPE, data_directory, initial_offset)
    print "Time Taken===>", time.time() - start_time
    logger.info("Time Taken===> {}".format(time.time() - start_time))
    logger.info('Finished')


if __name__ == "__main__":
    # ES_AUTH_USER = sys.argv[1]
    # ES_AUTH_PASSWORD = sys.argv[2]
    # ES_HOST = sys.argv[3]
    # INDEX_NAME = sys.argv[4]
    # DOC_TYPE = sys.argv[5]
    # data_directory = sys.argv[6]
    # initial_offset = sys.argv[7]
    init(int(0))
