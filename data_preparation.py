#!/usr/bin/env python2
import json
import logging
import math
import multiprocessing
import os
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


def term_vector_processing(term_vector, index):
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
        logging.error("{}: occured for one doc in index: {}".format(e, index))
        return {}


def process_doc(document, shared_doc_dict, index, doc_id):
    temp_dict = shared_doc_dict[index]
    temp_dict[doc_id] = {}
    temp_dict[doc_id]["year"] = document["article-meta"]["year"]
    temp_dict[doc_id]["article-id"] = document["article-meta"]["article-id"]
    temp_dict[doc_id]["journal-id"] = document["journal-meta"]["journal-id"]
    temp_dict[doc_id]["journal-title"] = document["journal-meta"]["journal-title"]
    temp_dict[doc_id]["term_vectors"] = {}
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
        scroll='1s',
        query=query,
        index=INDEX_NAME)
    return scanner


def scroll(elasticsearch_client, index, doc_type, query_body, page_size=25, debug=False, scroll='10h'):
    page = elasticsearch_client.search(index=index, doc_type=doc_type, scroll=scroll, size=page_size, body=query_body)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']
    total_pages = math.ceil(scroll_size / page_size)
    page_counter = 0
    if debug:
        print('Total items : {}'.format(scroll_size))
        print('Total pages : {}'.format(math.ceil(scroll_size / page_size)))
    # Start scrolling
    while scroll_size > 0:
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        if scroll_size > 0:
            if debug:
                print('> Scrolling page {} : {} items'.format(page_counter, scroll_size))
            yield total_pages, page_counter, scroll_size, page
        # get next page
        try:
            page = elasticsearch_client.scroll(scroll_id=sid, scroll='10h')
        except Exception as e:
            if e[0] == 404:
                logging.info(e)
        page_counter += 1
        # Update the scroll ID
        sid = page['_scroll_id']


def process(elasticsearch_client, data_directory, initial_offset):
    manager = Manager()
    offset = initial_offset
    # TODO: Processing limit number of records each time
    limit = 25
    result_template = {}
    for i in range(limit):
        result_template[i] = {}

    count = initial_offset // limit
    fetched_docs = []
    fetched_ids = []
    query = {
        "_source": {
            "includes": ["article.article-meta.year", "article.article-meta.article-id",
                         "article.journal-meta.journal-id", "article.journal-meta.journal-title"]
        },
        'query': {
            'match_all': {}
        }
    }
    doc_id_file = open("./doc_id_list.csv", 'a')
    for total_pages, page_counter, page_items, page_data in scroll(elasticsearch_client, INDEX_NAME, DOC_TYPE, query,
                                                                   page_size=limit):
        print('total_pages={}, page_counter={}, page_items={}'.format(total_pages, page_counter, page_items))
        batch_start_time = time.time()
        print("Batch {} started".format(count))
        for doc in page_data["hits"]["hits"]:
            fetched_docs.append(doc["_source"]["article"])
            print doc["_source"]["article"]["article-meta"]["article-id"]
            print doc["_source"]["article"]["article-meta"]["year"]
            fetched_ids.append(doc["_id"])
        # try:
        #     time.sleep(300)
        #     mTerms = elasticsearch_client.mtermvectors(index=INDEX_NAME, doc_type=DOC_TYPE, ids=fetched_ids,
        #                                                offsets=False,
        #                                                fields=["plain_text"],
        #                                                positions=False, payloads=False, term_statistics=True,
        #                                                field_statistics=True)
        # except Exception as e:
        #     logging.info("{} m_vectors failed 3 times and stopping the script".format(e))
        #     time.sleep(3)
        #     continue

        processes = []
        shared_doc_dict = manager.dict(result_template)
        for index, doc in enumerate(fetched_docs):
            processes.append(Process(target=process_doc,
                                     args=(
                                         doc, shared_doc_dict, index, fetched_ids[index])))

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
        count += 1
        if count == 5:
            break
        fetched_docs = []
        fetched_ids = []
        time.sleep(5)

    doc_id_file.close()


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
