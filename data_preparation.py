import utils
import json
import multiprocessing
import elasticsearch
from multiprocessing import Manager
from termvector_extraction import TermvectorExtraction
import elasticsearch_connection
import elasticsearch
from elasticsearch_dsl import Search
from multiprocessing import Process, Manager

import sys

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


def term_process(term_list, t_counter, term, term_dict):
    # print("Process number:- {0} started".format(t_counter))
    gram = len(term.split(" "))
    term_count = term_dict["term_freq"]
    return_dict = {"term": term, "term_count": term_count, "gram": gram}
    term_list.append(return_dict)
    if t_counter % 100 == 0:
        print("Process number:- {0} Completed".format(t_counter))


# TODO: Process term vector using multiprocessing
def term_vector_processing(term_vector, term_list):
    cpu_count = get_cpu_count()
    pool = multiprocessing.Pool(cpu_count)
    if "term_vectors" in term_vector:
        terms = term_vector["term_vectors"]["plain_text"]["terms"]
        term_counter = 0
        for term in terms:
            # print term
            pool.apply_async(term_process, args=(term_list, term_counter, term, terms[term]))
            term_counter += 1

        pool.close()
        pool.join()
        return_term_vector = {"field_statistics": term_vector["term_vectors"]["plain_text"]["field_statistics"],
                              "terms": list(term_list)}
        return return_term_vector
    return {}


def process_doc(document, shared_doc_list, terms, shared_terms):
    new_doc_dict = {"year": document["_source"]["article"]["article-meta"]["year"],
                    "article-id": document["_source"]["article"]["article-meta"]["article-id"],
                    "journal-id": document["_source"]["article"]["journal-meta"]["journal-id"],
                    "journal-title": document["_source"]["article"]["journal-meta"]["journal-title"],
                    # "plain_text": document["_source"]["plain_text"],
                    "term_vectors": term_vector_processing(terms, shared_terms)}
    shared_doc_list.append(new_doc_dict)


def get_termvectors_for_doc(elasticsearch_client, doc_id, fields=['plain_text'], ):
    return elasticsearch_client.termvectors(index="beckett_jstor", id=doc_id, offsets=False, fields=fields,
                                            positions=False, payloads=False)


def process(elasticsearch_client):
    manager = Manager()

    cpu_count = get_cpu_count()
    pool = multiprocessing.Pool(cpu_count)
    is_done = False
    offset = 0
    # TODO: Processing limit number of records each time
    limit = 20
    while not is_done:
        fetched_docs = elasticsearch_client.search(index="beckett_jstor_ngrams_part", doc_type='article', size=limit,
                                                   from_=offset)
        fetched_docs = fetched_docs["hits"]["hits"]
        process = []
        shared_doc_list = manager.list()
        shared_term_dict = manager.dict()
        for index, doc in enumerate(fetched_docs):
            shared_term_dict[index] = manager.list()
            terms = elasticsearch_client.termvectors(index="beckett_jstor_ngrams_part", id=doc["_id"], offsets=False,
                                                     fields=["plain_text"],
                                                     positions=False, payloads=False)
            # shared_terms = term_vector_processing(terms, shared_terms_list)
            process.append(Process(target=process_doc,
                                   args=(doc, shared_doc_list, terms, shared_term_dict[index])))
            print("process {} started".format(index))
            process[index].start()

        offset += limit
        for i in range(index):
            process[i].join()
        if len(fetched_docs) < limit:
            is_done = True
        break
    utils.json_file_writer("./", "result.json", json.dumps(list(shared_doc_list)))
    # print(list(shared_doc_list))
    print(len(list(shared_doc_list)))


if __name__ == "__main__":
    ES_AUTH_USER = 'ketan'
    ES_AUTH_PASSWORD = 'hk7PDr0I4toBA%e'
    ES_HOST = 'http://diging-elastic.asu.edu/elastic'
    db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)

    elasticsearch_client = db_connection.get_elasticsearch_client()
    INDEX_NAME = "beckett_jstor"
    DOC_TYPE = "article"
    process(elasticsearch_client)
