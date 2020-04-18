import json
import logging
import multiprocessing
import os
import sys
import time
from multiprocessing import Process

import elasticsearch_connection
import utils
from data_preparation import get_cpu_count

INDEX_NAME = "beckett_jstor_ngrams_all"
DOC_TYPE = "article"


def process_doc(shared_doc_dict, id, terms):
    shared_doc_dict[id] = term_vector_processing(terms)


def term_process(term_list, term, term_dict):
    term_count = term_dict["term_freq"]
    term_word_list = term.split()
    return_dict = {"term": " ".join(term_word_list), "term_count": term_count, "gram": len(term_word_list)}
    term_list.append(return_dict)


def term_vector_processing(term_vector):
    try:
        cpu_count = get_cpu_count()
        pool = multiprocessing.Pool(cpu_count)
        term_list = multiprocessing.Manager().list()
        terms = term_vector["term_vectors"]["plain_text"]["terms"]
        for term in terms:
            pool.apply_async(term_process, args=(term_list, term, terms[term]))

        pool.close()
        pool.join()
        return_term_vector = {"field_statistics": term_vector["term_vectors"]["plain_text"]["field_statistics"],
                              "terms": list(term_list)}
        return return_term_vector
    except KeyError as e:
        logging.error("{}: while getting mterms".format(e))
        return {}


def get_mterm_vectors(fetched_ids, elasticsearch_client):
    return elasticsearch_client.mtermvectors(index=INDEX_NAME, doc_type=DOC_TYPE, ids=fetched_ids,
                                             offsets=False,
                                             fields=["plain_text"],
                                             positions=False, payloads=False, term_statistics=True,
                                             field_statistics=True)


def get_term_vectors(file, elasticsearch_client, file_number):
    manager = multiprocessing.Manager()
    data = json.loads(utils.json_file_reader(file, ""))
    final_data = {}
    fetched_ids = []
    for k in data:
        try:
            final_data[k.keys()[0]] = k[k.keys()[0]]
            fetched_ids.append(k.keys()[0])
        except Exception as e:
            logging.info("Error occured while fetching ids from stored json files: {}".format(e))

    mTerms = get_mterm_vectors(fetched_ids, elasticsearch_client)

    processes = []

    shared_term_dict = manager.dict()
    for index, terms in enumerate(mTerms["docs"]):
        processes.append(Process(target=process_doc,
                                 args=(shared_term_dict, terms["_id"], terms)))

        processes[index].start()

    for i in range(len(mTerms["docs"])):
        try:
            processes[i].join()
        except Exception as e:
            logging.info(
                "{} couldn't find process as it wasn't started due to some error".format(e))

    for index, terms in enumerate(mTerms["docs"]):
        final_data[terms["_id"]]["term_vectors"] = shared_term_dict[terms["_id"]]

    utils.json_file_writer(
        os.path.join(r"./",
                     "result_{}.json".format(file_number)), "",
        json.dumps(list(final_data.values())))


if __name__ == "__main__":
    ES_AUTH_USER = sys.argv[1]
    ES_AUTH_PASSWORD = sys.argv[2]
    ES_HOST = sys.argv[3]
    start_time = time.time()
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        filename='./Logs/mterms_data_preparation.log',
                        level=logging.INFO)

    logger = logging.getLogger('LiteratureAnalysis')
    logger.info('Started')
    db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    elasticsearch_client = db_connection.get_elasticsearch_client()
    dir_path = r"D:\ASU_Part_time\LiteratureAnalysis\FullTermvectorResultJsonData\\"
    extension = "json"
    files_to_proceed = utils.get_all_files(dir_path, extension)
    for file_number, file in enumerate(files_to_proceed):
        batch_start_time = time.time()
        try:
            print("Processing of file number {} Started".format(file_number))
            logging.info("Processing of file number {} with filename {} started".format(file_number, file))
            get_term_vectors(file, elasticsearch_client, file_number)
            print("Processing of file number {} Completed".format(file_number))
            logging.info("Processing of file number {} with filename {} Completed".format(file_number, file))

        except Exception as e:
            logging.info(
                "Error occurred while getting term vectors of {} file: {} and error was {}".format(file_number, file, e))

        print("Time Taken===> {}".format(time.time() - batch_start_time))
        break

    print("Time Taken===> {}".format(time.time() - batch_start_time))
    logger.info("Time Taken===> {}".format(time.time() - start_time))
    logger.info('Finished')
