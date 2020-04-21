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
        return_term_vector = {"terms": list(term_list)}
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


def get_terms(fetched_id, elasticsearch_client):
    return elasticsearch_client.termvectors(index=INDEX_NAME, doc_type=DOC_TYPE, id=fetched_id,
                                            offsets=False,
                                            fields=["plain_text"],
                                            positions=False, payloads=False, term_statistics=True,
                                            field_statistics=False)


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

    processes = []

    non_processed_id = []
    shared_term_dict = manager.dict()
    for index, fetched_id in enumerate(fetched_ids):
        try:
            time.sleep(0.3)
            terms = elasticsearch_client.termvectors(index=INDEX_NAME, doc_type=DOC_TYPE, id=fetched_id,
                                                     offsets=False,
                                                     fields=["plain_text"],
                                                     positions=False, payloads=False, term_statistics=True,
                                                     field_statistics=False)
        except Exception as e:
            try:
                terms = elasticsearch_client.termvectors(index=INDEX_NAME, doc_type=DOC_TYPE, id=fetched_id,
                                                         offsets=False,
                                                         fields=["plain_text"],
                                                         positions=False, payloads=False, term_statistics=True,
                                                         field_statistics=False)
            except Exception as e:
                non_processed_id.append(fetched_id)
                logging.info(
                    "{} Couldn't finish this doc index {} and id {} due to some error".format(e, index, fetched_id))
                logging.info(
                    "Skipping this doc index {} and id {} due to connection error".format(e, index, fetched_id))
                continue
        processes.append(Process(target=process_doc,
                                 args=(shared_term_dict, fetched_id, terms)))

        processes[len(processes) - 1].start()

    for i in range(len(processes)):
        try:
            processes[i].join()
        except Exception as e:
            logging.info(
                "{} couldn't find process as it wasn't started due to some error".format(e))

    print("Non processed document count: {}".format(len(set(non_processed_id))))
    logging.info(
        "Non processed document count: {}".format(len(set(non_processed_id))))
    for index, fetched_id in enumerate(fetched_ids):
        if fetched_id not in set(non_processed_id):
            final_data[fetched_id]["term_vectors"] = shared_term_dict[fetched_id]

    utils.json_file_writer(
        os.path.join(r"D:/ASU_Part_time/LiteratureAnalysis/ReverseTermVectorJstorData/",
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
    dir_path = r"D:/ASU_Part_time/LiteratureAnalysis/TermVectorJstorData/"
    extension = "json"
    files_to_proceed = utils.get_all_files(dir_path, extension)[::-1][
                       len(utils.get_all_files(dir_path, extension)) - 24302:]

    for file_number, file in enumerate(files_to_proceed):
        completed_files = open("./completed_files_list.csv", 'a')
        noncompleted_files = open("./noncompleted_files_list.csv", 'a')
        batch_start_time = time.time()
        try:
            print("Processing of {} file number {} Started ".format(file.split("\\")[-1], 24302 - file_number - 1))
            logging.info("Processing of file number {} with filename {} started".format(24302 - file_number - 1, file))
            get_term_vectors(file, elasticsearch_client, 24302 - file_number - 1)
            print("Processing of file number {} Completed".format(24302 - file_number - 1))
            logging.info(
                "Processing of file number {} with filename {} Completed".format(24302 - file_number - 1, file))
            completed_files.write(file + ",")
        except Exception as e:
            print("Error while Processing of file number {}".format(24302 - file_number - 1))
            logging.info(
                "Error occurred while getting term vectors of {} file: {} and error was {}".format(
                    24302 - file_number - 1, file, e))
            noncompleted_files.write(file + ",")
        print("Time Taken===> {}".format(time.time() - batch_start_time))
        completed_files.close()
        noncompleted_files.close()

    print("Time Taken===> {}".format(time.time() - batch_start_time))
    logger.info("Time Taken===> {}".format(time.time() - start_time))
    logger.info('Finished')
