import json
import multiprocessing
import os
import pprint
import sys

import elasticsearch_connection
import utils
from data_preparation import get_cpu_count

INDEX_NAME = "beckett_jstor_ngrams_all"
DOC_TYPE = "article"


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
        return {}


def get_mterm_vectors(fetched_ids, elasticsearch_client):
    return elasticsearch_client.mtermvectors(index=INDEX_NAME, doc_type=DOC_TYPE, ids=fetched_ids,
                                             offsets=False,
                                             fields=["plain_text"],
                                             positions=False, payloads=False, term_statistics=True,
                                             field_statistics=True)


def get_term_vectors(file, elasticsearch_client):
    data = json.loads(utils.json_file_reader(file, ""))
    final_data = {}
    fetched_ids = []
    for k in data:
        final_data[k.keys()[0]] = k[k.keys()[0]]
        fetched_ids.append(k.keys()[0])

    mTerms = get_mterm_vectors(fetched_ids, elasticsearch_client)
    for terms in mTerms["docs"]:
        final_data[terms["_id"]]["term_vectors"] = term_vector_processing(terms)

    utils.json_file_writer(os.path.join(r"./", "mterms.json"), "",
                           json.dumps(list(final_data.values())))


if __name__ == "__main__":
    ES_AUTH_USER = sys.argv[1]
    ES_AUTH_PASSWORD = sys.argv[2]
    ES_HOST = sys.argv[3]
    db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    elasticsearch_client = db_connection.get_elasticsearch_client()
    dir_path = r"D:\ASU_Part_time\LiteratureAnalysis\FullTermvectorResultJsonData\\"
    extension = "json"
    files_to_proceed = utils.get_all_files(dir_path, extension)
    for file in files_to_proceed:
        get_term_vectors(file, elasticsearch_client)
        break
