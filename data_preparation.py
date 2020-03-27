import multiprocessing
import elasticsearch
from multiprocessing import Manager
from termvector_extraction import TermvectorExtraction


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
    print("Process number:- {0} started".format(t_counter))
    gram = len(term.split(" "))
    term_count = term_dict["term_freq"]
    return_dict = {"term": term, "term_count": term_count, "gram": gram}
    term_list.append(return_dict)
    print("Process number:- {0} Completed".format(t_counter))


class DataPreparation:
    def __init__(self):
        pass

    # TODO: Process term vector using multiprocessing
    def term_vector_processing(self, term_vector):
        manager = Manager()
        term_list = manager.list()
        print term_list

        cpu_count = get_cpu_count()
        print cpu_count
        pool = multiprocessing.Pool(cpu_count)

        terms = term_vector["term_vectors"]["plain_text"]["terms"]
        # print(terms)
        term_counter = 0
        for term in terms:
            print term
            pool.apply_async(term_process, args=(term_list, term_counter, term, terms[term]))
            term_counter += 1

        pool.close()
        pool.join()

        print term_list

        return_term_vector = {"field_statistics": term_vector["term_vectors"]["plain_text"]["field_statistics"], "terms": list(term_list)}
        return return_term_vector


if __name__ == "__main__":
    data_preparation = DataPreparation()
    ES_HOST = "localhost:9200"
    INDEX_NAME = "literature"
    DOC_TYPE = "_doc"
    termvector_extractor = TermvectorExtraction(ES_HOST, INDEX_NAME, DOC_TYPE)

    all_docs = elasticsearch.helpers.scan(termvector_extractor.elasticsearch_client, query={"query": {"match_all": {}}},
                                          scroll='1m',
                                          index=INDEX_NAME)

    for doc in all_docs:
        term_vector = termvector_extractor.get_termvectors_for_doc(doc['_id'], ['plain_text'])
        print(data_preparation.term_vector_processing(term_vector))
