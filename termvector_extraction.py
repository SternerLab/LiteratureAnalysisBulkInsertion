import json

import elasticsearch_connection
import elasticsearch
import pprint


class TermvectorExtraction:
    def __init__(self, es_host, index_name, doc_type, elasticsearch_client):
        self.ES_HOST = es_host
        self.INDEX_NAME = index_name
        self.DOC_TYPE = doc_type
        self.elasticsearch_client = elasticsearch_client

    def get_termvectors_for_doc(self, doc_id, fields):
        return self.elasticsearch_client.mtermvectors(index=self.INDEX_NAME, doc_type=self.DOC_TYPE, ids=doc_id, offsets=False, fields=fields,
                                                positions=False, payloads=False)


if __name__ == "__main__":
    ES_AUTH_USER = 'ketan'
    ES_AUTH_PASSWORD = 'hk7PDr0I4toBA%e'
    ES_HOST = 'http://diging-elastic.asu.edu/elastic'
    db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    INDEX_NAME = "beckett_jstor_ngrams_part"
    DOC_TYPE = "article"
    elasticsearch_client = db_connection.get_elasticsearch_client()
    termvector_extractor = TermvectorExtraction(ES_HOST, INDEX_NAME, DOC_TYPE, elasticsearch_client)
    #
    # all_docs = elasticsearch.helpers.scan(termvector_extractor.elasticsearch_client, query={"query": {"match_all": {}}}, scroll='1m',
    #                                       index=INDEX_NAME)
    # print(type(all_docs))
    # for doc in all_docs:
    termvector_extractor.get_termvectors_for_doc(['14','19'], ['plain_text'])

