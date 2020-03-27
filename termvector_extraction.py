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
        return self.elasticsearch_client.termvectors(index=self.INDEX_NAME, id=doc_id, offsets=False, fields=fields,
                                                positions=False, payloads=False)


if __name__ == "__main__":
    ES_HOST = 'localhost:9200'
    INDEX_NAME = "ramuji"
    DOC_TYPE = "_doc"
    db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST)
    elasticsearch_client = db_connection.get_elasticsearch_client
    termvector_extractor = TermvectorExtraction(ES_HOST, INDEX_NAME, DOC_TYPE, elasticsearch_client)

    all_docs = elasticsearch.helpers.scan(termvector_extractor.elasticsearch_client, query={"query": {"match_all": {}}}, scroll='1m',
                                          index=INDEX_NAME)
    print(type(all_docs))
    for doc in all_docs:
        print(json.dumps(termvector_extractor.get_termvectors_for_doc(doc['_id'], ['plain_text'])))

