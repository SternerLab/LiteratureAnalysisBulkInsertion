import elasticsearch_connection
import elasticsearch
import pprint


class TermvectorExtraction:
    def __init__(self, es_host, index_name, doc_type):
        self.ES_HOST = es_host
        self.INDEX_NAME = "ramuji"
        self.DOC_TYPE = "_doc"
        db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST)
        self.elasticsearch_client = db_connection.get_elasticsearch_client

    def get_termvectors_for_doc(self, doc_id, fields):
        return self.elasticsearch_client.termvectors(index=self.INDEX_NAME, id=doc_id, offsets=False, fields=fields,
                                                positions=False, payloads=False)


if __name__ == "__main__":
    ES_HOST = 'localhost:9200'
    INDEX_NAME = "ramuji"
    DOC_TYPE = "_doc"
    termvector_extractor = TermvectorExtraction(ES_HOST, INDEX_NAME, DOC_TYPE)

    all_docs = elasticsearch.helpers.scan(termvector_extractor.elasticsearch_client, query={"query": {"match_all": {}}}, scroll='1m',
                                          index=INDEX_NAME)

    for doc in all_docs:
        print(termvector_extractor.get_termvectors_for_doc(doc['_id'], ['plain_text']))

