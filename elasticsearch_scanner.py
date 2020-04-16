import json
import sys

from elasticsearch_connection import ElasticsearchConnection
import elasticsearch.helpers

INDEX_NAME = "beckett_jstor_ngrams_all"
DOC_TYPE = "article"


def get_scanner(elasticsearch_client):
    query = {
        'size': 25,
        'query': {
            'match_all': {}
        }
    }
    scanner = elasticsearch.helpers.scan(
        client=elasticsearch_client,
        scroll='2m',
        query=query,
        index=INDEX_NAME)
    return scanner



if __name__ == "__main__":
    ES_AUTH_USER = sys.argv[1]
    ES_AUTH_PASSWORD = sys.argv[2]
    ES_HOST = sys.argv[3]
    db_connection = ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    elasticsearch_client = db_connection.get_elasticsearch_client()
    scanner = get_scanner(elasticsearch_client)
    count = 0
    for i in scanner:
        print(i["_id"])
        count += 1
        if count == 27:
            break