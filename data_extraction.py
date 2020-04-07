import sys

import elasticsearch
import elasticsearch_connection
import elasticsearch.helpers
import utils

def print_id(elasticsearch_client):
    results = elasticsearch.helpers.scan(elasticsearch_client, index="beckett_jstor_ngrams_part", doc_type="article",
                                         query={"query": {"match_all": {}}}, scroll='5m', raise_on_error=True,
                                         preserve_order=False,
                                         size=25,
                                         request_timeout=None, clear_scroll=True)
    output = []
    for i in range(12312//25):
        temp = []
        for j in range(25):
            temp.append(results.next()['_id'])
        output.append(temp)

    print "Completed"
    utils.json_file_writer("Data", "ids.json", output)


if __name__ == "__main__":
    ES_AUTH_USER = sys.argv[1]
    ES_AUTH_PASSWORD = sys.argv[2]
    ES_HOST = sys.argv[3]
    db_connection = elasticsearch_connection.ElasticsearchConnection(ES_HOST, ES_AUTH_USER, ES_AUTH_PASSWORD)
    elasticsearch_client = db_connection.get_elasticsearch_client()
    print_id(elasticsearch_client)
