import elasticsearch


class ElasticsearchConnection:
    def __init__(self, es_host='http://diging-elastic.asu.edu/elastic',
                 es_auth_user='ketan', es_auth_password='hk7PDr0I4toBA%e'):
        self.es_host = es_host
        self.es_auth_user = es_auth_user
        self.es_auth_password = es_auth_password

    def get_elasticsearch_client(self):
        try:
            elasticsearch_client = elasticsearch.Elasticsearch([self.es_host], http_auth=self.es_auth_user
                                                                                         + ":" + self.es_auth_password,
                                                               connection_class=elasticsearch.RequestsHttpConnection)

            return elasticsearch_client
        except Exception as ex:
            print("Error:", ex)
            return None


if __name__ == "__main__":
    ES_HOST = 'localhost:9200'
    db_connection = ElasticsearchConnection(ES_HOST)
    elasticsearch_client = db_connection.get_elasticsearch_client
    INDEX_NAME = "ramuji"
    DOC_TYPE = "_doc"
    # Getting Mapping of the index
    res = elasticsearch_client.indices.get_mapping(INDEX_NAME)
