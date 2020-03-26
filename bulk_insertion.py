import elasticsearch
import elasticsearch.helpers


if ES_AUTH_USER:
    print "Using authentication for " + ES_AUTH_USER
    es = elasticsearch.Elasticsearch([ES_HOST], http_auth=ES_AUTH_USER+":"+ES_AUTH_PASSWORD, connection_class=elasticsearch.RequestsHttpConnection) if ES_HOST else elasticsearch.Elasticsearch()
else:
    es = elasticsearch.Elasticsearch([ES_HOST]) if ES_HOST else elasticsearch.Elasticsearch()


import json
if ES_CREATE_INDEX:
    with open(MAPPINGS_JSON, 'r') as fh:
        mappings = json.load(fh).values()[0]
    es.indices.create(index=ES_INDEX_NAME, body=mappings)



action_generator = generate_actions(DATASET_DIR, index=ES_INDEX_NAME, document_type=ES_DOCUMENT_TYPE)

# elasticsearch.helpers.bulk(es, action_generator, chunk_size=100)
#elasticsearch.helpers.bulk(es, action_generator, timeout=ES_TIMEOUT)
elasticsearch.helpers.bulk(es, action_generator, chunk_size=100, request_timeout=ES_TIMEOUT)


# #### Test
#
# The following command just gets the count of documents.

# In[ ]:


es.count(index=ES_INDEX_NAME, doc_type=ES_DOCUMENT_TYPE)['count']