'''
__author__: Jiaming Shen
__description__: Create index with static mapping in ES 5.4.0 (a.k.a. define schema).
'''
from elasticsearch import Elasticsearch

if __name__ == '__main__':
    INDEX_NAME = "s2"
    TYPE_NAME = "s2_papers"
    NUMBER_SHARDS = 1 # keep this as one if no cluster
    NUMBER_REPLICAS = 0

    '''
    following is the defined schema
    total: 11 + 7 + 1 = 19 fields
    '''
    request_body = {
        "settings": {
            "number_of_shards": NUMBER_SHARDS,
            "number_of_replicas": NUMBER_REPLICAS
        },
        "mappings": {
            TYPE_NAME: {
                "properties": {
                    "docno": { # a single atomic string id, cannot be empty
                        "type": "keyword"
                    },
                    "venue": {
                        "type": "keyword"
                    },
                    "numCitedBy": {
                        "type": "long"
                    },
                    "numKeyCitations": {
                        "type": "long"
                    },
                    "title": {
                        "type": "text",
                        "similarity": "BM25"
                    },
                    "abstract": {
                        "type": "text",
                        "similarity": "BM25"
                    },
                    "keyphrase": {
                        "type": "text",
                        "similarity": "BM25"
                    },
                    "title_ana": {
                        "type": "text", "analyzer": "whitespace",
                        "similarity": "BM25"
                    },
                    "abstract_ana": {
                        "type": "text", "analyzer": "whitespace",
                        "similarity": "BM25"
                    },
                    "bodytext_ana": {
                        "type": "text", "analyzer": "whitespace",
                        "similarity": "BM25"
                    },
                    "keyphrase_ana": {
                        "type": "text", "analyzer": "whitespace",
                        "similarity": "BM25"
                    },
                    ## following are the length of each field, used in advanced scripting
                    "title_length": {
                        "type": "long"
                    },
                    "abstract_length": {
                        "type": "long"
                    },
                    "keyphrase_length": {
                        "type": "long"
                    },
                    "title_ana_length": {
                        "type": "long"
                    },
                    "abstract_ana_length": {
                        "type": "long"
                    },
                    "bodytext_ana_length": {
                        "type": "long"
                    },
                    "keyphrase_ana_length": {
                        "type": "long"
                    },
                    "total_length": {
                        "type": "long"
                    }
                }
            }
        }
    }

    es = Elasticsearch()
    if es.indices.exists(INDEX_NAME):
        res = es.indices.delete(index = INDEX_NAME)
        print("Deleting index %s , Response: %s" % (INDEX_NAME, res))
    res = es.indices.create(index = INDEX_NAME, body = request_body)
    print("Create index %s , Response: %s" % (INDEX_NAME, res))