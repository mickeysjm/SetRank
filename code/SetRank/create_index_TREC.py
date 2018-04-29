'''
__author__: Jiaming Shen
__description__: Create index with static mapping in ES 5.4.0 (a.k.a. define schema).
'''

from elasticsearch import Elasticsearch

if __name__ == '__main__':
    INDEX_NAME = "trec"
    TYPE_NAME = "trec_papers"
    NUMBER_SHARDS = 1 # keep this as one if no cluster
    NUMBER_REPLICAS = 0

    '''
    following is the defined schema
    pmid, date, author_list, journal_name, mesh, ( title, abstract, title_ana, abstract_ana) with their length, plus total length
    '''
    request_body = {
        "settings": {
            "number_of_shards": NUMBER_SHARDS,
            "number_of_replicas": NUMBER_REPLICAS
        },
        "mappings": {
            TYPE_NAME: {
                "properties": {
                    "pmid": {
                        "type": "keyword"
                    },
                    "date": {
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
                    "author_list": {
                        "type": "keyword"
                    },
                    "journal_name": {
                        "type": "keyword"
                    },
                    "title_ana": {
                        "type": "text",
                        "similarity": "BM25"
                    },
                    "abstract_ana": {
                        "type": "text",
                        "similarity": "BM25"
                    },
                    "mesh": {
                        "type": "text",
                        "similarity": "BM25"
                    },
                    "title_length": {
                        "type": "long"
                    },
                    "abstract_length": {
                        "type": "long"
                    },
                    "title_ana_length": {
                        "type": "long"
                    },
                    "abstract_ana_length": {
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