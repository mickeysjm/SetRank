'''
__author__: Jiaming Shen
__description__: Create index with static mapping in ES 5.4.0 (a.k.a. define schema).
'''
from elasticsearch import Elasticsearch
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='create_index.py', description='create index with different similarities.')
    parser.add_argument('-sim', required=True, help='name of similarity module')
    args = parser.parse_args()

    NUMBER_SHARDS = 1 # keep this as one if no cluster
    NUMBER_REPLICAS = 0
    SIM_MODULE_NAME = args.sim # one of ["tfidf", "bm25", "lm_dir", "lm_jm", "ib"]
    INDEX_NAME = "s2_"+SIM_MODULE_NAME
    TYPE_NAME = "s2_papers_"+SIM_MODULE_NAME

    '''
    following is the defined schema
    total: 11 + 7 + 1 = 19 fields
    '''
    request_body = {
        "settings": {
            "number_of_shards": NUMBER_SHARDS,
            "number_of_replicas": NUMBER_REPLICAS,
            "index": {
                "similarity": {
                    "tfidf": {
                        "type": "classic"
                    },
                    "bm25": {
                        "type": "BM25"
                    },
                    "lm_dir": {
                        "type": "LMDirichlet",
                        "mu": 2000
                    },
                    "lm_jm": {
                        "type": "LMJelinekMercer",
                        "lambda": 0.1
                    },
                    "ib": {
                        "type": "IB",
                        "distribution": "ll",
                        "lambda": "df",
                        "normalization": "z"
                    }
                }
            }
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
                        "similarity": SIM_MODULE_NAME
                    },
                    "abstract": {
                        "type": "text",
                        "similarity": SIM_MODULE_NAME
                    },
                    "keyphrase": {
                        "type": "text",
                        "similarity": SIM_MODULE_NAME
                    },
                    "title_ana": {
                        "type": "text",
                        "analyzer": "whitespace",
                        "similarity": SIM_MODULE_NAME
                    },
                    "abstract_ana": {
                        "type": "text",
                        "analyzer": "whitespace",
                        "similarity": SIM_MODULE_NAME
                    },
                    "bodytext_ana": {
                        "type": "text",
                        "analyzer": "whitespace",
                        "similarity": SIM_MODULE_NAME
                    },
                    "keyphrase_ana": {
                        "type": "text",
                        "analyzer": "whitespace",
                        "similarity": SIM_MODULE_NAME
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
