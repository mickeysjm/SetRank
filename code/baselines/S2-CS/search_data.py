"""
__author__: Jiaming Shen
__description__: Search data with different similarity modules
"""

import argparse
import json
import sys
from elasticsearch import Elasticsearch


INDEX_NAME = None
TYPE_NAME = None
es = Elasticsearch()

def load_query():
  with open("../../../data/S2-CS/s2_query.json", "r") as fin:
    queries = []
    for line in fin:
      queryInfo = json.loads(line.strip())
      queries.append([queryInfo['qid'], queryInfo['query'], queryInfo["ana"]])
    return queries

def save_results(sim, results, mode):
  with open("./%s_%s.run" % (sim,mode), "w") as fout:
    for ele in results:
      fout.write("\t".join(ele)+"\n")


def search_data(query_string, entity_string, field_weights, topk, mode):
  '''
  :param query: (currently) a raw input string
  :param field_weights: a dict: {"title": 16, "abstract": 3, "keyphrase": 16}
  :param topk, top k number of results
  :return: a ranked results
  '''
  if(mode == 'word'):
	  search_body = {
	    "size": topk,
	    "query": {
	      "bool": {
		"should": [
		  {"match": {"title": {"query": query_string, "boost": field_weights["title"]}}},
		  {"match": {"abstract": {"query": query_string, "boost": field_weights["abstract"]}}},
		  {"match": {"keyphrase": {"query": query_string, "boost": field_weights["keyphrase"]}}},
		]
	      }
	    }
	  }
  elif(mode == 'entity'):
          search_body = {
            "size": topk,
            "query": {
              "bool": {
                "should": [
                        {"match": {"title_ana": {"query": entity_string, "boost": field_weights["title_ana"]}}},
                        {"match": {"abstract_ana": {"query": entity_string, "boost": field_weights["abstract_ana"]}}},
                        {"match": {"keyphrase_ana": {"query": entity_string, "boost": field_weights["keyphrase_ana"]}}},
                        {"match": {"bodytext_ana": {"query": entity_string, "boost": field_weights["bodytext_ana"]}}}
                ]
              }
            }
          }
  elif(mode == 'both'):
          search_body = {
            "size": topk,
            "query": {
              "bool": {
                "should": [
                  {"match": {"title": {"query": query_string, "boost": field_weights["title"]}}},
                  {"match": {"abstract": {"query": query_string, "boost": field_weights["abstract"]}}},
                  {"match": {"keyphrase": {"query": query_string, "boost": field_weights["keyphrase"]}}},
                        {"match": {"title_ana": {"query": entity_string, "boost": field_weights["title_ana"]}}},
                        {"match": {"abstract_ana": {"query": entity_string, "boost": field_weights["abstract_ana"]}}},
                        {"match": {"keyphrase_ana": {"query": entity_string, "boost": field_weights["keyphrase_ana"]}}},
                        {"match": {"bodytext_ana": {"query": entity_string, "boost": field_weights["bodytext_ana"]}}}
                ]
              }
            }
          }

  else:
          print('please enter a correct mode')
          sys.exit(0)

  res = es.search(index=INDEX_NAME, request_timeout=180, body=search_body)
  return res



if __name__ == "__main__":
  parser = argparse.ArgumentParser(prog='search_data.py', description='Search data using different similarities.')
  parser.add_argument('-sim', required=True, help='name of similarity module')
  parser.add_argument('-mode', required=True, help='mode of search')
  parser.add_argument('-field_weights', required=False, default="title:16,abstract:3,keyphrase:16,"
                              "title_ana:16,abstract_ana:3,keyphrase_ana:16,bodytext_ana:1",
                      help="Relative weights of each field")
  args = parser.parse_args()

  SIM_MODULE_NAME = args.sim  # one of ["tfidf", "bm25", "lm_dir", "lm_jm", "ib"]
  INDEX_NAME = "s2_" + SIM_MODULE_NAME
  TYPE_NAME = "s2_papers_" + SIM_MODULE_NAME

  queries = load_query()
  result_all = []
  field_weights = {ele.split(":")[0] : float(ele.split(":")[1]) for ele in args.field_weights.split(",")}

  for query in queries:
    query_id = query[0]
    query_string = query[1]
    query_entities_list = []
    for k, v in query[2].items():
      for i in range(v):
        query_entities_list.append(k)
    query_entities_string = " ".join(query_entities_list)

    print("Running query %s: %s, %s" % (query_id, query_string, query_entities_string))
    res = search_data(query_string=query_string, entity_string=query_entities_string,
                      field_weights=field_weights, topk=20, mode = args.mode)
    rank = 1
    for hit in res['hits']['hits']:
      result_all.append([query_id, "Q0", hit["_source"]["docno"], str(rank), str(hit["_score"]), SIM_MODULE_NAME])
      rank += 1

  save_results(SIM_MODULE_NAME, result_all, args.mode)


