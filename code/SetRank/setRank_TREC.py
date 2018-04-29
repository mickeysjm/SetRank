'''
__author__: Jiaming Shen
__description__: Implement unsupervised SetRank algorithm in ES 5.4.0.
'''
import argparse
import json
import sys
from elasticsearch import Elasticsearch
from collections import Counter
from textblob import TextBlob

es = Elasticsearch()

FLAGS_INDEX_NAME = 'trec'
FLAGS_TYPE_NAME = 'trec_papers'
FLAGS_REQUEST_TIMEOUT = 180  # Timeout limit in seconds
FLAGS_TOPK = 20  # The final number of documents returned
FLAGS_RESCORE_WINDOW_SIZE = 1000  # The window size of rescoring results.

### Following are model selection parameters
FLAGS_QUERY_WEIGHT = 0  # The weight of retrieval query. Set 0 if you want to use our own model
FLAGS_RESCORE_WEIGHT = 1  # The weight of rescore query. Set 1 if you want to use our own model.


def load_query(args):
  with open(args.query, "r") as fin:
    queries = []
    for line in fin:
      queryInfo = json.loads(line.strip())
      queries.append([queryInfo['qid'], queryInfo['query'].lower(), queryInfo["ana"]])
    return queries


def load_kb(args):
  if not args.kb:
    return {}
  else:
    eid2types = {}  # eid -> a single type path
    with open(args.kb, "r") as fin:
      for line in fin:
        line = line.strip().split("\t")
        if line:
          if len(line) != 2:
            print("ERROR:", line)
          else:
            entity = line[0].replace(" ","_").lower()
            type = line[1]
            eid2types[entity] = type
    return eid2types

def save_results(args, results):
  with open(args.output, "w") as fout:
    for ele in results:
      fout.write("\t".join(ele) + "\n")

def type_dist(eid1, eid2, kb, params):
  ''' Obtain the interaction strength between entity 1 and entity 2 based on their distance on type hierarchy

  :param eid1:
  :param eid2:
  :return:
  '''
  eid1_type = kb[eid1]
  eid2_type = kb[eid2]
  if eid1_type != eid2_type:
    LCA_dist = 2
  else:
    LCA_dist = 1
  return LCA_dist

def generate_retrieval_query(query_string, entity_string, field_weights, DEBUG=False):
  ''' Generate the retrieval query which is used to pre-rank the documents.

  :param query_string: a string of unigram tokens, e.g., "convolutional neural network time series"
  :param entity_string: a string of entity id tokens, e.g., "/m/024hw2 /m/0b6xt /m/05dhw"
  :param field_weights: a dict of each field's strength
  :param DEBUG: debug flag
  :return: a retrieval query
  '''
  blob = TextBlob(query_string)
  query_string = " ".join([str(word) for word in blob.words])
  retrieval_query = {
    "bool": {
      "should": [
        {"match": {"title": {"query": query_string, "boost": field_weights["title"]}}},
        {"match": {"abstract": {"query": query_string, "boost": field_weights["abstract"]}}},
        {"match": {"title_ana": {"query": entity_string, "boost": field_weights["title_ana"]}}},
        {"match": {"abstract_ana": {"query": entity_string, "boost": field_weights["abstract_ana"]}}}
      ]
    }
  }
  if DEBUG:
    print("Retreival query:", retrieval_query)
  return retrieval_query


def generate_rescore_query(query_string, entity_string, kb, params, DEBUG=False):
  ## Processing entities
  c = Counter(entity_string.split())
  eids = []
  eid_counts = []
  for ele in c.items():
    eids.append(ele[0])
    eid_counts.append(ele[1])

  ## Processing words
  blob = TextBlob(query_string)
  query_string = " ".join([str(word) for word in blob.words])
  c = Counter(query_string.split())
  words = []
  word_counts = []
  for ele in c.items():
    words.append(ele[0])
    word_counts.append(ele[1])

  ## obtain relative entity field weights
  entity_fields_weights_sum = params["title_ana"] + params["abstract_ana"]
  title_ana_relative_weight = 1.0 * params["title_ana"] / entity_fields_weights_sum
  abstract_ana_relative_weight = 1.0 * params["abstract_ana"] / entity_fields_weights_sum
  entity_field_relative_weights = [title_ana_relative_weight, abstract_ana_relative_weight]

  ## obtain relative word field weights
  word_fields_weights_sum = params["title"] + params["abstract"]
  title_relative_weight = 1.0 * params["title"] / word_fields_weights_sum
  abstract_relative_weight = 1.0 * params["abstract"] / word_fields_weights_sum
  word_field_relative_weights = [title_relative_weight, abstract_relative_weight]

  ## obtain entity interaction (based on type hierarchy) strength
  eid_interactions = []
  for i, eid1 in enumerate(eids):
    eid_interaction = []
    for j, eid2 in enumerate(eids):
      if j == i:  # diagonal is zero
        eid_interaction.append(0.0)
      else:
        if params["consider_type"]:
          eid_interaction.append(type_dist(eid1, eid2, kb, params))
        else:
          eid_interaction.append(1.0)

    eid_interactions.append(eid_interaction)

  ## obtain word interaction (based on word similarity) strength or just all zeros
  word_interactions = []
  for i, word1 in enumerate(words):
    word_interaction = []
    for j, word2 in enumerate(words):
      if j == i:  # diagonal is zero
        word_interaction.append(0)
      else:
        word_interaction.append(1)
    word_interactions.append(word_interaction)

  if DEBUG:
    print("=== Entity Information ===")
    print("eids: ", eids)
    print("eid_counts: ", eid_counts)
    print("eid_interactions: ", eid_interactions)
    print("entity_field_relative_weights: ", entity_field_relative_weights)
    print("=== Word Information ===")
    print("words: ", words)
    print("word_counts: ", word_counts)
    print("word_interactions: ", word_interactions)
    print("word_field_relative_weights: ", word_field_relative_weights)

  params = {
    "entities": eids,
    "entity_query_counts": eid_counts,
    "entity_interactions": eid_interactions,
    "entity_fields": ["title_ana", "abstract_ana"],
    "entity_field_relative_weights": entity_field_relative_weights,
    "entity_field_mus": [params["title_ana_mu"], params["abstract_ana_mu"]],
    "entity_field_length_sums": [6163685.0, 43615331.0],
    "consider_entity_set":params['consider_entity_set'],

    "words": words,
    "word_query_counts": word_counts,
    "word_interactions": word_interactions,
    "word_fields": ["title", "abstract"],
    "word_field_relative_weights": word_field_relative_weights,
    "word_field_mus": [params["title_mu"], params["abstract_mu"]],
    "word_field_length_sums": [54350257.0, 650533454.0],
    "consider_word_set": params['consider_word_set'],

    # this should be a number in [0, 1]
    "entity_lambda": params["entity_lambda"]
  }

  rescore_query = {
    "function_score": {
      "script_score": {
        "script": {
          "lang": "groovy",  ## need to explicitly state the usage of groovy
          "params": params,
          "inline": """
            double total_score = 0.0;

            /* Entity space score */
            eid_base_scores = []; // used to cache base score for each entity
            eid_exist_flags = []; // used to cache whether an entity exists

            // for each entity, calculate base score
            for (int i = 0; i < entities.size(); ++i) {;
              String eid = entities[i];
              double cur_score = 0.0;
              eid_exist_flag = 0;

              // for each field 
              for (int k = 0; k < entity_fields.size(); ++k) {;
                String field = entity_fields[k];
                field_length = field + "_length";
                field_length_sum = entity_field_length_sums[k];
                field_mu = entity_field_mus[k];

                tf_d = _index[field][eid].tf();
                if (tf_d > 0) {;
                  eid_exist_flag = 1;
                };
                tf_D = _index[field][eid].ttf();
                L_d = doc[field_length].value;
                L_D = field_length_sum;

                field_weight = entity_field_relative_weights[k];
                cur_score = cur_score + field_weight * (tf_d + field_mu*(tf_D / L_D)) / (L_d + field_mu);  
              };

              // smoothing
              cur_score = cur_score ** 0.5;
              
              eid_base_scores += cur_score; 
              eid_exist_flags += eid_exist_flag; 
            };

            // for each entity, calculate final score and add to total_score
            for (int i = 0; i < entities.size(); ++i) {;
              entity_weights = entity_query_counts[i]; // entity frequency in query
              if (consider_entity_set > 0) {;
                for (int j = 0; j < entities.size(); ++j) {;
                  if (eid_exist_flags[j] > 0) {;
                    entity_weights = entity_weights + entity_interactions[i][j] * eid_base_scores[j] * entity_query_counts[j];
                  };
                };
              };
              entity_base_score = eid_base_scores[i];

              total_score = total_score + entity_lambda * entity_weights * entity_base_score;
            };


            /* Word space score */

            word_base_scores = []; // used to cache base score for each word
            word_exist_flags = []; // used to cache whether an word exists


            // for each word, calculate its base score
            for (int i = 0; i < words.size(); ++i) {;
              String word = words[i];
              double cur_score = 0.0;
              word_exist_flag = 0;

              // for each word field 

              for (int k = 0; k < word_fields.size(); ++k) {;
                String field = word_fields[k];
                field_length = field + "_length";
                field_length_sum = word_field_length_sums[k];
                field_mu = word_field_mus[k];

                tf_d = _index[field][word].tf();
                if (tf_d > 0) {;
                  word_exist_flag = 1;
                };
                tf_D = _index[field][word].ttf();
                L_d = doc[field_length].value;
                L_D = field_length_sum;

                field_weight = word_field_relative_weights[k];
                cur_score = cur_score + field_weight * (tf_d + field_mu*(tf_D / L_D)) / (L_d + field_mu);

              };
              
              // smoothing
              cur_score = cur_score ** 0.5;

              word_base_scores += cur_score; 
              word_exist_flags += word_exist_flag; 
            };


            // for each word, calculate final score and add to total_score
            for (int i = 0; i < words.size(); ++i) {;
              word_weight = word_query_counts[i]; // word frequency in query
              if (consider_word_set > 0) {;
                for (int j = 0; j < words.size(); ++j) {;
                  if (word_exist_flags[j] > 0) {;
                    word_weight = word_weight + word_interactions[i][j] * word_base_scores[j] * word_query_counts[j];
                  };
                };
              };
              word_base_score = word_base_scores[i];

              total_score = total_score + (1.0 - entity_lambda) * word_weight * word_base_score;
            };

            return total_score;
          """
        }
      },
      "score_mode": "sum", # defines how the computed scores are combined if multiple functions present, not important in our case
      "boost_mode": "replace"  # defines how the original query score and newly computed function score are combined.
    }
  }
  return rescore_query


def entity_count(entity_string, entity_set):
  c = Counter(entity_string.split())
  tmp = []
  for entity in entity_set:
    tmp.append([entity, c[entity]])
  return ",".join([ele[0] + ":" + str(ele[1]) for ele in tmp])


def setRank(query_words_string, query_entities_string, kb, params, DEBUG=False):
  retrieval_query = generate_retrieval_query(query_string=query_words_string, entity_string=query_entities_string,
                                             field_weights=params, DEBUG=DEBUG)
  rescore_query = generate_rescore_query(query_string=query_words_string, entity_string=query_entities_string, kb=kb,
                                         params=params, DEBUG=DEBUG)

  search_body = {
    "size": FLAGS_TOPK,
    "query": retrieval_query,
    "rescore": {
      "window_size": FLAGS_RESCORE_WINDOW_SIZE,
      "query": {
        "rescore_query": rescore_query,
        "query_weight": FLAGS_QUERY_WEIGHT, # define how the scores of original retrieval query and rescore query are combined
        "rescore_query_weight": FLAGS_RESCORE_WEIGHT # define how the scores of original retrieval query and rescore query are combined}}}
      }
    }
  }

  res = es.search(index=FLAGS_INDEX_NAME, request_timeout=FLAGS_REQUEST_TIMEOUT, body=search_body)
  return res


def main(args):
  queries = load_query(args)
  kb = load_kb(args)
  result_all = []
  debug_flag = ( int(args.debug) == 1 )
  params = {ele.split(":")[0]: float(ele.split(":")[1]) for ele in args.params.split(",")}
  if debug_flag:
    print("=== Check KB ===")
    for k in kb:
      print("{}\t{}".format(k, kb[k]))

  for query in queries:
    query_id = query[0]
    query_string = query[1]
    query_entities_list = []
    for k, v in query[2].items():
      for i in range(v):
        query_entities_list.append(k)
    query_entities_string = " ".join(query_entities_list)

    # print("Runing query %s: %s" % (query_id, query_string))
    res = setRank(query_string, query_entities_string, kb, params, DEBUG=debug_flag)
    rank = 1
    for hit in res['hits']['hits']:
      result_all.append([query_id, "Q0", hit["_source"]["pmid"], str(rank), str(hit["_score"]), "setRank-TREC"])
      rank += 1

  save_results(args, result_all)


if __name__ == "__main__":
  # Example usage: python3 setRank_TREC.py -query ./XXX -output ./YYY.run
  parser = argparse.ArgumentParser(prog='setRank_TREC.py', description='Run setRank algorithm on TREC-BIO dataset.')
  parser.add_argument('-query', required=False, default='../../data/TREC_BIO/trec_query.json',
                      help='File name of test queries.')
  parser.add_argument('-output', required=False, default="../results/trec/setrank.run",
                      help='File name of output results.')
  parser.add_argument('-kb', required=False, default="../../data/TREC_BIO/trec_entity_type.tsv")
  parser.add_argument('-params', required=False, default="title:20.0,abstract:5.0,"
                                                         "title_ana:20.0,abstract_ana:5.0,"
                                                         "title_mu:1000.0,abstract_mu:1000.0,"
                                                         "title_ana_mu:1000.0,abstract_ana_mu:1000.0,"
                                                         "entity_lambda:0.5,type_interaction:1.0,"
                                                         "consider_entity_set:1.0,consider_word_set:1.0,"
                                                         "consider_type:1.0,word_dependency:1.0",
                      help="tunable parameters in our model")
  parser.add_argument('-debug', required=False, default=0, help="debug flag")
  args = parser.parse_args()
  print("=== Arguments ===")
  print("  Input Query: %s" % args.query)
  print("  Output Run: %s" % args.output)
  print("  Parameters: %s" % args.params)
  sys.exit(main(args))