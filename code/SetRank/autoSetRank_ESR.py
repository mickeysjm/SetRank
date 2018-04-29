import argparse
import sys
from collections import Counter
from collections import defaultdict
import itertools
import time
import numpy as np
import math
import pickle
from scipy import stats

import setRank_ESR

def string2dict(s):
  d = {ele.split(":")[0]: float(ele.split(":")[1]) for ele in s.split(",")}
  return d

def dict2string(d):
  s = ",".join(str(k)+":"+str(d[k]) for k in d)
  return s

def multiSetRank(query_words_string, query_entities_string, kb, params_set, DEBUG=False):
  bulk = []
  for params in params_set:
    retrieval_query = setRank_ESR.generate_retrieval_query(
      query_string=query_words_string, entity_string=query_entities_string, field_weights=params, DEBUG=DEBUG
    )
    rescore_query = setRank_ESR.generate_rescore_query(
      query_string=query_words_string, entity_string=query_entities_string, kb=kb, params=params, DEBUG=DEBUG
    )
    search_body = {
      "size": 20,
      "_source": ["docno"],
      "query": retrieval_query,
      "rescore": {
        "window_size": 1000,
        "query": {
          "rescore_query": rescore_query,
          "query_weight": 0,
          "rescore_query_weight": 1
        }
      }
    }
    op_dict = {"index": setRank_ESR.FLAGS_INDEX_NAME, "type": setRank_ESR.FLAGS_TYPE_NAME}
    bulk.append(op_dict)
    bulk.append(search_body)

  start = time.time()
  resp = setRank_ESR.es.msearch(body=bulk, request_timeout=600)["responses"]
  end = time.time()
  print("Finish retrieve %s pre-rankers' results using %s seconds" % (len(bulk)/2, (end-start)))

  rankings = []
  for res in resp:
    ranking = [hit["_source"]["docno"] for hit in res["hits"]["hits"]]
    rankings.append(ranking)

  return rankings

def rankAggregate(doc_rankings, maxIters=10, distanceMetric='KT', checkConverge=False, DEBUG=False):
  ## Step 1: Construct the document pool
  docCounter = sorted(Counter(itertools.chain(*doc_rankings)).items(), key=lambda x: -x[1])
  docno2docid = {} # docid aligns with the frequency of docno in all ranking list
  docid2docno = {}
  for idx, ele in enumerate(docCounter):  # notice: a small docid indicates most frequent documents
    docno2docid[ele[0]] = idx
    docid2docno[idx] = ele[0]
  rankings = []
  docid2positions = defaultdict(list) # docid -> [(position in rank list, len of rank list)]
  for i, doc_ranking in enumerate(doc_rankings):
    ranking = []
    k = len(doc_ranking) # current rank list i is of length k
    for j, docno in enumerate(doc_ranking):
      docid = docno2docid[docno]
      ranking.append(docid)
      docid2positions[docid].append((i, j, k)) # current document is at position j of rank list i which is of size k
    rankings.append(ranking)

  p = len(doc_rankings)
  K = len(docno2docid)

  if DEBUG:
    print("Number of ranker p = %s" % p)
    print("Size of document pool K = %s" % K)
    for _, r in enumerate(rankings):
      print("Ranking list %s : \n \t\t%s" % (_, r))
    for j in sorted(docid2positions.keys()):
      print(j, docid2positions[j])
    for docid in docid2docno:
      print(docid, "=>", docid2docno[docid])

  ## Step 2: Iteratively apply weighted rank aggregation
  alphas = np.ones(p) / p
  prev_aggregated_rank = None
  convergedFlag = False
  for iter in range(maxIters):
    ## weighted Borda Counting
    docid2scores = defaultdict(float)
    for docid in docid2positions:
      score = 0.0
      for pos in docid2positions[docid]:
        score += (alphas[pos[0]] * (pos[2]-pos[1]))
      docid2scores[docid] = score

    aggregated_rank = [ele[0] for ele in sorted(docid2scores.items(), key = lambda x:-x[1])]
    docid2rank = {docid:r for r, docid in enumerate(aggregated_rank)}
    if DEBUG:
      print("Iteration: %s, aggregated list: %s" % (iter, aggregated_rank))
      print("Iteration: %s, docid2rank: %s" % (iter, docid2rank))
    if aggregated_rank == prev_aggregated_rank:
      print("Converged at iteration %s" % iter)
      convergedFlag = True
      break
    else:
      if DEBUG and prev_aggregated_rank:
        # print("alpha:", alphas)
        differences = [] # (docno, prev_rank, current_rank)
        for i in range(len(prev_aggregated_rank)):
          if docid2rank[prev_aggregated_rank[i]] != i:
            differences.append((docid2docno[prev_aggregated_rank[i]], i, docid2rank[prev_aggregated_rank[i]]))
        # for ele in differences:
        #   print("Position changed doc:", ele)
      prev_aggregated_rank = aggregated_rank

    ## confidence score alignment
    positions2discouts = {}
    consider_not_appeared_docs = False
    rank_inversion_cnt = 0
    for r_id, r in enumerate(rankings):
      k = len(r)
      distance = 0.0
      ## Include influence of those not appeared documents
      if consider_not_appeared_docs:
        not_appeared_docs = set(docid2rank.keys()) - set(r)  # set of docids that are not appeared in current rank list
      for a in range(k-1):
        for b in range(a+1,k) :
          pi_a = docid2rank[r[a]]
          pi_b = docid2rank[r[b]]
          if pi_a > pi_b: # a position inversion
            if distanceMetric == "dKT": # discounted KT distance
              if (pi_a, pi_b) in positions2discouts:
                discount = positions2discouts[(pi_a, pi_b)]
              else:
                # change zero-index to one-index
                discount = (1.0 / math.log(1+pi_b+1, 2)) - (1.0 / math.log(1+pi_a+1, 2))
                positions2discouts[(pi_a, pi_b)] = discount
              distance += (discount * 1.0)
              rank_inversion_cnt += 1
            elif distanceMetric == 'KT': # normal KT distance
              distance += 1.0
              rank_inversion_cnt += 1
            else:
              print("[ERROR] Unsupported distanceMetric: %s" % distanceMetric)
        if consider_not_appeared_docs:
          for not_appeared_doc in not_appeared_docs:
            pi_appear = docid2rank[r[a]]
            pi_not_appeared_doc = docid2rank[not_appeared_doc]
            if pi_not_appeared_doc > pi_appear:
              if distanceMetric == "dKT":  # discounted KT distance
                if (pi_not_appeared_doc, pi_appear) in positions2discouts:
                  discount = positions2discouts[(pi_not_appeared_doc, pi_appear)]
                else:
                  # change zero-index to one-index
                  discount = (1.0 / math.log(1 + pi_appear + 1, 2)) - (1.0 / math.log(1 + pi_not_appeared_doc + 1, 2))
                  positions2discouts[(pi_not_appeared_doc, pi_appear)] = discount
                distance += (discount * 1.0)
                rank_inversion_cnt += 1
              elif distanceMetric == 'KT':  # normal KT distance
                distance += 1.0
                rank_inversion_cnt += 1
              else:
                print("[ERROR] Unsupported distanceMetric: %s" % distanceMetric)

      alphas[r_id] = math.exp(-1.0 * distance)

    Z = sum(alphas)
    alphas = alphas / Z
    uniform_dist = np.ones(p) / p
    kl = stats.entropy(pk=alphas, qk=uniform_dist)
    print("Iteration: %s, confidence scores normalizer = %s" % (iter, Z))
    print("Iteration: %s, kl to uniform = %s" % (iter, kl))
    print("Iteration: %s, total rank inversion = %s" % (iter, rank_inversion_cnt))
    print("Iteration: %s, confidence scores: %s" % (iter, alphas))

  if not convergedFlag:
    print("Not converged after %s iterations" % maxIters)
  aggregated_rank_docno = [docid2docno[docid] for docid in aggregated_rank]
  return (alphas, aggregated_rank_docno)

def rankAggregateCorpus(corpus_doc_rankings, maxIters=10, distanceMetric="KT", checkVonverge=False, DEBUG=False):
  p = len(corpus_doc_rankings[0]) # number of distinct rankers
  if DEBUG:
    print("Number of ranker p = %s" % p)

  alphas = np.ones(p) / p
  for iter in range(maxIters):
    if DEBUG:
      print("Iteration: %s" % iter)
      print("Alphas: %s" % alphas)

    alpha_distances = np.zeros(p)
    ## go through the query set
    for qid, doc_rankings in enumerate(corpus_doc_rankings):
      ## obtain the docid
      docCounter = sorted(Counter(itertools.chain(*doc_rankings)).items(), key=lambda x: -x[1])
      docno2docid = {}
      docid2docno = {}
      for idx, ele in enumerate(docCounter):  # notice: a small docid indicates most frequent documents
        docno2docid[ele[0]] = idx
        docid2docno[idx] = ele[0]
      rankings = []
      docid2positions = defaultdict(list)  # docid -> [(position in rank list, len of rank list)]
      for i, doc_ranking in enumerate(doc_rankings):
        ranking = []
        k = len(doc_ranking)  # current rank list i is of length k
        for j, docno in enumerate(doc_ranking):
          docid = docno2docid[docno]
          ranking.append(docid)
          docid2positions[docid].append((i, j, k)) # current document is at position j of rank list i which is of size k
        rankings.append(ranking)

      ## weighted Borda Counting
      docid2scores = defaultdict(float)
      for docid in docid2positions:
        score = 0.0
        for pos in docid2positions[docid]:
          score += (alphas[pos[0]] * (pos[2] - pos[1]))
        docid2scores[docid] = score

      aggregated_rank = [ele[0] for ele in sorted(docid2scores.items(), key=lambda x: -x[1])]
      docid2rank = {docid: r for r, docid in enumerate(aggregated_rank)}

      ## accumlate each parameter's dKT
      positions2discouts = {}
      query_distance_sum = 0
      for r_id, r in enumerate(rankings): # r_id is the index of its corresponding parameter
        k = len(r)
        distance = 0.0
        for a in range(k - 1):
          for b in range(a + 1, k):
            pi_a = docid2rank[r[a]]
            pi_b = docid2rank[r[b]]
            if pi_a > pi_b:  # a position inversion
              if distanceMetric == "dKT":  # discounted KT distance
                if (pi_a, pi_b) in positions2discouts:
                  discount = positions2discouts[(pi_a, pi_b)]
                else:
                  # change zero-index to one-index
                  discount = (1.0 / math.log(1 + pi_b + 1, 2)) - (1.0 / math.log(1 + pi_a + 1, 2))
                  positions2discouts[(pi_a, pi_b)] = discount
                distance += (discount * 1.0)
                query_distance_sum += (discount * 1.0)
              elif distanceMetric == 'KT':  # normal KT distance
                distance += 1.0
                query_distance_sum += 1.0
              else:
                print("[ERROR] Unsupported distanceMetric: %s" % distanceMetric)
        # accumlate the distance
        alpha_distances[r_id] += distance
      if DEBUG:
        print("query_distance_sum for query %s = %s" % (qid,query_distance_sum))

    if DEBUG:
      Z_distance = sum(alpha_distances)
      print("Sum of distances at iteration %s = %s" % (iter, Z_distance))
      print("Distances at iteration %s = %s" % (iter, alpha_distances))

    ## Adjust confidence score
    # alpha_distances = np.exp(-1.0 * alpha_distances)
    alpha_distances = 1.0 / alpha_distances
    Z = sum(alpha_distances)
    alphas = alpha_distances / Z

  return alphas

def main(args):
  queries = setRank_ESR.load_query(args)
  kb = setRank_ESR.load_kb(args)
  result_all = []

  ## Step 1: determine the anchor parameter and the parameters that we want to tune
  anchor_params = {
    'title': 20.0, 'abstract': 5.0, 'keyphrase': 16.0,
    'title_ana': 20.0, 'abstract_ana': 5.0, 'keyphrase_ana': 16.0, 'bodytext_ana': 1.0,
    'title_mu': 1000.0, 'abstract_mu': 1100.0, 'keyphrase_mu': 1000.0,
    'title_ana_mu': 1000.0, 'abstract_ana_mu': 1000.0, 'keyphrase_ana_mu': 1000.0, 'bodytext_ana_mu': 1000.0,
    "entity_lambda": 0.5, "type_interaction": 1.0,
    "consider_entity_set": 1.0, "consider_word_set" : 1.0, "consider_type":1.0, "word_dependency":1.0
  }
  params_names = ["title", "abstract", "title_mu", "abstract_mu", "entity_lambda"]

  ## Step 2: fix the parameter set that we want to tune, based on the mode
  if args.mode == "tune" or args.mode == "tune-best-rank":
    params_values = [
      [5.0, 10.0, 15.0, 20.0], # -> title
      [1.0, 3.0, 5.0, 7.0], # -> abstract
      [500, 1000, 1500, 2000], # -> title_mu
      [500, 1000, 1500, 2000], # -> abstract_mu
      [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8], # -> entity_lambda
    ]
    all_combinations = list(itertools.product(*params_values))
    params_set = []
    for ele in all_combinations:
      tmp_params = anchor_params.copy()
      for param_index, param_value in enumerate(ele):
        tmp_params[params_names[param_index]] = param_value
      params_set.append(tmp_params)
  elif args.mode == "rank":
    params_values = [
      [15.0, 3.0, 15.0, 0.3, 1.0],
      [15.0, 3.0, 15.0, 0.3, 0.5],
      [15.0, 3.0, 15.0, 0.3, 1.5],
      [20.0, 3.0, 15.0, 0.3, 1.5],
      [15.0, 3.0, 15.0, 0.4, 0.5]
    ]
    params_set = []
    for ele in params_values:
      print("ele:", ele)
      tmp_params = anchor_params.copy()
      for param_index, param_value in enumerate(ele):
        print("param_index", param_index, "param_value", param_value)
        tmp_params[params_names[param_index]] = param_value
      params_set.append(tmp_params)
  else:
    print("Unsupported mode: %s" % args.mode)
    return

  ## Step 3: auto model selection over either query or corpus level
  if args.agglevel == "query":
    saved_result = (int(args.load_pre_saved_rankings) == 1) ## load results from query
    if saved_result:
      print("=== Loading pre-saved ranking results ===")
      with open(args.pre_saved_rankings, "rb") as fin:
        all_docno_rankings = pickle.load(fin) # a list of docno_rankings
    else:
      print("=== Cannot load pre-saved ranking results, generate rankings from scratch ===")
      all_docno_rankings = {} # query_id -> docno_rankings

    confidence_over_all_queries = np.zeros(len(params_set))
    for query in queries:
      query_id = query[0]
      query_string = query[1]
      query_entities_list = []
      for k, v in query[2].items():
        for i in range(v):
          query_entities_list.append(k)
      query_entities_string = " ".join(query_entities_list)

      print("=== Running query: %s (id = %s) ===" % (query_string, query_id))
      if saved_result:
        rankings = all_docno_rankings[query_id]
      else:
        rankings = multiSetRank(query_string, query_entities_string, kb, params_set, DEBUG=False)
        all_docno_rankings[query_id] = rankings
      (confidences, aggregated_rank) = rankAggregate(rankings, DEBUG=True)
      confidence_over_all_queries += confidences

      if args.mode == "tune-best-rank": # use the best parameter to rank this query again
        best_parameter = params_set[np.argmax(confidences)]
        print("Best parameters for query %s: %s" % (query_id, best_parameter))
        res = setRank_ESR.setRank(query_string, query_entities_string, kb, best_parameter)
        rank = 1
        for hit in res['hits']['hits']:
          result_all.append([query_id, "Q0", hit["_source"]["docno"], str(rank), str(hit["_score"]), "autoSetRank"])
          rank += 1
      else:
        rank = 1
        for docno in aggregated_rank:
          result_all.append([query_id, "Q0", docno, str(rank), str(100-rank), "autoSetRank"])
          rank += 1

    ## save results
    if not saved_result:
      with open(args.pre_saved_rankings, "wb") as fout:
        print("=== Save rankings for next time's usage ===")
        pickle.dump(all_docno_rankings, fout, protocol=pickle.HIGHEST_PROTOCOL)

  elif args.agglevel == "corpus": ## corpus level aggregation
    load_data_from_pickle = False
    if load_data_from_pickle:
      with open("all_rankings.pickle", "rb") as fin:
        all_docno_rankings = pickle.load(fin)
    else:
      ## step 1: obtain all query
      all_docno_rankings = []
      for query in queries:
        query_id = query[0]
        query_string = query[1]
        query_entities_list = []
        for k, v in query[2].items():
          for i in range(v):
            query_entities_list.append(k)
        query_entities_string = " ".join(query_entities_list)

        print("=== Running query %s (id = %s) ===" % (query_string, query_id))
        rankings = multiSetRank(query_string, query_entities_string, kb, params_set, DEBUG=False)
        all_docno_rankings.append(rankings)

      with open(args.pre_saved_rankings, "wb") as fout:
        pickle.dump(all_docno_rankings, fout, protocol=pickle.HIGHEST_PROTOCOL)

    ## step 2: rank aggregation
    confidence_over_all_queries = rankAggregateCorpus(all_docno_rankings, DEBUG=True)
  else:
    print("[ERROR] Unsupported agglevel configuration: %s" % args.agglevel)
    return

  params2confidence = [(params, confidence_over_all_queries[i]) for i, params in enumerate(params_set)]
  for ele in sorted(params2confidence, key = lambda x:-x[1])[0:10]:
    print("Confidence = %s, parameters = %s" % (ele[1], ele[0]))

  if args.mode == "query": # save results only for query level aggregation
    setRank_ESR.save_results(args, result_all)
    print("Finish saving results to path: %s" % args.output)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(prog='autoSetRank_ESR.py',
                                   description='Use rank aggregation to automatically learn parameters in setRank'
                                               'algorithm on S2-CS dataset.')
  parser.add_argument('-query', required=False, default="../../data/S2-CS/s2_query.json",
                      help='File name of test queries.')
  parser.add_argument('-output', required=False, default="../results/s2/auto-tune.run",
                      help='File name of output results.')
  parser.add_argument('-kb', required=False, default="../../data/S2-CS/s2_entity_type.tsv")
  parser.add_argument('-mode', required=False, default="tune",
                      help="mode can be 'tune', 'rank', 'tune-best-rank'."
                           "tune: aggregate over all candidate parameters and save the aggregated rank list "
                           "      based on all the candidate rank lists,"
                           "rank: use topK (selected) parameters to obtain pre-ranked list and aggregate them"
                           "tune-best-rank: first tune the query level best parameters and return the rank list for that"
                           "                query using the parameter suits it best, only works for aggLevel=query")
  parser.add_argument('-agglevel', required=False, default="query",
                      help="agglevel can be 'query' or 'corpus', and it represents the level of rank aggregation")
  parser.add_argument('-pre_saved_rankings', required=False, default="",
                      help="name of (previously saved OR about to be saved) ranking results")
  parser.add_argument('-load_pre_saved_rankings', required=False, default="0",
                      help="set load_pre_saved_rankings to True if using presaved rankings")
  args = parser.parse_args()
  sys.exit(main(args))

