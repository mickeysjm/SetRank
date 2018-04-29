import argparse
import os
import sys
from collections import defaultdict

def main(args):
  with open(args.trec_res, "r") as fin:
    qid2evals = defaultdict(lambda : defaultdict(float))
    for line in fin:
      segs = line.strip().split()
      if segs:
        eval_name = segs[0]
        query_id = segs[1]
        eval_res = float(segs[2])
        qid2evals[query_id][eval_name] = eval_res

  with open(args.res_out, "w") as fout:
    fout.write("\t".join(["qid","map@5","map@10","map@15","map@20","ndcg@5","ndcg@10","ndcg@15","ndcg@20",
                          "success@1","success@5","success@10"])+"\n")
    queries = [str(i) for i in range(1,101)]
    queries.append("all")
    for qid in queries:
      map_5 = qid2evals[qid]["map_cut_5"]
      map_10 = qid2evals[qid]["map_cut_10"]
      map_15 = qid2evals[qid]["map_cut_15"]
      map_20 = qid2evals[qid]["map_cut_20"]
      ndcg_5 = qid2evals[qid]["ndcg_cut_5"]
      ndcg_10 = qid2evals[qid]["ndcg_cut_10"]
      ndcg_15 = qid2evals[qid]["ndcg_cut_15"]
      ndcg_20 = qid2evals[qid]["ndcg_cut_20"]
      success_1 = qid2evals[qid]["success_1"]
      success_5 = qid2evals[qid]["success_5"]
      success_10 = qid2evals[qid]["success_10"]
      fout.write("\t".join([str(ele) for ele in [qid,map_5,map_10,map_15,map_20,ndcg_5,ndcg_10,ndcg_15,ndcg_20,
                                                 success_1, success_5, success_10]]))
      fout.write("\n")

if __name__ == "__main__":
  # Example usage: python3 parse_eval_res.py -trec_res ./XXX.eval -res_out ./YYY.txt
  parser = argparse.ArgumentParser(prog='parse_eval_res.py', description='Parse TREC Evaluation Result and save in a '
                                   "compact file for evaluation")
  parser.add_argument('-trec_res', required=True, help='File name of input TREC Evaluation Result file.')
  parser.add_argument('-res_out', required=True, help='File name of output compact result file.')
  args = parser.parse_args()
  sys.exit(main(args))

