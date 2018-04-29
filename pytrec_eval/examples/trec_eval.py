"""Approximately simulates trec_eval using pytrec_eval."""

import argparse
import os
import sys

import pytrec_eval


def main(args):
    assert os.path.exists(args.qrel)
    assert os.path.exists(args.run)

    with open(args.qrel, 'r') as f_qrel:
        qrel = pytrec_eval.parse_qrel(f_qrel)

    with open(args.run, 'r') as f_run:
        run = pytrec_eval.parse_run(f_run)

    if args.measures == "all":
        evaluator = pytrec_eval.RelevanceEvaluator(qrel, pytrec_eval.supported_measures)
    else:
        measures = set(args.measures.split(","))
        evaluator = pytrec_eval.RelevanceEvaluator(qrel, measures)


    results = evaluator.evaluate(run)

    def print_line(measure, scope, value):
        print('{:25s}{:8s}{:.6f}'.format(measure, scope, value))

    for query_id, query_measures in sorted(results.items()):
        for measure, value in sorted(query_measures.items()):
            print_line(measure, query_id, value)

    # Scope hack: use query_measures of last item in previous loop to
    # figure out all unique measure names.
    #
    # TODO(cvangysel): add member to RelevanceEvaluator
    #                  with a list of measure names.
    for measure in sorted(query_measures.keys()):
        print_line(
            measure,
            'all',
            pytrec_eval.compute_aggregated_measure(
                measure,
                [query_measures[measure]
                 for query_measures in results.values()]))

if __name__ == "__main__":
    # Example usage: python3 -qrel ./XXX.qrel -run ./YYY.run -measures ndcg,map,ndcg_cut,map_cut
    parser = argparse.ArgumentParser(prog='trec_eval.py', description='Python Implementation of TREC Evaluation Toolkit.')
    parser.add_argument('-qrel', required=True, help='File name of query relevance file.')
    parser.add_argument('-run', required=True, help='File name of each method\'s run.')
    parser.add_argument('-measures', required=False, default='all', help='Evaluation measures for current run. separated by \',\','
        'please refer to https://github.com/usnistgov/trec_eval for the names of all measures.')
    args = parser.parse_args()
    sys.exit(main(args))
