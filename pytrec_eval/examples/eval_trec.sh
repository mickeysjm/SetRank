#!/bin/bash

python3 trec_eval.py -qrel ../../data/TREC-BIO/trec.qrel -run $1 -measures all > ../../results/$2.eval.tmp
python3 parse_eval_res.py -trec_res ../../results/$2.eval.tmp -res_out ../../results/$2.eval.tsv

echo "=== TREC eval results ==="
tail -n 21 ../../results/$2.eval.tmp
head -n 1 ../../results/$2.eval.tsv
tail -n 1 ../../results/$2.eval.tsv