# SetRank

## Introduction

This repo includes all the benchmark datasets, source code, evaluation toolkit, and experiment results for SetRank framework developed for entity-set-aware literature search. 

## Data 

The **./data/** folder contains two benchmark datasets used for evaluating literature search, namely _S2-CS_ and _TREC-BIO_.

## Model Implementations

The **./code/** folder includes the baseline models and our proposed SetRank framework (including AutoSetRank). The model implementations depend heavily on [ElasticSearch 5.4.0](https://www.elastic.co/guide/en/elasticsearch/reference/5.4/index.html) which is an open-sourced search engine for indexing and performing full-text search. Furthermore, you need to install the following python packages by typing the command:

```
$ pip3 install -r requirements.txt
```

After creating the index, you can perform the search using following commands:

```
$ cd ./code/SetRank
$ python3 setRank_TREC.py -query ../../data/S2-CS/s2_query.json -output ../../results/s2/setRank.run
```

The results will then be saved in "../../results/s2/setRank.run".

## Evaluation Tool

The **./pytrec_eval/** folder includes the original evaluation toolkit [pytrec_eval](https://github.com/cvangysel/pytrec_eval) and our customized scripts for performing model evaluation.

You may first follow the instructions in **./pytrec_eval/README.md** to install this packages and then conduct the model evaluation using following commands:

```
$ cd ./pytrec_eval/examples
$ ./eval.sh ../../results/s2/setRank.run setRank ## first argument is path to run file and the result save files
```

## Experiment Results

The **./results/** folder includes all the experiment results reported in our paper. Specifically, each file with suffix _.run_ is the model output ranking files; each file with suffix _.eval.tsv_ is the query-specific evaluation result. Notice that in the paper, we only report the NDCG@{5,10,15,20}, while here we releases the experiment results in terms of other metrics, including MAP@{5,10,15,20} and success@{1,5,10}. 


## Citation 

If you use the datasets or model implementation code produced in this paper, please refer to our SIGIR paper:

```
@inproceedings{JiamingShen2018ess,
  title={Entity Set Search of Scientific Literature: An Unsupervised Ranking Approach},
  author={Jiaming Shen, Jinfeng Xiao, Xinwei He, Jingbo Shang, Saurabh Sinha, and Jiawei Han},
  publisher={ACM},
  booktitle={SIGIR},
  year={2018},
}
```

Furthermore, if you use the pytrec_eval toolkit, please also consider citing the original paper:

```
@inproceedings{VanGysel2018pytreceval,
  title={Pytrec\_eval: An Extremely Fast Python Interface to trec\_eval},
  author={Van Gysel, Christophe and de Rijke, Maarten},
  publisher={ACM},
  booktitle={SIGIR},
  year={2018},
}
```
