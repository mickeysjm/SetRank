## Baselines

We implement the following five baseline methods based on [ElasticSearch Similarity Modules](https://www.elastic.co/guide/en/elasticsearch/reference/5.4/index-modules-similarity.html): 

1. classic (TFIDF) (performs significantly worse than the other four and thus not report in the paper)
2. BM25
3. LM-Dir (Language Model with Dirichlet Smoothing)
4. LM-JM (Language Model with Jelinek Mercer Smoothing)
5. IB (Information-Based IR)

After first data index creation, you can dynamically modify the hyper-parameters using the script [https://gist.github.com/mickeystroller/a2134d96fb1a67cea4b5bc5cd7c4afe3](https://gist.github.com/mickeystroller/a2134d96fb1a67cea4b5bc5cd7c4afe3).

## SetRank & AutoSetRank

We implement the SetRank algorithm based on ElasticSearch's scripting functions. This includes the following four steps: 

1. Create Index using either create_index_ESR.py or create_index_TREC.py
2. Index files using either index_data_ESR.py or index_data_TREC.py
3. Hyperparameter tuning using either autoSetRank_ESR.py or autoSetRank_TREC.py, and obtain the best hyperparameters.
4. Perform SetRank with the best hyperparameters using either setRank_ESR.py or setRank_TREC.py




