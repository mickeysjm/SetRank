'''
__author__: Jiaming Shen
__description__: Index data from precomputed JSON, which includes merged PubMed and PubTator
'''
import time
import json
from collections import defaultdict
from elasticsearch import Elasticsearch
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='index_data.py', description='index data with different similarities.')
    parser.add_argument('-sim', required=True, help='name of similarity module')
    args = parser.parse_args()

    SIM_MODULE_NAME = args.sim  # one of ["tfidf", "bm25", "lm_dir", "lm_jm", "ib"]
    INDEX_NAME = "trec0405_" + SIM_MODULE_NAME
    TYPE_NAME = "trec0405_papers_" + SIM_MODULE_NAME

    inputFilePath = "../../../data/TREC-BIO/trec_doc.json"
    logFilePath = "./log_%s.txt" % SIM_MODULE_NAME
    statFilePath = "./stats_%s.txt" % SIM_MODULE_NAME

    es = Elasticsearch()

    with open(inputFilePath, "r") as fin, open(logFilePath, "w") as fout:
        start = time.time()
        bulk_size = 500 # number of document processed in each bulk index
        bulk_data = [] # data in bulk index
        
        ## saving the sum of all lengths for later model usage
        title_length_sum = 0
        abstract_length_sum = 0
        title_ana_length_sum = 0
        abstract_ana_length_sum = 0
        total_length_sum = 0
            
        cnt = 0
        for line in fin: ## each line is single document
            cnt += 1
            paperInfo = json.loads(line.strip())
            
            data_dict = {}
            total_length = 0
            
            # update PMID
            data_dict["pmid"] = paperInfo["pmid"]
            
            # update title
            data_dict["title"] = paperInfo["title"]
            data_dict["title_length"] = len(paperInfo["title"].split())
            total_length += data_dict["title_length"]
            
            # update abstract
            data_dict["abstract"] = paperInfo["abstract"]
            data_dict["abstract_length"] = len(paperInfo["abstract"].split())
            total_length += data_dict["abstract_length"]
            
            # update date
            data_dict["date"] = paperInfo["date"]
            
            # update author list
            if paperInfo["author"]:
                data_dict["author_list"] = paperInfo["author"].split(";")
            else:
                data_dict["author_list"] = []
                
            # update journal name
            data_dict["journal_name"] = paperInfo["journal"]

            # update mesh
            if paperInfo["mesh_heading"]:
                data_dict["mesh"] = paperInfo["mesh_heading"]
            else:
                data_dict["mesh"] = ""
            
            # update entities information
            entities = defaultdict(list)
            title_ana = []
            abstract_ana = []
            title_ana_length = 0
            abstract_ana_length = 0
            for ele in paperInfo["entity"]:
                entity_mention = "_".join(ele["name"].split()).lower() # use "_" to connect multi-tokens entity mention
                if ele["position"] == "title":
                    title_ana.append(entity_mention)
                    title_ana_length += 1
                elif ele["position"] == "abstract":
                    abstract_ana.append(entity_mention)
                    abstract_ana_length += 1
                else:
                    continue
            data_dict["title_ana"] = " ".join(title_ana)
            data_dict["abstract_ana"] = " ".join(abstract_ana)
            data_dict["title_ana_length"] = title_ana_length
            data_dict["abstract_ana_length"] = abstract_ana_length

            total_length += data_dict["title_ana_length"]
            total_length += data_dict["abstract_ana_length"]
            data_dict["total_length"] = total_length
            
            ## update the length status of each field
            title_length_sum += data_dict["title_length"]
            abstract_length_sum += data_dict["abstract_length"]
            title_ana_length_sum += data_dict["title_ana_length"]
            abstract_ana_length_sum += data_dict["abstract_ana_length"]
            total_length_sum += data_dict["total_length"]
            
            ## Put current data into the bulk
            op_dict = {
                "index": {
                    "_index": INDEX_NAME,
                    "_type": TYPE_NAME,
                    "_id": data_dict["pmid"]
                }
            }

            bulk_data.append(op_dict)
            bulk_data.append(data_dict)       
                    
            ## Start Bulk indexing
            if cnt % bulk_size == 0 and cnt != 0:
                tmp = time.time()
                es.bulk(index=INDEX_NAME, body=bulk_data, request_timeout = 180)
                fout.write("bulk indexing... %s, escaped time %s (seconds) \n" % ( cnt, tmp - start ) )
                print("bulk indexing... %s, escaped time %s (seconds) " % ( cnt, tmp - start ) )
                bulk_data = []
        
        ## indexing those left papers
        if bulk_data:
            tmp = time.time()
            es.bulk(index=INDEX_NAME, body=bulk_data, request_timeout = 180)
            fout.write("bulk indexing... %s, escaped time %s (seconds) \n" % ( cnt, tmp - start ) )
            print("bulk indexing... %s, escaped time %s (seconds) " % ( cnt, tmp - start ) )
            bulk_data = []

        end = time.time()
        fout.write("Finish indexing. Total escaped time %s (seconds) \n" % (end - start) )
        print("Finish indexing. Total escaped time %s (seconds) " % (end - start) )
        

    print("Start saving statistics \n ")
    with open(statFilePath, "w") as fout:
        fout.write("NUM_PAPER = %s\n" % cnt)
        fout.write("TITLE_LENGTH_SUM = %s\n" % title_length_sum)
        fout.write("ABSTRACT_LENGTH_SUM = %s\n" % abstract_length_sum)
        fout.write("TITLE_ANA_LENGTH_SUM = %s\n" % title_ana_length_sum)
        fout.write("ABSTRACT_ANA_LENGTH_SUM = %s\n" % abstract_ana_length_sum)
        fout.write("TOTAL_LENGTH_SUM = %s\n" % total_length_sum)
