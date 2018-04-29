'''
__author__: Jiaming Shen
__description__: Index data from precomputed JSON.
'''
import time
import json
from elasticsearch import Elasticsearch

if __name__ == '__main__':
    inputFilePath = "../../data/S2-CS/s2_doc.json"
    logFilePath = "../../data/S2-CS/log.txt"
    statFilePath = "../../data/S2-CS/stats.txt"

    INDEX_NAME = "s2"
    TYPE_NAME = "s2_papers"

    es = Elasticsearch()

    with open(inputFilePath, "r") as fin, open(logFilePath, "w") as fout:
        start = time.time()
        bulk_size = 500 # number of document processed in each bulk index
        bulk_data = [] # data in bulk index
        
        ## saving the sum of all eight lengths for later model usage
        title_length_sum = 0
        abstract_length_sum = 0
        keyphrase_length_sum = 0
        title_ana_length_sum = 0
        abstract_ana_length_sum = 0
        bodytext_ana_length_sum = 0
        keyphrase_ana_length_sum = 0
        total_length_sum = 0
            
        cnt = 0
        for line in fin: ## each line is single document
            cnt += 1
            paperInfo = json.loads(line.strip())
            
            data_dict = {}
            total_length = 0
            
            # update docno
            data_dict["docno"] = paperInfo["docno"]

            # update venue, can be empty
            data_dict["venue"] = paperInfo["venue"][0]

            # update number of citation
            data_dict["numCitedBy"] = paperInfo["numCitedBy"]

            # update number of key citation
            data_dict["numKeyCitations"] = paperInfo["numKeyCitations"]

            # update title and its length field
            data_dict["title"] = paperInfo["title"][0]
            data_dict["title_length"] = len(paperInfo["title"][0].split())
            total_length += data_dict["title_length"]

            # update abstract and its length field
            data_dict["abstract"] = paperInfo["paperAbstract"][0]
            data_dict["abstract_length"] = len(paperInfo["paperAbstract"][0].split())
            total_length += data_dict["abstract_length"]
            
            # update keyphrase and its length field
            keyphrase = paperInfo.get("keyPhrases",[])
            data_dict["keyphrase"] = " ".join(keyphrase)
            data_dict["keyphrase_length"] = len(data_dict["keyphrase"].split())
            total_length += data_dict["keyphrase_length"]

            # update annotations
            # e.g., "keyPhrases": {"/m/04rbjc": 1, "/m/0cpvr": 1, "/m/02cjl": 1, "/m/03gj321": 1},
            annotations = paperInfo["ana"]
            for ann_field in annotations:
                ann_length = 0
                ann_list = []
                for k, v in annotations[ann_field].items():
                    ann_length += v
                    for i in range(v):
                        ann_list.append(k)
                if ann_field == "bodyText":
                    data_dict["bodytext_ana"] = " ".join(ann_list)
                    data_dict["bodytext_ana_length"] = ann_length
                elif ann_field == "title":
                    data_dict["title_ana"] = " ".join(ann_list)
                    data_dict["title_ana_length"] = ann_length
                elif ann_field == "paperAbstract":
                    data_dict["abstract_ana"] = " ".join(ann_list)
                    data_dict["abstract_ana_length"] = ann_length
                elif ann_field == "keyPhrases":
                    data_dict["keyphrase_ana"] = " ".join(ann_list)
                    data_dict["keyphrase_ana_length"] = ann_length
                else:
                    print("[ERROR] Wrong annotation field: %s" % ann_field)
                total_length += ann_length
            data_dict["total_length"] = total_length

            ## append zero for those papers without certain annotation fields
            for ann_field in ["bodytext_ana", "title_ana", "abstract_ana", "keyphrase_ana"]:
                if ann_field not in data_dict:
                    data_dict[ann_field] = ""
                    data_dict[ann_field+"_length"] = 0

            ## update the length status of each field and total length
            title_length_sum += data_dict["title_length"]
            abstract_length_sum += data_dict["abstract_length"]
            keyphrase_length_sum += data_dict["keyphrase_length"]

            title_ana_length_sum += data_dict["title_ana_length"]
            abstract_ana_length_sum += data_dict["abstract_ana_length"]
            keyphrase_ana_length_sum += data_dict["keyphrase_ana_length"]
            bodytext_ana_length_sum += data_dict["bodytext_ana_length"]

            total_length_sum += data_dict["total_length"]
            
            ## Put current data into the bulk
            op_dict = {
                "index": {
                    "_index": INDEX_NAME,
                    "_type": TYPE_NAME,
                    "_id": data_dict["docno"]
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
        

    print("Start saving statistics\n ")
    with open(statFilePath, "w") as fout:
        fout.write("NUM_PAPER = %s\n" % cnt)
        fout.write("TITLE_LENGTH_SUM = %s\n" % title_length_sum)
        fout.write("ABSTRACT_LENGTH_SUM = %s\n" % abstract_length_sum)
        fout.write("KEYPHRASE_LENGTH_SUM = %s\n" % keyphrase_length_sum)
        fout.write("TITLE_ANN_LENGTH_SUM = %s\n" % title_ana_length_sum)
        fout.write("ABSTRACT_ANA_LENGTH_SUM = %s\n" % abstract_ana_length_sum)
        fout.write("BODYTEXT_ANA_LENGTH_SUM = %s\n" % bodytext_ana_length_sum)
        fout.write("KEYPHRASES_ANA_LENGTH_SUM = %s\n" % keyphrase_ana_length_sum)
        fout.write("TOTAL_LENGTH_SUM = %s\n" % total_length_sum)

