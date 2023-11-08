import os
import sys
import estools
import pandas
from elasticsearch.helpers import scan

index = sys.argv[1]
es = estools.get_es_connection()
res = es.search(index=index, body={"query" : {"match_all": {}}}, size=100)

print(res["hits"]["hits"])

##using scan:
#res = scan(es, index=index, query={"query": {"match_all": {}}})
#row_list = []
#nlines=0
#for item in res:
#    row_list.append(item["_source"])
#    nlines+=1
#    if nlines==1000:
#        break
#df = pandas.DataFrame(row_list)
#df.to_csv('/output/%s.csv'%index.replace(".", "_"))


#print("Exported %i entries."%nlines)
