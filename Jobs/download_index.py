import os
import sys
import estools
import pandas
#from elasticsearch.helpers import scan

index = sys.argv[1]
date = index.split("-")[1].replace(".", "-")
print(date)
total_entries=0
timewindows = []
for i in range(24):
   hh = "0%i"%i if i<10 else "%i"%i
   hhup = hh
   hhp1 = "0%i"%(i+1) if i+1<10 else "%i"%(i+1)
   for j in range(60):
      mm = "0%i"%(j) if j<10 else "%i"%(j)
      mmp1 = "0%i"%(j+1) if j+1<10 else "%i"%(j+1)
      if j==59:
         hhup=hhp1
         mmp1="00"
      x = {}
      if i!=0 or j!=0:
         x["gte"] = "%sT%s:%s:00"%(date, hh, mm)
      if i!=23 or j!=59:
          x["lt"] = "%sT%s:%s:00"%(date, hhup, mmp1)

      #if i*60+j==1192 or i*60+j==1193:
      #   y = {}
      #   y["gte"] = x["gte"]
      #   y["lt"] = y["gte"].replace(":%s:00"%mm, ":%s:30"%mm)
      #   x["gte"] = y["lt"]
      #   print(y)
      #   print(x)
      #   timewindows.append(y)

      #if i*60+j==549:
      #   y = {}
      #   yy = {}
      #   yyy = {}
      #   y["gte"] = x["gte"]
      #   y["lt"] = y["gte"].replace(":%s:00"%mm, ":%s:15"%mm)
      #   yy["gte"] = y["lt"]
      #   yy["lt"] = y["gte"].replace(":%s:00"%mm, ":%s:30"%mm)
      #   yyy["gte"] = yy["lt"]
      #   yyy["lt"] = y["gte"].replace(":%s:00"%mm, ":%s:45"%mm)
      #   x["gte"] = yyy["lt"]
      #   print(y)
      #   print(yy)
      #   print(yyy)
      #   print(x)
      #   timewindows.append(y)
      #   timewindows.append(yy)
      #   timewindows.append(yyy)

      timewindows.append(x)

#print(timewindows)
#print(timewindows[-1])
#timewindows.append(timewindows[-1])
#timewindows[-1]["gte"] = "%sT23:59:30"%date
#timewindows[-2]["lt"] = "%sT23:59:30"%date
#timewindows[0]["gte"] = "2023-08-20T00:00:00"
#print(timewindows[0])

es = estools.get_es_connection()

row_list = []
for i in range(len(timewindows)):
    #if i<=int(len(timewindows)*2/3)-1:
    #    continue
    res = es.search(index=index, body={"query" : {"range": {"modificationtime" : timewindows[i]}}}, size=10000)

    total_entries += len(res["hits"]["hits"])
    if len(res["hits"]["hits"])==10000:
        print("WARNING: Max search length reached! Data incomplete!")
        raise Exception
    #print(res["hits"]["hits"][0]["_source"])

    ##using scan:
    #res = scan(es, index=index, query={"query": {"match_all": {}}})

    #row_list = []

    for item in res["hits"]["hits"]:
        row_list.append(item["_source"])
    print("Done %i/%i"%(i+1, len(timewindows)))

    ##Alternative method creating df already here (comment out df creation in ifs below):
    #if i==int(len(timewindows)/3)-1 or i==int(len(timewindows)/3*2)-1 or i==len(timewindows)-1:
    #    df2 = pandas.DataFrame(row_list)
    #    df = pandas.concat([df, df2], ignore_index=True)
    #    row_list = []
    #    del df2
    #if i==int(len(timewindows)/3)-1-240 or i==int(len(timewindows)/3*2)-1-240 or i==len(timewindows)-1-240:
    #    df = pandas.DataFrame(row_list)
    #    row_list = []


    if i==int(len(timewindows)/3)-1:
        df = pandas.DataFrame(row_list)
        df.to_csv('/afs/cern.ch/work/s/swozniew/private/ES_dump/%sa.csv'%index.replace(".", "_"))
        #df.to_hdf('/afs/cern.ch/work/s/swozniew/private/ES_dump/%sa.h5'%index.replace(".", "_"), key='jobtable')
        row_list = []
        del df
        #break
    if i==int(len(timewindows)/3*2)-1:
        df = pandas.DataFrame(row_list)
        df.to_csv('/afs/cern.ch/work/s/swozniew/private/ES_dump/%sb.csv'%index.replace(".", "_"))
        #df.to_hdf('/afs/cern.ch/work/s/swozniew/private/ES_dump/%sb.h5'%index.replace(".", "_"), key='jobtable')
        row_list = []
        del df
        #break
    if i==len(timewindows)-1:
        df = pandas.DataFrame(row_list)
        df.to_csv('/afs/cern.ch/work/s/swozniew/private/ES_dump/%sc.csv'%index.replace(".", "_"))
        #df.to_hdf('/afs/cern.ch/work/s/swozniew/private/ES_dump/%sc.h5'%index.replace(".", "_"), key='jobtable')
        row_list = []
        del df

print("Exported %i entries."%total_entries)
