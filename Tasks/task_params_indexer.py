import os
import sys
import cx_Oracle
import estools

if not 'JOB_ORACLE_CONNECTION_STRING' in os.environ:
    print('Connection to ORACLE DB not configured. Please set variable: JOB_ORACLE_CONNECTION_STRING ')
    sys.exit(-1)

if not 'JOB_ORACLE_PASS' in os.environ or not 'JOB_ORACLE_USER' in os.environ:
    print('Please set variables:JOB_ORACLE_USER and JOB_ORACLE_PASS.')
    sys.exit(-1)

user = os.environ['JOB_ORACLE_USER']
passw = os.environ['JOB_ORACLE_PASS']
conn = os.environ['JOB_ORACLE_CONNECTION_STRING'].replace('jdbc:oracle:thin:@//', '')
con = cx_Oracle.connect(user + '/' + passw + '@' + conn)
print(con.version)

es = estools.get_es_connection()
maxJID = """{
    "query": {
        "match_all": {}
    },
    "sort": [
        {
            "_id": {
                "order": "desc"
            }
        }
    ],
    "size": 1
}"""

res = es.search(index="tasks_parameters_write", body=maxJID)
mJID = res['hits']['hits'][0]['_id']

cursor = con.cursor()

sel = 'SELECT JEDITASKID, TASKPARAMS FROM ATLAS_PANDA.JEDI_TASKPARAMS '
sel += 'WHERE JEDITASKID>' + str(mJID)
print(sel)

cursor.execute(sel)

data = []
count = 0
for row in cursor:
    doc = {
        "_type": "docs",
        "_index": "tasks_parameters_write",
        "pipeline": "tasks_parameters",
        "_id": row[0],
        "taskparams": row[1]
    }

    # if doc['lockedtime']:
    #     doc['lockedtime'] = str(doc['lockedtime']).replace(' ', 'T')
    # if doc['statechangetime']:
    #     doc['statechangetime'] = str(doc['statechangetime']).replace(' ', 'T')
    # if doc['creationdate']:
    #     doc['creationdate'] = str(doc['creationdate']).replace(' ', 'T')
    # if doc['modificationtime']:
    #     doc['modificationtime'] = str(doc['modificationtime']).replace(' ', 'T')
    # if doc['starttime']:
    #     doc['starttime'] = str(doc['starttime']).replace(' ', 'T')
    # if doc['endtime']:
    #     doc['endtime'] = str(doc['endtime']).replace(' ', 'T')
    # if doc['frozentime']:
    #     doc['frozentime'] = str(doc['frozentime']).replace(' ', 'T')
    # if doc['rescuetime']:
    #     doc['rescuetime'] = str(doc['rescuetime']).replace(' ', 'T')
    # if doc['ttcpredictiondate']:
    #     doc['ttcpredictiondate'] = str(doc['ttcpredictiondate']).replace(' ', 'T')

    data.append(doc)
    # print(doc)

    if not count % 500:
        print(count)
        res = estools.bulk_index(data, es)
        if res:
            del data[:]
        break
    count += 1

estools.bulk_index(data, es)
print('final count:', count)


con.close()
