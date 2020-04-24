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

if not len(sys.argv) == 3:
    print('Please provide Start and End times in YYYY-mm-DD HH:MM::SS format.')
    sys.exit(-1)

start_date = sys.argv[1]
end_date = sys.argv[2]

print('Start date:', start_date, '\t End date:', end_date)

user = os.environ['JOB_ORACLE_USER']
passw = os.environ['JOB_ORACLE_PASS']
conn = os.environ['JOB_ORACLE_CONNECTION_STRING'].replace('jdbc:oracle:thin:@//', '')
con = cx_Oracle.connect(user + '/' + passw + '@' + conn)
print(con.version)

cursor = con.cursor()

columns = [
    'JEDITASKID', 'TASKPARAMS'
]

escolumns = [
    'jeditaskid', 'taskparams'
]

sel = 'SELECT '
sel += ','.join(columns)
sel += ' FROM ATLAS_PANDA.JEDI_TASKPARAMS  '
# sel += 'WHERE JEDITASKID=123'
# sel += "WHERE MODIFICATIONTIME >= TO_DATE('" + start_date + \
    # "','YYYY - MM - DD HH24: MI: SS') AND MODIFICATIONTIME < TO_DATE('" + end_date + "','YYYY - MM - DD HH24: MI: SS') "

# print(sel)

cursor.execute(sel)

es = estools.get_es_connection()

data = []
count = 0
for row in cursor:
    doc = {"_type": "docs"}
    for colName, colValue in zip(escolumns, row):
        # print(colName, colValue)
        doc[colName] = colValue

    if doc['lockedtime']:
        doc['lockedtime'] = str(doc['lockedtime']).replace(' ', 'T')
    if doc['statechangetime']:
        doc['statechangetime'] = str(doc['statechangetime']).replace(' ', 'T')
    if doc['creationdate']:
        doc['creationdate'] = str(doc['creationdate']).replace(' ', 'T')
    if doc['modificationtime']:
        doc['modificationtime'] = str(doc['modificationtime']).replace(' ', 'T')
    if doc['starttime']:
        doc['starttime'] = str(doc['starttime']).replace(' ', 'T')
    if doc['endtime']:
        doc['endtime'] = str(doc['endtime']).replace(' ', 'T')
    if doc['frozentime']:
        doc['frozentime'] = str(doc['frozentime']).replace(' ', 'T')
    if doc['rescuetime']:
        doc['rescuetime'] = str(doc['rescuetime']).replace(' ', 'T')
    if doc['ttcpredictiondate']:
        doc['ttcpredictiondate'] = str(doc['ttcpredictiondate']).replace(' ', 'T')

    doc["_index"] = "tasks_parameters_write"
    doc["_id"] = doc['jeditaskid']

    data.append(doc)
    # print(doc)

    if not count % 500:
        print(count)
        res = estools.bulk_index(data, es)
        if res:
            del data[:]
    count += 1

estools.bulk_index(data, es)
print('final count:', count)


con.close()
