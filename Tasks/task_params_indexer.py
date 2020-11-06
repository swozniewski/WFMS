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


def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)


user = os.environ['JOB_ORACLE_USER']
passw = os.environ['JOB_ORACLE_PASS']
conn = os.environ['JOB_ORACLE_CONNECTION_STRING'].replace(
    'jdbc:oracle:thin:@//', '')
con = cx_Oracle.connect(user + '/' + passw + '@' + conn)
con.outputtypehandler = OutputTypeHandler

print(con.version)

es = estools.get_es_connection()

cursor = con.cursor()


sel = 'SELECT JEDI_TASKS.JEDITASKID, JEDI_TASKS.STATUS, TASKPARAMS, CREATIONDATE '
sel += 'FROM ATLAS_PANDA.JEDI_TASKPARAMS INNER JOIN ATLAS_PANDA.JEDI_TASKS '
sel += 'ON (JEDI_TASKPARAMS.JEDITASKID=JEDI_TASKS.JEDITASKID) '
sel += "WHERE JEDI_TASKS.CREATIONDATE >= TO_DATE('" + \
    start_date + "','YYYY - MM - DD HH24: MI: SS') "
sel += "AND JEDI_TASKS.CREATIONDATE < TO_DATE('" + \
    end_date + "','YYYY - MM - DD HH24: MI: SS') "

print(sel)


cursor.execute(sel)

data = []
count = 1
for row in cursor:
    doc = {
        "_index": "tasks_parameters_write",
        "pipeline": "tasks_parameters",
        "_id": row[0],
        "status": row[1],
        "taskparams": row[2],
        "creationdate": row[3]
    }
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
