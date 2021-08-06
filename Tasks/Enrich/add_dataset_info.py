import os
import sys
import cx_Oracle
from .. import estools

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
conn = os.environ['JOB_ORACLE_CONNECTION_STRING'].replace(
    'jdbc:oracle:thin:@//', '')
con = cx_Oracle.connect(user + '/' + passw + '@' + conn)

print(con.version)

es = estools.get_es_connection()

cursor = con.cursor()

sel = """
select JEDITASKID, REQID, SUM(OUTPUT_EV), SUM(INPUT_EV) from (
    select 
        t.jeditaskid, 
        t.reqid, 
        CASE WHEN d.type='output' THEN d.nevents ELSE 0 end as OUTPUT_EV, 
        CASE WHEN d.type in ('input', 'pseudo_input') THEN d.nevents ELSE 0 end as INPUT_EV
    from JEDI_TASKS t 
    join JEDI_DATASETS d 
    on d.jeditaskid = t.jeditaskid 
    where 
    t.status='finished' and 
    t.tasktype='prod' and 
    t.processingtype in ('simul', 'evgen') and 
    t.modificationtime BETWEEN TO_DATE('""" + \
    start_date + "','YYYY - MM - DD HH24: MI: SS') " + \
    "AND TO_DATE('" + \
    end_date + "','YYYY - MM - DD HH24: MI: SS') ) group by JEDITASKID, REQID"


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
