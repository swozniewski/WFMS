import os
import sys
import cx_Oracle
import estools
import conversions

if not 'JOB_ORACLE_CONNECTION_STRING' in os.environ:
    print('Connection to ORACLE DB not configured. Please set variable: JOB_ORACLE_CONNECTION_STRING ')
    sys.exit(-1)

if not 'JOB_ORACLE_PASS' in os.environ or not 'JOB_ORACLE_USER' in os.environ:
    print('Please set variables:JOB_ORACLE_USER and JOB_ORACLE_PASS.')
    sys.exit(-1)

if not len(sys.argv) == 3:
    print('Pleae provide Start and End times in YYYY-mm-DD HH:MM::SS format.')
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
not_stored_anymore = ['MAXCPUUNIT', 'MAXDISKUNIT', 'IPCONNECTIVITY', 'MINRAMUNIT', 'PRODDBUPDATETIME', 'NINPUTFILES']
print('omitting columns:', not_stored_anymore)


columns = [
    'PANDAID', 'JOBDEFINITIONID', 'SCHEDULERID', 'PILOTID', 'CREATIONTIME', 'CREATIONHOST', 'MODIFICATIONTIME',
    'MODIFICATIONHOST', 'ATLASRELEASE', 'TRANSFORMATION', 'HOMEPACKAGE', 'PRODSERIESLABEL', 'PRODSOURCELABEL',
    'PRODUSERID', 'ASSIGNEDPRIORITY', 'CURRENTPRIORITY', 'ATTEMPTNR', 'MAXATTEMPT', 'JOBSTATUS', 'JOBNAME',
    'MAXCPUCOUNT', 'MAXDISKCOUNT', 'MINRAMCOUNT',
    'STARTTIME', 'ENDTIME', 'CPUCONSUMPTIONTIME', 'CPUCONSUMPTIONUNIT', 'COMMANDTOPILOT', 'TRANSEXITCODE',
    'PILOTERRORCODE', 'PILOTERRORDIAG', 'EXEERRORCODE', 'EXEERRORDIAG', 'SUPERRORCODE', 'SUPERRORDIAG',
    'DDMERRORCODE', 'DDMERRORDIAG', 'BROKERAGEERRORCODE', 'BROKERAGEERRORDIAG', 'JOBDISPATCHERERRORCODE',
    'JOBDISPATCHERERRORDIAG', 'TASKBUFFERERRORCODE', 'TASKBUFFERERRORDIAG', 'COMPUTINGSITE', 'COMPUTINGELEMENT',
    'PRODDBLOCK', 'DISPATCHDBLOCK', 'DESTINATIONDBLOCK', 'DESTINATIONSE', 'NEVENTS', 'GRID', 'CLOUD', 'CPUCONVERSION',
    'SOURCESITE', 'DESTINATIONSITE', 'TRANSFERTYPE', 'TASKID', 'CMTCONFIG', 'STATECHANGETIME',
    'LOCKEDBY', 'RELOCATIONFLAG', 'JOBEXECUTIONID', 'VO', 'WORKINGGROUP', 'PROCESSINGTYPE', 'PRODUSERNAME',
    'COUNTRYGROUP', 'BATCHID', 'PARENTID', 'SPECIALHANDLING', 'JOBSETID', 'CORECOUNT', 'NINPUTDATAFILES',
    'INPUTFILETYPE', 'INPUTFILEPROJECT', 'INPUTFILEBYTES', 'NOUTPUTDATAFILES', 'OUTPUTFILEBYTES', 'JOBMETRICS',
    'WORKQUEUE_ID', 'JEDITASKID', 'JOBSUBSTATUS', 'ACTUALCORECOUNT', 'REQID', 'MAXRSS', 'MAXVMEM', 'MAXPSS',
    'AVGRSS', 'AVGVMEM', 'AVGSWAP', 'AVGPSS', 'MAXWALLTIME', 'NUCLEUS', 'EVENTSERVICE', 'FAILEDATTEMPT', 'HS06SEC', 'HS06', 'GSHARE',
    'TOTRCHAR', 'TOTWCHAR', 'TOTRBYTES', 'TOTWBYTES', 'RATERCHAR', 'RATEWCHAR', 'RATERBYTES', 'RATEWBYTES',
    'PILOTTIMING', 'MEMORY_LEAK', 'RESOURCE_TYPE', 'DISKIO'
]

escolumns = [
    'pandaid', 'jobdefinitionid', 'schedulerid', 'pilotid', 'creationtime', 'creationhost', 'modificationtime',
    'modificationhost', 'atlasrelease', 'transformation', 'homepackage', 'prodserieslabel', 'prodsourcelabel',
    'produserid', 'assignedpriority', 'currentpriority', 'attemptnr', 'maxattempt', 'jobstatus', 'jobname',
    'maxcpucount', 'maxdiskcount', 'minramcount',
    'starttime', 'endtime', 'cpuconsumptiontime', 'cpuconsumptionunit', 'commandtopilot', 'transexitcode',
    'piloterrorcode', 'piloterrordiag', 'exeerrorcode', 'exeerrordiag', 'superrorcode', 'superrordiag',
    'ddmerrorcode', 'ddmerrordiag', 'brokerageerrorcode', 'brokerageerrordiag', 'jobdispatchererrorcode',
    'jobdispatchererrordiag', 'taskbuffererrorcode', 'taskbuffererrordiag', 'computingsite', 'computingelement',
    'proddblock', 'dispatchdblock', 'destinationdblock', 'destinationse', 'nevents', 'grid', 'cloud', 'cpuconversion',
    'sourcesite', 'destinationsite', 'transfertype', 'taskid', 'cmtconfig', 'statechangetime',
    'lockedby', 'relocationflag', 'jobexecutionid', 'vo', 'workinggroup', 'processingtype', 'produsername',
    'countrygroup', 'batchid', 'parentid', 'specialhandling', 'jobsetid', 'corecount', 'ninputdatafiles',
    'inputfiletype', 'inputfileproject', 'inputfilebytes', 'noutputdatafiles', 'outputfilebytes', 'jobmetrics',
    'workqueue_id', 'jeditaskid', 'jobsubstatus', 'actualcorecount', 'reqid', 'maxrss', 'maxvmem', 'maxpss',
    'avgrss', 'avgvmem', 'avgswap', 'avgpss', 'maxwalltime', 'nucleus', 'eventservice', 'failedattempt', 'hs06sec', 'hs06', 'gShare',
    'IOcharRead', 'IOcharWritten', 'IObytesRead', 'IObytesWritten', 'IOcharReadRate', 'IOcharWriteRate', 'IObytesReadRate', 'IObytesWriteRate',
    'pilottiming', 'memory_leak', 'resource_type', 'diskio'
]

sel = 'SELECT '
sel += ','.join(columns)
sel += ' FROM ATLAS_PANDAARCH.JOBSARCHIVED '
# sel += 'WHERE PANDAID=4225560422'
sel += "WHERE STATECHANGETIME >= TO_DATE('" + start_date + \
    "','YYYY - MM - DD HH24: MI: SS') AND STATECHANGETIME < TO_DATE('" + end_date + "','YYYY - MM - DD HH24: MI: SS') "

# print(sel)

cursor.execute(sel)

es = estools.get_es_connection()

data = []
count = 0
for row in cursor:
    doc = {}
    for colName, colValue in zip(escolumns, row):
        # print(colName, colValue)
        doc[colName] = colValue

    if doc['creationtime']:
        doc['creationtime'] = str(doc['creationtime']).replace(' ', 'T')
    if doc['modificationtime']:
        doc['modificationtime'] = str(doc['modificationtime']).replace(' ', 'T')
    if doc['starttime']:
        doc['starttime'] = str(doc['starttime']).replace(' ', 'T')
    if doc['endtime']:
        doc['endtime'] = str(doc['endtime']).replace(' ', 'T')
    doc['cpuconsumptiontime'] = int(doc['cpuconsumptiontime'])
    if doc['statechangetime']:
        doc['statechangetime'] = str(doc['statechangetime']).replace(' ', 'T')

    (doc['dbTime'], doc['dbData'], doc['workDirSize'], doc['jobmetrics']) = conversions.splitJobmetrics(doc['jobmetrics'])
    (doc['wall_time'], doc['cpu_eff'], doc['queue_time']) = conversions.deriveDurationAndCPUeff(
        doc['creationtime'], doc['starttime'], doc['endtime'], doc['cpuconsumptiontime'])
    (doc['timeGetJob'], doc['timeStageIn'], doc['timeExe'], doc['timeStageOut'],
     doc['timeSetup']) = conversions.deriveTimes(doc['pilottiming'])
    doc["_index"] = "jobs_archive_" + doc['creationtime'].split('T')[0]
    doc["_id"] = doc['pandaid']

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
