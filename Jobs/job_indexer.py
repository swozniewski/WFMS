import os
import sys
import cx_Oracle
import estools
import conversions
from mapping import mapping

if 'JOB_ORACLE_CONNECTION_STRING' not in os.environ:
    print('Connection to ORACLE DB not configured. Please set variable: JOB_ORACLE_CONNECTION_STRING ')
    sys.exit(-1)

if 'JOB_ORACLE_PASS' not in os.environ or 'JOB_ORACLE_USER' not in os.environ:
    print('Please set variables:JOB_ORACLE_USER and JOB_ORACLE_PASS.')
    sys.exit(-1)

if not len(sys.argv) == 4:
    print('Please provide Start and End times in YYYY-mm-DD HH:MM::SS format and indexbase.')
    sys.exit(-1)

start_date = sys.argv[1]
end_date = sys.argv[2]
indexbase = sys.argv[3]

print('Start date:', start_date, '\tEnd date:', end_date)

user = os.environ['JOB_ORACLE_USER']
passw = os.environ['JOB_ORACLE_PASS']
conn = os.environ['JOB_ORACLE_CONNECTION_STRING'].replace('jdbc:oracle:thin:@//', '')
con = cx_Oracle.connect(user + '/' + passw + '@' + conn)
# con = cx_Oracle.connect(user + '/' + passw + '@adcr_prodsys')
print(con.version)


cursor = con.cursor()
not_stored_anymore = ['MAXCPUUNIT', 'MAXDISKUNIT',
                      'IPCONNECTIVITY', 'MINRAMUNIT', 'PRODDBUPDATETIME', 'NINPUTFILES', 'VO']
print('omitting columns:', not_stored_anymore)


columns = [
    'JOBS.PANDAID', 'JOBS.JOBDEFINITIONID', 'JOBS.SCHEDULERID', 'JOBS.PILOTID', 'JOBS.CREATIONTIME', 'JOBS.CREATIONHOST', 'JOBS.MODIFICATIONTIME',
    'JOBS.MODIFICATIONHOST', 'JOBS.ATLASRELEASE', 'JOBS.TRANSFORMATION', 'JOBS.HOMEPACKAGE', 'JOBS.PRODSERIESLABEL', 'JOBS.PRODSOURCELABEL',
    'JOBS.PRODUSERID', 'JOBS.ASSIGNEDPRIORITY', 'JOBS.CURRENTPRIORITY', 'JOBS.ATTEMPTNR', 'JOBS.MAXATTEMPT', 'JOBS.JOBSTATUS', 'JOBS.JOBNAME',
    'JOBS.MAXCPUCOUNT', 'JOBS.MAXDISKCOUNT', 'JOBS.MINRAMCOUNT',
    'JOBS.STARTTIME', 'JOBS.ENDTIME', 'JOBS.CPUCONSUMPTIONTIME', 'JOBS.CPUCONSUMPTIONUNIT', 'JOBS.COMMANDTOPILOT', 'JOBS.TRANSEXITCODE',
    'JOBS.PILOTERRORCODE', 'JOBS.PILOTERRORDIAG', 'JOBS.EXEERRORCODE', 'JOBS.EXEERRORDIAG', 'JOBS.SUPERRORCODE', 'JOBS.SUPERRORDIAG',
    'JOBS.DDMERRORCODE', 'JOBS.DDMERRORDIAG', 'JOBS.BROKERAGEERRORCODE', 'JOBS.BROKERAGEERRORDIAG', 'JOBS.JOBDISPATCHERERRORCODE',
    'JOBS.JOBDISPATCHERERRORDIAG', 'JOBS.TASKBUFFERERRORCODE', 'JOBS.TASKBUFFERERRORDIAG', 'JOBS.COMPUTINGSITE', 'JOBS.COMPUTINGELEMENT',
    'JOBS.PRODDBLOCK', 'JOBS.DISPATCHDBLOCK', 'JOBS.DESTINATIONDBLOCK', 'JOBS.DESTINATIONSE', 'JOBS.NEVENTS', 'JOBS.GRID', 'JOBS.CLOUD', 'JOBS.CPUCONVERSION',
    'JOBS.SOURCESITE', 'JOBS.DESTINATIONSITE', 'JOBS.TRANSFERTYPE', 'JOBS.TASKID', 'JOBS.CMTCONFIG', 'JOBS.STATECHANGETIME',
    'JOBS.LOCKEDBY', 'JOBS.RELOCATIONFLAG', 'JOBS.JOBEXECUTIONID', 'JOBS.WORKINGGROUP', 'JOBS.PROCESSINGTYPE', 'JOBS.PRODUSERNAME',
    'JOBS.COUNTRYGROUP', 'JOBS.BATCHID', 'JOBS.PARENTID', 'JOBS.SPECIALHANDLING', 'JOBS.JOBSETID', 'JOBS.CORECOUNT', 'JOBS.NINPUTDATAFILES',
    'JOBS.INPUTFILETYPE', 'JOBS.INPUTFILEPROJECT', 'JOBS.INPUTFILEBYTES', 'JOBS.NOUTPUTDATAFILES', 'JOBS.OUTPUTFILEBYTES', 'JOBS.JOBMETRICS',
    'JOBS.WORKQUEUE_ID', 'JOBS.JEDITASKID', 'JOBS.JOBSUBSTATUS', 'JOBS.ACTUALCORECOUNT', 'JOBS.REQID', 'JOBS.MAXRSS', 'JOBS.MAXVMEM', 'JOBS.MAXPSS',
    'JOBS.AVGRSS', 'JOBS.AVGVMEM', 'JOBS.AVGSWAP', 'JOBS.AVGPSS', 'JOBS.MAXWALLTIME', 'JOBS.NUCLEUS', 'JOBS.EVENTSERVICE', 'JOBS.FAILEDATTEMPT', 'JOBS.HS06SEC', 'JOBS.HS06', 'JOBS.GSHARE',
    'JOBS.TOTRCHAR', 'JOBS.TOTWCHAR', 'JOBS.TOTRBYTES', 'JOBS.TOTWBYTES', 'JOBS.RATERCHAR', 'JOBS.RATEWCHAR', 'JOBS.RATERBYTES', 'JOBS.RATEWBYTES',
    'JOBS.PILOTTIMING', 'JOBS.MEMORY_LEAK', 'JOBS.RESOURCE_TYPE', 'JOBS.DISKIO', 'JOBS.CONTAINER_NAME', 'TASKS.SIMULATION_TYPE', 'JOBS.MAXSWAP',
    'JOBS.MAXCPUUNIT', 'JOBS.MAXDISKUNIT', 'JOBS.MINRAMUNIT', 'JOBS.MEANCORECOUNT', 'JOBS.MEMORY_LEAK_X2',
    'JOBS.IPCONNECTIVITY', 'JOBS.PRODDBUPDATETIME', 'JOBS.NINPUTFILES', 'JOBS.JOBPARAMETERS', 'JOBS.METADATA', 'JOBS.JOB_LABEL'
]

jedi_columns = [
    'JEDI.TASKNAME', 'JEDI.USERNAME', 'JEDI.STATUS', 'JEDI.MODIFICATIONTIME', 'JEDI.CREATIONDATE', 'JEDI.OLDSTATUS',
    'JEDI.FROZENTIME', 'JEDI.STARTTIME', 'JEDI.TASKPRIORITY', 'JEDI.ENDTIME', 'JEDI.ARCHITECTURE',
    'JEDI.TRANSHOME', 'JEDI.CORECOUNT', 'JEDI.LOCKEDTIME', 'JEDI.TERMCONDITION', 'JEDI.CURRENTPRIORITY', 'JEDI.SPLITRULE',
    'JEDI.WALLTIME', 'JEDI.WALLTIMEUNIT', 'JEDI.SITE',
    'JEDI.STATECHANGETIME', 'JEDI.OUTDISKUNIT', 'JEDI.OUTDISKCOUNT', 'JEDI.GSHARE', 'JEDI.WORKDISKUNIT',
    'JEDI.DISKIO', 'JEDI.WORKDISKCOUNT', 'JEDI.MEMORY_LEAK_X2', 'JEDI.RAMCOUNT', 'JEDI.ATTEMPTNR', 'JEDI.RAMUNIT',
    'JEDI.IOINTENSITY', 'JEDI.IOINTENSITYUNIT', 'JEDI.PROGRESS', 'JEDI.FAILURERATE', 'JEDI.ERRORDIALOG', 'JEDI.PARENT_TID',
    'JEDI.TICKETID', 'JEDI.TICKETSYSTEMTYPE', 'JEDI.SUPERSTATUS', 'JEDI.CAMPAIGN', 'JEDI.MERGERAMCOUNT', 'JEDI.MERGERAMUNIT',
    'JEDI.MERGEWALLTIME', 'JEDI.MERGEWALLTIMEUNIT', 'JEDI.THROTTLEDTIME', 'JEDI.NUMTHROTTLED', 'JEDI.MERGECORECOUNT', 'JEDI.GOAL',
    'JEDI.ASSESSMENTTIME', 'JEDI.CPUTIME', 'JEDI.CPUTIMEUNIT', 'JEDI.CPUEFFICIENCY', 'JEDI.BASEWALLTIME', 'JEDI.AMIFLAG_OLD',
    'JEDI.AMIFLAG', 'JEDI.BASERAMCOUNT', 'JEDI.TTCREQUESTED', 'JEDI.TTCPREDICTED', 'JEDI.TTCPREDICTIONDATE', 'JEDI.RESCUETIME',
    'JEDI.REQUESTTYPE', 'JEDI.USEJUMBO', 'JEDI.DISKIOUNIT', 'JEDI.MEMORY_LEAK_CORE'
]

columns += jedi_columns
kicked_jedi_colums = ['JEDI.REQID', 'JEDI.CLOUD', 'JEDI.PRODSOURCELABEL', 'JEDI.WORKINGGROUP', 'JEDI.VO', 'JEDI.PROCESSINGTYPE',
    'JEDI.LOCKEDBY', 'JEDI.WORKQUEUE_ID', 'JEDI.COUNTRYGROUP', 'JEDI.EVENTSERVICE', 'JEDI.NUCLEUS', 'JEDI.CONTAINER_NAME',
    'JEDI.RESOURCE_TYPE', 'JEDI.TASKTYPE', 'JEDI.TRANSUSES', 'JEDI.TRANSPATH']
#'JEDI.STATECHANGETIME' would be the next candidate to drop

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
    'lockedby', 'relocationflag', 'jobexecutionid', 'workinggroup', 'processingtype', 'produsername',
    'countrygroup', 'batchid', 'parentid', 'specialhandling', 'jobsetid', 'corecount', 'ninputdatafiles',
    'inputfiletype', 'inputfileproject', 'inputfilebytes', 'noutputdatafiles', 'outputfilebytes', 'jobmetrics',
    'workqueue_id', 'jeditaskid', 'jobsubstatus', 'actualcorecount', 'reqid', 'maxrss', 'maxvmem', 'maxpss',
    'avgrss', 'avgvmem', 'avgswap', 'avgpss', 'maxwalltime', 'nucleus', 'eventservice', 'failedattempt', 'hs06sec', 'hs06', 'gShare',
    'IOcharRead', 'IOcharWritten', 'IObytesRead', 'IObytesWritten', 'IOcharReadRate', 'IOcharWriteRate', 'IObytesReadRate', 'IObytesWriteRate',
    'pilottiming', 'memory_leak', 'resource_type', 'diskio', 'container_name', 'simulation_type', 'maxswap',
    'maxcpuunit', 'maxdiskunit', 'minramunit', 'meancorecount', 'memory_leak_x2',
    'ipconnectivity', 'proddbupdatetime', 'n_inputfiles', 'jobparameters', 'metadata', 'joblabel',
    'jedi_taskname', 'jedi_username', 'jedi_status', 'jedi_modificationtime', 'jedi_creationdate', 'jedi_oldstatus',
    'jedi_frozentime', 'jedi_starttime', 'jedi_taskpriority', 'jedi_endtime', 'jedi_architecture',
    'jedi_transhome', 'jedi_corecount', 'jedi_lockedtime', 'jedi_termcondition', 'jedi_currentpriority', 'jedi_splitrule',
    'jedi_walltime', 'jedi_walltimeunit', 'jedi_site',
    'jedi_statechangetime', 'jedi_outdiskunit', 'jedi_outdiskcount', 'jedi_gshare', 'jedi_workdiskunit',
    'jedi_diskio', 'jedi_workdiskcount', 'jedi_memory_leak_x2', 'jedi_ramcount', 'jedi_attemptnr', 'jedi_ramunit',
    'jedi_iointensity', 'jedi_iointensityunit', 'jedi_progress', 'jedi_failurerate', 'jedi_errordialog', 'jedi_parent_tid',
    'jedi_ticketid', 'jedi_ticketsystemtype', 'jedi_superstatus', 'jedi_campaign', 'jedi_mergeramcount', 'jedi_mergeramunit',
    'jedi_mergewalltime', 'jedi_mergewalltimeunit', 'jedi_throttledtime', 'jedi_numthrottled', 'jedi_mergecorecount', 'jedi_goal',
    'jedi_assessmenttime', 'jedi_cputime', 'jedi_cputimeunit', 'jedi_cpuefficiency', 'jedi_basewalltime', 'jedi_amiflag_old',
    'jedi_amiflag', 'jedi_baseramcount', 'jedi_ttc_requested', 'jedi_ttc_predicted', 'jedi_ttc_predictiondate', 'jedi_rescuetime',
    'jedi_requesttype', 'jedi_usejumbo', 'jedi_diskiounit', 'jedi_memory_leak_core'
]

parsers = []
parsers.append(conversions.StringParser("pilottiming", "|", ["getjob", "stagein", "payload", "stageout", "total_setup"], type='i'))
parsers.append(conversions.StringParser("jobmetrics", " ", "="))
parsers.append(conversions.StringParser("atlasrelease", ".", ["major", "minor", "patch"], cleaninput=["Atlas-"], type='i'))

sel = 'SELECT '
sel += ','.join(columns)
sel += ' FROM ATLAS_PANDA.JOBSARCHIVED4 JOBS LEFT JOIN ATLAS_DEFT.T_PRODUCTION_TASK TASKS'
sel += ' ON JOBS.JEDITASKID = TASKS.TASKID'
sel += ' LEFT JOIN ATLAS_PANDA.JEDI_TASKS JEDI'
sel += ' ON TASKS.TASKID = JEDI.JEDITASKID'
# sel += ' AND PANDAID=4225560422'
# sel += ' AND ROWNUM < 3'
sel += " WHERE JOBS.STATECHANGETIME >= TO_DATE( :start_date, 'YYYY-MM-DD HH24:MI:SS')"
sel += " AND JOBS.STATECHANGETIME < TO_DATE( :end_date, 'YYYY-MM-DD HH24:MI:SS') "

# print(sel)

filecolumns = ['FILES.TYPE', 'FILES.FSIZE', 'FILES.SCOPE', 'FILES.DATASET']

filesel = 'SELECT '
filesel += ','.join(filecolumns)
filesel += ' FROM ATLAS_PANDA.FILESTABLE4 FILES'
filesel += " WHERE FILES.MODIFICATIONTIME != TO_DATE('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')"
filesel += " AND FILES.PANDAID = :pandaid"

cursor.execute(sel, start_date=start_date, end_date=end_date)

es = estools.get_es_connection()

data = []
count = 0
indexname = "-".join([indexbase, start_date.split(" ")[0].replace("-", ".")])
#if not es.indices.exists(indexname):
#    print('Creating new index:', indexname)
#    es.indices.create(index=indexname, ignore=400, body=mapping)
for row in cursor:
    doc = {}
    for colName, colValue in zip(escolumns, row):
        # print(colName, colValue)
        doc[colName] = colValue

    if doc['creationtime']:
        doc['creationtime'] = str(doc['creationtime']).replace(' ', 'T')
    if doc['modificationtime']:
        doc['modificationtime'] = str(
            doc['modificationtime']).replace(' ', 'T')
    if doc['starttime']:
        doc['starttime'] = str(doc['starttime']).replace(' ', 'T')
    if doc['endtime']:
        doc['endtime'] = str(doc['endtime']).replace(' ', 'T')
    doc['cpuconsumptiontime'] = int(doc['cpuconsumptiontime'])
    if doc['statechangetime']:
        doc['statechangetime'] = str(doc['statechangetime']).replace(' ', 'T')
    if doc['jedi_modificationtime']:
        doc['jedi_modificationtime'] = str(
            doc['jedi_modificationtime']).replace(' ', 'T')
    if doc['jedi_starttime']:
        doc['jedi_starttime'] = str(doc['jedi_starttime']).replace(' ', 'T')
    if doc['jedi_endtime']:
        doc['jedi_endtime'] = str(doc['jedi_endtime']).replace(' ', 'T')
    if doc['jedi_statechangetime']:
        doc['jedi_statechangetime'] = str(doc['jedi_statechangetime']).replace(' ', 'T')

    #(doc['dbTime'], doc['dbData'], doc['workDirSize'], doc['jobmetrics']
    # ) = conversions.splitJobmetrics(doc['jobmetrics']) #done below automatically
    (doc['wall_time'], doc['cpu_eff'], doc['queue_time']) = conversions.deriveDurationAndCPUeff(
        doc['creationtime'], doc['starttime'], doc['endtime'], doc['cpuconsumptiontime'])
    doc['walltime_x_core'] = doc['wall_time']
    doc['io_intensity'] = 0.0
    doc['maxpss_per_core'] = 0.0
    doc['walltime_x_core_per_event'] = 0.0
    doc['hs06secperevent'] = 0.0
    if doc['wall_time']!=0.0:
        if isinstance(doc['inputfilebytes'], int):
            doc['io_intensity'] += doc['inputfilebytes']
        if isinstance(doc['outputfilebytes'], int):
            doc['io_intensity'] += doc['outputfilebytes']
        doc['io_intensity'] /= doc['wall_time']
    if isinstance(doc['actualcorecount'], int) and doc['actualcorecount']!=0:
        doc['cpu_eff'] /= doc['actualcorecount']
        doc['walltime_x_core'] *= doc['actualcorecount']
        doc['io_intensity'] /= doc['actualcorecount']
        if doc['maxpss']:
            doc['maxpss_per_core'] = doc['maxpss'] / doc['actualcorecount']
        else:
            doc['maxpss_per_core'] = doc['maxpss']
        if doc['maxrss']:
            doc['maxrss_per_core'] = doc['maxrss'] / doc['actualcorecount']
        else:
            doc['maxrss_per_core'] = doc['maxrss']
    if doc['nevents']!=0.0:
        doc['walltime_x_core_per_event'] = doc['walltime_x_core'] / doc['nevents']
        if doc['hs06sec']:
            doc['hs06secperevent'] = doc['hs06sec'] / doc['nevents']
        else:
            doc['hs06secperevent'] = doc['hs06sec']
    doc['walltime_year'] = doc['wall_time'] / 31536000
    (doc['timeGetJob'], doc['timeStageIn'], doc['timeExe'], doc['timeStageOut'],
     doc['timeSetup']) = conversions.deriveTimes(doc['pilottiming'])
    doc["_index"] = indexname
    doc["_id"] = doc['pandaid']

    for parser in parsers:
        doc.update(parser.parse(doc[parser.source]))
    if doc['modificationhost'] and '@' in doc['modificationhost']:
        doc['modificationhost'] = doc['modificationhost'].split('@')[1]
    input_amitags = conversions.AMItags(doc['proddblock'])
    if len(input_amitags) > 0:
        doc['amitag_input'] = input_amitags[-1]
    output_amitags = conversions.AMItags(doc['destinationdblock'])
    if len(output_amitags) > 0:
        doc['amitag_output'] = output_amitags[-1]

    #doc['logfilesize'] = 0
    inputtypes = []
    outputtypes = []
    inputscopes = []
    outputscopes = []
    undefined_types = []
    filecursor = con.cursor()
    filecursor.execute(filesel, pandaid=doc['pandaid'])
    for filerow in filecursor:
        filedoc = {}
        for colName, colValue in zip(filecolumns, filerow):
            filedoc[colName] = colValue.split('.')[0] if colName == 'FILES.LFN' else colValue
        datatype = conversions.datatype_from_dataset(filedoc['FILES.DATASET'], filedoc['FILES.TYPE'])
        if datatype=='log':
            doc['logfilesize'] = filedoc['FILES.FSIZE']
            continue
        datatype = conversions.datatype_from_dataset(filedoc['FILES.DATASET'], filedoc['FILES.TYPE'])
        if datatype=="UNDEFINED" and filedoc['FILES.DATASET'] not in undefined_types:
            undefined_types.append(filedoc['FILES.DATASET'])
        if filedoc['FILES.TYPE']=='input':
            if datatype not in inputtypes:
                inputtypes.append(datatype)
            if filedoc['FILES.SCOPE'] not in inputscopes:
                inputscopes.append(filedoc['FILES.SCOPE'])
            continue
        if filedoc['FILES.TYPE']=='output':
            if datatype not in outputtypes:
                outputtypes.append(datatype)
            if filedoc['FILES.SCOPE'] not in outputscopes:
                outputscopes.append(filedoc['FILES.SCOPE'])
            continue
    inputtypes.sort()
    inputscopes.sort()
    outputtypes.sort()
    outputscopes.sort()
    undefined_types.sort()
    doc['inputfiletype'] = ','.join(inputtypes)
    doc['inputscope'] = ','.join(inputscopes)
    doc['outputfiletype'] = ','.join(outputtypes)
    doc['outputscope'] = ','.join(outputscopes)
    doc['undefined_filetype_datasets'] = ','.join(undefined_types)

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
estools.clean_up_oldest_by_diskusage(es, indexbase+"*", 10.0)


con.close()
