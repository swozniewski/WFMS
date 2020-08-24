#!/usr/bin/env python3

# this code looks up older jobs in merging state in ES
# find them in ATLAS_PANDAARCH.JOBSARCHIVED and updates state in ES

import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
import cx_Oracle

username = os.environ['JOB_ORACLE_USER']
password = os.environ['JOB_ORACLE_PASS']
# JOB_ORACLE_CONNECTION_STRING or JOB_ORACLE_ADG_CONNECTION_STRING
server = os.environ['JOB_ORACLE_CONNECTION_STRING'].replace(
    "jdbc:oracle:thin:@//", "")

connString = username + "/" + password + "@" + server


con = cx_Oracle.connect(connString)
print('Oracle version:', con.version)

es = Elasticsearch([{'host': 'atlas-kibana.mwt2.org',
                     'port': 9200, 'scheme': 'https'}], timeout=60)


mergin_job_query = {
    "size": 0,
    "version": True,
    "_source": ["_id"],
    "query": {
        "term": {"jobstatus": "merging"}
    }
}

res = scan(client=es, index='jobs', query=mergin_job_query,
           scroll='5m', timeout="5m", size=10000)

counter = 0
skipped = 0
jobs = {}
jbs = ''
new_statuses = {}

for job in res:
    counter += 1
    if not counter % 1000:
        print("scanned: ", counter, "\tskipped:",
              skipped, '\tnew statuses:', new_statuses)
    # print(job)
    # if counter > 1000:
        # break
    if (job['_version']) > 10:
        skipped += 1
        continue

    jobs[int(job['_id'])] = job['_index']
    jbs += str(job['_id'] + ',')

    if len(jobs) > 900:
        jbs = jbs[:-1]

        # geting info from oracle
        cur = con.cursor()
        cur.execute(
            'SELECT pandaid, jobstatus FROM ATLAS_PANDAARCH.JOBSARCHIVED WHERE PANDAID IN(' + jbs + ')')

        lookedup = []
        for result in cur:
            # print(result)
            ns = result[1]
            if ns not in new_statuses:
                new_statuses[ns] = 0
            new_statuses[ns] += 1
            lookedup.append(result)

        cur.close()

        # do ES update
        data = []
        for pid, jstatus in lookedup:
            if jstatus == 'merging':
                continue
            d = {
                '_op_type': 'update',
                '_index': jobs[pid],
                '_type': 'jobs_data',
                '_id': pid,
                'doc': {'jobstatus': jstatus}
            }
            data.append(d)

        status = bulk(client=es, actions=data, stats_only=True, timeout="5m")
        print(status)

        jobs = {}
        jbs = ''


con.close()
print('Done.')
