import os
import time
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers


def get_es_connection():
    """
    establishes es connection.
    """
    print("make sure we are connected to ES...")
    try:
        if 'ES_USER' in os.environ and 'ES_PASS' in os.environ and 'ES_HOST' in os.environ:
            es_conn = Elasticsearch(
                os.environ['ES_HOST'],
                http_auth=(os.environ['ES_USER'], os.environ['ES_PASS']),
                ca_certs="/etc/pki/tls/certs/ca-bundle.trust.crt"
            )
        else:
            es_conn = Elasticsearch(
                [{'host': 'atlas-kibana.mwt2.org', 'port': 9200, 'scheme': 'https'}])
        print("connected OK!")
    except es_exceptions.ConnectionError as error:
        print('ConnectionError in get_es_connection: ', error)
    except Exception as e:
        print('Something seriously wrong happened in getting ES connection.', e)
    else:
        return es_conn

    time.sleep(70)
    get_es_connection()


def bulk_index(data, es_conn=None, thread_name=''):
    """
    sends the data to ES for indexing.
    if successful returns True.
    """
    success = False
    if es_conn is None:
        es_conn = get_es_connection()
    try:
        res = helpers.bulk(
            es_conn, data, raise_on_exception=True, request_timeout=120)
        print(thread_name, "inserted:", res[0], 'errors:', res[1])
        success = True
    except es_exceptions.ConnectionError as error:
        print('ConnectionError ', error)
    except es_exceptions.TransportError as error:
        print('TransportError ', error)
    except helpers.BulkIndexError as error:
        print(error)
    except Exception as e:
        print('Something seriously wrong happened.', e)

    return success

def clean_up_oldest_by_diskusage(es_conn, field_regex, limit_du):
    success = False
    diskusage = 0.0
    indiceslist = []
    try:
        indicesinfo = es_conn.cat.indices(field_regex)
        for line in indicesinfo.split("\n"):
            entries = line.split()
            if len(entries)==0:
                continue
            if not "active" in entries[2]: #don't potentially delete data from the active data table
                indiceslist.append(entries[2])
            if entries[8].endswith("gb"):
                diskusage += float(entries[8].replace("gb", ""))
            elif entries[8].endswith("mb"):
                diskusage += float(entries[8].replace("mb", "")) / 1000
            elif entries[8].endswith("kb"):
                diskusage += float(entries[8].replace("kb", "")) / 1000000
            else:
                diskusage += float(entries[8].replace("b", "")) / 1000000000
        print("Current disk usage: {}GB. Allowed: {}GB".format(diskusage, limit_du))
        if(diskusage > limit_du):
            indiceslist.sort()
            print("Deleting index {}".format(indiceslist[0]))
            es_conn.indices.delete(index=indiceslist[0])
            success = clean_up_oldest_by_diskusage(es_conn, field_regex, limit_du)
        else:
            success = True
    except es_exceptions.ConnectionError as error:
        print('ConnectionError ', error)
    except es_exceptions.TransportError as error:
        print('TransportError ', error)
    except helpers.BulkIndexError as error:
        print(error)
    except Exception as e:
        print('Something seriously wrong happened.', e)

    return success

def get_diskusage(es_conn, field_regex):
    diskusage = 0.0
    try:
        indicesinfo = es_conn.cat.indices(field_regex)
        for line in indicesinfo.split("\n"):
            entries = line.split()
            if len(entries)==0:
                continue
            if entries[8].endswith("gb"):
                diskusage += float(entries[8].replace("gb", "")) * 1000000000
            elif entries[8].endswith("mb"):
                diskusage += float(entries[8].replace("mb", "")) * 1000000
            elif entries[8].endswith("kb"):
                diskusage += float(entries[8].replace("kb", "")) * 1000
            else:
                diskusage += float(entries[8].replace("b", ""))
    except es_exceptions.ConnectionError as error:
        print('ConnectionError ', error)
    except es_exceptions.TransportError as error:
        print('TransportError ', error)
    except helpers.BulkIndexError as error:
        print(error)
    except Exception as e:
        print('Something seriously wrong happened.', e)

    return diskusage

def remove_index(es_conn, index):
    success = False
    if es_conn is None:
        es_conn = get_es_connection()
    try:
        es_conn.indices.delete(index=index)
        success = True
    except es_exceptions.ConnectionError as error:
        print('ConnectionError ', error)
    except es_exceptions.TransportError as error:
        print('TransportError ', error)
    except Exception as e:
        print('Something seriously wrong happened.', e)

    return success

def remove_existing(es_conn, index_regex, id):
    """
    Checks if id exists and removes such docs.
    if successful returns True.
    """
    success = False
    if es_conn is None:
        es_conn = get_es_connection()
    try:
        doc = es_conn.search(index=index_regex, _source="pandaid", size=20, body={"query" : {"term" : {"_id" : id}}})
        for x in doc["hits"]["hits"]:
            es_conn.delete(x["_index"], id)
        success = True
    except es_exceptions.ConnectionError as error:
        print('ConnectionError ', error)
    except es_exceptions.TransportError as error:
        print('TransportError ', error)
    except Exception as e:
        print('Something seriously wrong happened.', e)

    return success
