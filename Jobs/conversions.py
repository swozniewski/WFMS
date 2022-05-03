from datetime import datetime
import time


def strToTS(d):
    (dat, tim) = d.split('T')
    (Y, M, D) = dat.split('-')
    (h, m, s) = tim.split(':')
    t = datetime(int(Y), int(M), int(D), int(h), int(m), int(float(s)))
    return t


def deriveTimes(origString):
    if origString is None:
        return (0, 0, 0, 0, 0)
    times = origString.split('|')
    if len(times) == 4:
        times.append(0)
    return (int(float(times[0])), int(float(times[1])), int(float(times[2])), int(float(times[3])), int(float(times[4])))


def splitJobmetrics(origString):
    if origString is None:
        return (0, 0, 0, None)
    parts = origString.split(' ')
    rest = ''
    dbTime = None
    dbData = None
    workDirSize = None
    for p in parts:
        if not '=' in p:
            rest += p + ' '
            continue
        k, v = p.split('=')
        if k == 'coreCount' or k == 'nEvents':
            continue
        if k == 'dbTime':
            try:
                dbTime = float(v)
            except:
                pass
                # print('issue with dbTime:', v)
            continue
        if k == 'dbData':
            try:
                dbData = int(v)
            except:
                pass
                # print('issue with dbData:', v)
            continue
        if k == 'workDirSize':
            try:
                workDirSize = int(v)
            except:
                pass
                # print('issue with workDirSize:', v)
            continue
        rest += p + ' '
    rest = rest.strip()
    if len(rest) == 0:
        rest = None
    return (dbTime, dbData, workDirSize, rest)


def deriveDurationAndCPUeff(CREATIONTIME, STARTTIME, ENDTIME, CPUCONSUMPTIONTIME):
    if CREATIONTIME is None or STARTTIME is None or ENDTIME is None or CPUCONSUMPTIONTIME is None:
        return (0, 0.0, 0)
    CREATIONTIME = strToTS(CREATIONTIME)
    STARTTIME = strToTS(STARTTIME)
    ENDTIME = strToTS(ENDTIME)

    wt = ENDTIME - STARTTIME
    qt = STARTTIME - CREATIONTIME
    walltime = wt.seconds + wt.days * 86400
    queue_time = qt.seconds + qt.days * 86400

    cpueff = 0
    try:
        if walltime > 0 and CPUCONSUMPTIONTIME != '':
            cpueff = float(CPUCONSUMPTIONTIME) / walltime
    except:
        print("problem with cpueff: " + CPUCONSUMPTIONTIME)

    return (walltime, cpueff, queue_time)


def Tstamp(ts):
    if ts is None:
        return(0)
    else:
        return(datetime.fromtimestamp(ts / 1000).isoformat())


def TstampNEW(ts):
    if ts is None:
        return(0)
    else:
        d = strToTS(ts)
        return(int(1000 * time.mktime(d.timetuple())))

def filetype_from_dataset(ds, iotype):
    if iotype == "log":
        return "log"
    if ".AOD." in ds:
        return "AOD"
    if ".DAOD_" in ds:
        return "DAOD_" + ds.split(".DAOD_")[1].split(".")[0]
    if ".DESDM_" in ds:
        return "DESDM_" + ds.split(".DESDM_")[1].split(".")[0]
    if ".DRAW." in ds:
        return "DRAW"
    if ".DRAW_" in ds:
        return "DRAW_" + ds.split(".DRAW_")[1].split(".")[0]
    if ".ESD." in ds:
        return "ESD"
    if ".EVNT." in ds:
        return "EVNT"
    if ".HIST." in ds:
        return "HIST"
    if ".HIST_HLTMON." in ds:
        return "HIST_HLTMON"
    if ".HITS." in ds:
        return "HITS"
    if ".NTUP_" in ds:
        return "NTUP_" + ds.split(".NTUP_")[1].split(".")[0]
    if ".RDO." in ds:
        return "RDO"
    if ".RAW." in ds or ds.endswith(".RAW"):
        return "RAW"
    if ".TXT." in ds:
        return "TXT"
    if ".log" in ds:
        return "log"
    if ".lib." in ds:
        return "lib"
    if "hc_test" in ds:
        return "hc_test"
    if True: #iotype == "output":
        if "mmascher" in ds:
            return "mmascher"
        if "user" in ds:
            return "user"
        if "group.art" in ds:
            return "group.art"
        if "group10.perf-tau" in ds:
            return "group10.perf-tau"
        if "group.det-indet" in ds:
            return "group.det-indet"
        if "group.det-muon" in ds:
            return "group.det-muon"
        if "group.perf-egamma" in ds:
            return "group.perf-egamma"
        if "group.perf-gener" in ds:
            return "group.perf-gener"
        if "group.perf-muons" in ds:
            return "group.perf-muons"
        if "group.phys-exotics" in ds:
            return "group.phys-exotics"
        if "group.phys-gener" in ds:
            return "group.phys-gener"
        if "group.phys-hdbs" in ds:
            return "group.phys-hdbs"
        if "group.phys-higgs" in ds:
            return "group.phys-higgs"
        if "group.phys-sm" in ds:
            return "group.phys-sm"
        #cover all other groups as well
        if "group." in ds:
            return "group." + ds.split("group.")[1].split(".")[0]
        if "destDB" in ds:
            return "destDB"
    if ds.startswith("panda.um"):
        return "panda"
    if ds.startswith("ddo"):
        return "ddo"
    return "UNDEFINED"
