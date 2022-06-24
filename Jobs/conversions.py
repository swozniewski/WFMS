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

def datatype_from_dataset(ds, iotype):
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

def isAMItag(probe):
    for key in ['e', 's', 'a', 'r', 'p']:
        if probe.startswith(key) and probe.replace(key, "").isnumeric():
            return True
    return False

def AMItags(source):
    amitags = []
    if not source:
        return amitags
    entries = source.split("_")
    for entry in entries:
        if isAMItag(entry):
            amitags.append(entry)
    return amitags

class StringParser:
    def __init__(self, source, split_char, keydef, mask=None, cleaninput=[], type='auto'):
        self.print_once = True
        self.source = source
        self.split_char = split_char
        self.mask = mask
        self.cleaninput = cleaninput
        self.type = type
        if isinstance(keydef, list):
            self.readkeys = False
            self.keys = keydef
            self.key_split_char = None
        else:
            self.readkeys = True
            self.keys = None
            self.key_split_char = keydef

    def checktype(self, val, type):
        val_out = None
        if type=='auto':
            if val.isnumeric():
                val_out = float(val)
            else:
                val_out = str(val)
        elif type=='i':
            if val.isnumeric() and int(val)==float(val):
                val_out = int(val)
        elif type=='f':
            if val.isnumeric():
                val_out = float(val)
        elif type=='s':
            val_out = str(val)
        return val_out

    def parse(self, input):
        output = {}
        if not input:
            return output
        for entry in self.cleaninput:
            input = input.replace(entry, "")
        if not self.split_char in input:
            return output
        items = input.split(self.split_char)
        for i, item in enumerate(items):
            if self.mask:
                if i >= len(mask) or not mask[i]:
                    continue
            if self.readkeys:
                keyvalue = item.split(self.key_split_char)
                val = self.checktype(keyvalue[1], self.type)
                if val!=None:
                    output["_".join([self.source, keyvalue[0]])] = val
            else:
                if i >= len(self.keys):
                    self.keys.append("dummy%i"%i)
                val = self.checktype(item, self.type)
                if val!=None:
                    output["_".join([self.source, self.keys[i]])] = val
        if self.print_once:
            self.print_once = False
            print("Splitting %s = %s"%(self.source, input))
            print(output)
        return output
