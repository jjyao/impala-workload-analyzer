import sys
sys.path.append('gen-py')

import re
import zlib
import base64
import pymongo
from RuntimeProfile.ttypes import *
from thrift.protocol import TCompactProtocol

def prettyPrintSizeToBytes(size):
    """ https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/util/pretty-printer.h
        convert a pretty printed size to bytes
        special cases: "0", "-1.00 B"
        "278.73 KB" => 285419
    """
    match = re.match(
        '^((?P<GB>[0-9.]+) GB)?((?P<MB>[0-9.]+) MB)?((?P<KB>[0-9.]+) KB)?((?P<B>-?[0-9.]+) B)?(0)?$',
        size)
    # bytes is a built-in function name
    bytees = 0.0
    if match.group('GB') is not None:
        bytees = bytees + float(match.group('GB')) * 1024 * 1024 * 1024
    if match.group('MB') is not None:
        bytees = bytees + float(match.group('MB')) * 1024 * 1024
    if match.group('KB') is not None:
        bytees = bytees + float(match.group('KB')) * 1024
    if match.group('B') is not None:
        bytees = bytees + float(match.group('B'))
    return long(bytees)

def prettyPrintNumberToUnits(number):
    """ https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/util/pretty-printer.h
        convert a pretty printed number to units
        special case: "-1"
        "2.85K" => 2850L
    """
    match = re.match(
        '^((?P<B>[0-9.]+)B)?((?P<M>[0-9.]+)M)?((?P<K>[0-9.]+)K)?((?P<S>-?[0-9.]+))?$',
        number)
    units = 0.0
    if match.group('B') is not None:
        units = units + float(match.group('B')) * 10 ** 9
    if match.group('M') is not None:
        units = units + float(match.group('M')) * 10 ** 6
    if match.group('K') is not None:
        units = units + float(match.group('K')) * 10 ** 3
    if match.group('S') is not None:
        units = units + float(match.group('S'))
    return long(units)

def prettyPrintTimeToNanoSeconds(time):
    """ https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/util/pretty-printer.h
        convert a pretty printed time to nanoseconds
        "795.202us" => 795202L
    """
    match = re.match(
        '^((?P<h>[0-9.]+)h)?((?P<m>[0-9.]+)m)?((?P<s>[0-9.]+)s)?((?P<ms>[0-9.]+)ms)?((?P<us>[0-9.]+)us)?((?P<ns>[0-9.]+)ns)?$',
        time)
    nanoseconds = 0.0
    if match.group('h') is not None:
        nanoseconds = nanoseconds + float(match.group('h')) * 60 * 60 * 10 ** 9
    if match.group('m') is not None:
        nanoseconds = nanoseconds + float(match.group('m')) * 60 * 10 ** 9
    if match.group('s') is not None:
        nanoseconds = nanoseconds + float(match.group('s')) * 10 ** 9
    if match.group('ms') is not None:
        nanoseconds = nanoseconds + float(match.group('ms')) * 10 ** 6
    if match.group('us') is not None:
        nanoseconds = nanoseconds + float(match.group('us')) * 10 ** 3
    if match.group('ns') is not None:
        nanoseconds = nanoseconds + float(match.group('ns'))
    return long(nanoseconds)

with open(sys.argv[1], 'r') as profileFile:
    profileData = zlib.decompress(base64.b64decode(profileFile.read()))

memoryBuffer = TTransport.TMemoryBuffer(profileData)
compactProtocol = TCompactProtocol.TCompactProtocol(memoryBuffer)
profileTree = TRuntimeProfileTree()
profileTree.read(compactProtocol)

db = pymongo.MongoClient().impala
db.fragments.ensure_index(
        [('query_id', pymongo.ASCENDING), ('id', pymongo.ASCENDING)],
        unique=True)

db.operators.ensure_index(
        [('query_id', pymongo.ASCENDING), ('id', pymongo.ASCENDING)],
        unique=True)

queryId = db.queries.insert({'sql': profileTree.nodes[1].info_strings['Sql Statement']})

operators = {}
for line in profileTree.nodes[1].info_strings['ExecSummary'].split('\n')[3:]:
    match = re.match(
        '^[^0-9]*(?P<id>[0-9]+):(?P<name>[A-Z\- ]+?)\s+(?P<num_hosts>[0-9]+)\s+(?P<avg_time>[0-9.hmsun]+)\s+(?P<max_time>[0-9.hmsun]+)\s+(?P<num_rows>[0-9.BMK]+)\s+(?P<est_num_rows>[0-9.\-BMK]+)\s+(?P<peak_mem>[0-9.]+( [GMKB]+)?)\s+(?P<est_peak_mem>[0-9.\-]+( [GMKB]+)?)\s+(?P<detail>.*)$',
        line)
    operator = {
        'id': int(match.group('id')),
        'name': match.group('name'),
        'num_hosts': int(match.group('num_hosts')),
        'avg_time': prettyPrintTimeToNanoSeconds(match.group('avg_time')),
        'max_time': prettyPrintTimeToNanoSeconds(match.group('max_time')),
        'num_rows': prettyPrintNumberToUnits(match.group('num_rows')),
        'est_num_rows': prettyPrintNumberToUnits(match.group('est_num_rows')),
        'peak_mem': prettyPrintSizeToBytes(match.group('peak_mem')),
        'est_peak_mem': prettyPrintSizeToBytes(match.group('est_peak_mem')),
        'detail': match.group('detail').strip()
    }
    operators[operator['id']] = operator

prev_fragment = None
curr_fragment = None
prev_operator = None
curr_operator = None
for line in profileTree.nodes[1].info_strings['Plan'].split('\n'):
    match = re.match(
        '^F(?P<id>[0-9]+):PLAN FRAGMENT \[.+\]\s*$',
        line)
    if match:
        if curr_fragment is not None:
            db.operators.insert(curr_operator)
            db.fragments.insert(curr_fragment)
        # start of a new fragment
        curr_fragment = {
            'id': int(match.group('id')),
            'query_id': queryId,
            'parent_id': None if prev_fragment is None else prev_fragment['id'],
        }
        prev_fragment = curr_fragment
        prev_operator = None
        curr_operator = None
        continue

    match = re.match(
        '^\s+(?P<indent>\|-+)?(?P<id>[0-9]+):(?P<name>[A-Z\- ]+?)(\s+\[.+\])?\s*$',
        line)
    if match:
        if curr_operator is not None:
            db.operators.insert(curr_operator)
        # start of a new operator
        curr_operator = operators[int(match.group('id'))]
        curr_operator.update({
            'query_id': queryId,
            'fragment_id': curr_fragment['id'],
            'parent_id': None if prev_operator is None else prev_operator['id'],
        })
        if (match.group('indent')) is None:
            prev_operator = curr_operator
        continue
db.operators.insert(curr_operator)
db.fragments.insert(curr_fragment)
