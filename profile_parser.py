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

fragments = {}
prevOperator = None
currOperator = None
for line in profileTree.nodes[1].info_strings['Plan'].split('\n'):
    match = re.match(
        '^F(?P<id>[0-9]+):PLAN FRAGMENT \[.+\]\s*$',
        line)
    if match:
        # start of a new fragment
        fragment = {
            'id': int(match.group('id')),
            'query_id': queryId,
            'exchange_id': None,
        }
        fragments[fragment['id']] = fragment
        prevOperator = None
        currOperator = None
        continue

    match = re.match(
        '^\s+DATASTREAM SINK \[FRAGMENT=F(?P<fragment_id>[0-9]+), EXCHANGE=(?P<exchange_id>[0-9]+), (?P<detail>.*)\]\s*$',
        line)
    if match:
        fragment.update({
            'exchange_id': int(match.group('exchange_id')),
        })
        continue

    match = re.match(
        '^\s+(?P<indent>\|-+)?(?P<id>[0-9]+):(?P<name>[A-Z\- ]+?)(\s+\[(?P<detail>.+)\])?\s*$',
        line)
    if match:
        # start of a new operator
        currOperator = operators[int(match.group('id'))]
        currOperator.update({
            'query_id': queryId,
            'fragment_id': fragment['id'],
            'parent_id': None if prevOperator is None else prevOperator['id'],
        })

        if match.group('name') == 'SCAN HDFS':
            currOperator.update({
                'table': re.split(' |,', match.group('detail'))[0],
            })

        if (match.group('indent')) is None:
            prevOperator = currOperator
        continue

for profileNode in profileTree.nodes:
    match = re.match('^HDFS_SCAN_NODE \(id=(?P<id>[0-9]+)\)$', profileNode.name)
    if match:
        operator = operators[int(match.group('id'))]
        if 'File Formats' in profileNode.info_strings:
            operator.update({
                'file_formats': profileNode.info_strings['File Formats'],
            })
        continue

for operator in operators.itervalues():
    db.operators.insert(operator)

for fragment in fragments.itervalues():
    db.fragments.insert(fragment)

hdfsScans = db.operators.find({'query_id': queryId, 'name': 'SCAN HDFS'})
db.queries.update(
    {'_id': queryId},
    {'$set': {
        'num_hdfs_scans': hdfsScans.count(),
        'num_tables': len(hdfsScans.distinct('table'))}})
