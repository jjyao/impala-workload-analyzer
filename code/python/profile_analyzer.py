import sys
sys.path.append('gen-py')

import re
import time
import struct
import hashlib
import pymongo
import datetime
from RuntimeProfile.ttypes import *

class ProfileAnalyzer:
    def __init__(self):
        self.db = pymongo.MongoClient().impala
        self.db.fragments.ensure_index(
                [('query_id', pymongo.ASCENDING), ('id', pymongo.ASCENDING)],
                unique=True)

        self.db.operators.ensure_index(
                [('query_id', pymongo.ASCENDING), ('id', pymongo.ASCENDING)],
                unique=True)

    def analyze(self, profileTree, tag):
        if 'ExecSummary' not in profileTree.nodes[1].info_strings:
            # skip queries like 'GET_SCHEMAS'
            return

        operators = {}
        fragments = {}
        queryId = self.db.queries.insert({})

        for line in profileTree.nodes[1].info_strings['ExecSummary'].split('\n')[3:]:
            match = re.match(
                '^[^0-9]*(?P<id>[0-9]+):(?P<name>[A-Z\- ]+?)\s+(?P<num_hosts>[0-9]+)\s+(?P<avg_time>[0-9.hmsun]+)\s+(?P<max_time>[0-9.hmsun]+)\s+(?P<num_rows>[0-9.BMK]+)\s+(?P<est_num_rows>[0-9.\-BMK]+)\s+(?P<peak_mem>[0-9.]+( [GMKB]+)?)\s+(?P<est_peak_mem>[0-9.\-]+( [GMKB]+)?)\s+(?P<detail>.*)$',
                line)
            operator = {
                'id': int(match.group('id')),
                'query_id': queryId,
                'name': match.group('name'),
                'num_hosts': int(match.group('num_hosts')),
                'avg_time': self.prettyPrintTimeToNanoseconds(match.group('avg_time')),
                'max_time': self.prettyPrintTimeToNanoseconds(match.group('max_time')),
                'num_rows': self.prettyPrintNumberToUnits(match.group('num_rows')),
                'est_num_rows': self.prettyPrintNumberToUnits(match.group('est_num_rows')),
                'peak_mem': self.prettyPrintSizeToBytes(match.group('peak_mem')),
                'est_peak_mem': self.prettyPrintSizeToBytes(match.group('est_peak_mem')),
                'detail': match.group('detail').strip()
            }
            operators[operator['id']] = operator

        prevOperator = None
        currOperator = None
        parentOperators = {0: None}
        iterator = iter(profileTree.nodes[1].info_strings['Plan'].split('\n'))
        while True:
            try:
                line = iterator.next()
            except StopIteration:
                break

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
                parentOperators = {0: None}
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
                '^\s+(?P<indent>[|\- ]+)?(?P<id>[0-9]+):(?P<name>[A-Z\- ]+?)(\s+\[(?P<detail>.+)\])?\s*$',
                line)
            if match:
                # start of a new operator
                currOperator = operators[int(match.group('id'))]
                if match.group('indent') is None:
                    parentOperator = parentOperators[0]
                elif match.group('indent').endswith('--'):
                    parentOperator = prevOperator
                else:
                    parentOperator = parentOperators[len(match.group('indent'))]
                currOperator.update({
                    'fragment_id': fragment['id'],
                    'parent_id': None if parentOperator is None else parentOperator['id'],
                })

                if parentOperator is not None and parentOperator['name'] in ('HASH JOIN', 'CROSS JOIN', 'UNION'):
                    # right child first
                    if 'right_child_id' not in parentOperator:
                        parentOperator['right_child_id'] = currOperator['id']
                    else:
                        parentOperator['left_child_id'] = currOperator['id']

                if match.group('name') == 'SCAN HDFS':
                    currOperator.update({
                        'table': re.split(' |,', match.group('detail'))[0],
                    })
                elif match.group('name') == 'HASH JOIN':
                    currOperator.update({
                        'join_type': re.split(', ', match.group('detail'))[0],
                        'join_impl': re.split(', ', match.group('detail'))[1],
                    })
                elif match.group('name') == 'CROSS JOIN':
                    currOperator.update({
                        'join_impl': match.group('detail'),
                    })

                prevOperator = currOperator
                if match.group('indent') is None:
                    parentOperators[0] = currOperator
                else:
                    parentOperators[len(match.group('indent'))] = currOperator

                continue

            match = re.match(
                '^\s+\|?\s+tuple-ids=(?P<tuple_ids>[0-9,]+) row-size=(?P<row_size>[0-9.]+[GMKB]+) cardinality=(?P<cardinality>[0-9]+|unavailable)\s*$',
                line)
            if match:
                cardinality = match.group('cardinality')
                currOperator.update({
                    'cardinality': -1L if cardinality == 'unavailable' else long(cardinality),
                    'row_size': self.prettyPrintSizeToBytes(match.group('row_size')),
                })
                continue

            match = re.match(
                '^\s+partitions=(?P<partitions>[0-9]+/[0-9]+) files=(?P<files>[0-9]+) size=(?P<size>[0-9.]+[GMKB]+)\s*$',
                line)
            if match:
                currOperator.update({
                    'size': self.prettyPrintSizeToBytes(match.group('size')),
                })
                continue

        isCoordinatorFragment = None
        isAveragedFragment = None
        currFragment = None
        for profileNode in profileTree.nodes:
            match = re.match('^Coordinator Fragment F(?P<id>[0-9]+)$', profileNode.name)
            if match:
                isCoordinatorFragment = True
                isAveragedFragment = False
                currFragment = fragments[int(match.group('id'))]
                continue

            match = re.match('^Averaged Fragment F(?P<id>[0-9]+)$', profileNode.name)
            if match:
                isCoordinatorFragment = False
                isAveragedFragment = True
                currFragment = fragments[int(match.group('id'))]
                continue

            match = re.match('^Fragment F(?P<id>[0-9]+)$', profileNode.name)
            if match:
                isCoordinatorFragment = False
                isAveragedFragment = False
                currFragment = fragments[int(match.group('id'))]
                continue

            match = re.match('^CodeGen$', profileNode.name)
            if match:
                if isAveragedFragment:
                    currFragment['avg_code_gen'] = {}
                    for counter in profileNode.counters:
                        currFragment['avg_code_gen'][counter.name] = self.getCounterValue(counter)
                else:
                    if 'code_gen' not in currFragment:
                        currFragment['code_gen'] = {}
                    for counter in profileNode.counters:
                        if counter.name not in currFragment['code_gen']:
                            currFragment['code_gen'][counter.name] = []
                        currFragment['code_gen'][counter.name].append(self.getCounterValue(counter))
                continue

            match = re.match('^(?P<name>.+_NODE) \(id=(?P<id>[0-9]+)\)$', profileNode.name)
            if match:
                operator = operators[int(match.group('id'))]
                if isAveragedFragment:
                    operator['avg_counters'] = {}
                    for counter in profileNode.counters:
                        operator['avg_counters'][counter.name] = self.getCounterValue(counter)
                else:
                    if 'info' not in operator:
                        operator['info'] = {}
                    if 'counters' not in operator:
                        operator['counters'] = {}
                    for key, value in profileNode.info_strings.iteritems():
                        if key not in operator['info']:
                            operator['info'][key] = []
                        operator['info'][key].append(value)
                    for counter in profileNode.counters:
                        if counter.name not in operator['counters']:
                            operator['counters'][counter.name] = []
                        operator['counters'][counter.name].append(self.getCounterValue(counter))
                continue

        for operator in operators.itervalues():
            self.checkOperatorConsistency(operator)
            self.checkJoinOperator(operator, operators)

        for fragment in fragments.itervalues():
            self.checkFragmentConsistency(fragment)

        for operator in operators.itervalues():
            self.db.operators.insert(operator)

        for fragment in fragments.itervalues():
            self.db.fragments.insert(fragment)

        hosts = re.findall('(?P<host>[^() ]+:[0-9]+)', \
                profileTree.nodes[3].info_strings['Per Node Peak Memory Usage'])
        hosts.sort()

        hdfsScans = self.db.operators.find({'query_id': queryId, 'name': 'SCAN HDFS'})
        query = {
            'tag': tag,
            'sql': profileTree.nodes[1].info_strings['Sql Statement'],
            'runtime': profileTree.nodes[1].event_sequences[0].timestamps[-1], # nanoseconds
            # Start execution + Planning finished
            'plan_time': profileTree.nodes[1].event_sequences[0].timestamps[1],
            # Ready to start remote fragments + Remote fragments started
            'fragment_start_time': profileTree.nodes[1].event_sequences[0].timestamps[3] - \
                    profileTree.nodes[1].event_sequences[0].timestamps[1],
            'start_time': self.datetimeToMicroseconds(datetime.datetime.strptime(profileTree.nodes[1].info_strings['Start Time'], \
                                        '%Y-%m-%d %H:%M:%S.%f000')),
            'end_time': self.datetimeToMicroseconds(datetime.datetime.strptime(profileTree.nodes[1].info_strings['End Time'], \
                                        '%Y-%m-%d %H:%M:%S.%f000')),
            'hosts': hosts,
            'cluster': hashlib.md5(' '.join(hosts)).hexdigest(),
            'num_hosts': max([operator['num_hosts'] for operator in operators.itervalues()]),
            'num_hdfs_scans': hdfsScans.count(),
            'num_tables': len(hdfsScans.distinct('table'))
        }

        assert len(query['hosts']) >= query['num_hosts']

        self.db.queries.update(
            {'_id': queryId},
            {'$set': query}
        )

    def datetimeToMicroseconds(self, datetime):
        return long(time.mktime(datetime.timetuple()) * 1e6) + datetime.microsecond

    def prettyPrintSizeToBytes(self, size):
        """ https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/util/pretty-printer.h
            convert a pretty printed size to bytes
            special cases: "0", "-1.00 B"
            "278.73 KB" => 285419
        """
        match = re.match(
            '^((?P<GB>[0-9.]+) ?GB)?((?P<MB>[0-9.]+) ?MB)?((?P<KB>[0-9.]+) ?KB)?((?P<B>-?[0-9.]+) ?B)?(0)?$',
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

    def prettyPrintNumberToUnits(self, number):
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

    def prettyPrintTimeToNanoseconds(self, time):
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

    def getCounterValue(self, counter):
        # https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/util/pretty-printer.h
        # reinterpret long as double
        if counter.type == TCounterType.DOUBLE_VALUE:
            return struct.unpack('d', struct.pack('q', counter.value))[0]
        else:
            return long(counter.value)

    def checkOperatorConsistency(self, operator):
        if 'avg_counters' not in operator:
            return

        for key, value in operator['avg_counters'].iteritems():
            # these two counters are ignored for averages
            # https://github.com/cloudera/Impala/blob/cdh5-trunk/be/src/util/runtime-profile.cc
            if key in ('InactiveTotalTime', 'AsyncTotalTime'):
                continue

            if value != (sum(operator['counters'][key]) / len(operator['counters'][key])):
                print '%s %s %s %s %s' % (operator['name'], operator['id'], key, value, operator['counters'][key])

    def checkFragmentConsistency(self, fragment):
        if 'avg_code_gen' not in fragment:
            return

        for key, value in fragment['avg_code_gen'].iteritems():
            if value != (sum(fragment['code_gen'][key]) / len(fragment['code_gen'][key])):
                print '%s %s %s %s' % (fragment['id'], key, value, fragment['code_gen'][key])

    def checkJoinOperator(self, operator, operators):
        if operator['name'] not in ('HASH JOIN', 'CROSS JOIN'):
            return

        left_child = operators[operator['left_child_id']]
        right_child = operators[operator['right_child_id']]

        # check if impala uses the wrong join implementation
        broadcast_join_cost = min(left_child['num_rows'] * left_child['row_size'],
                right_child['num_rows'] * right_child['row_size']) * operator['num_hosts']
        partitioned_join_cost = left_child['num_rows'] * left_child['row_size'] + \
                right_child['num_rows'] * right_child['row_size']
        if broadcast_join_cost < partitioned_join_cost:
            if operator['join_impl'] != 'BROADCAST':
                print 'BAD JOIN IMPLEMENTATION %s' % operator['id']
                return
        else:
            if operator['join_impl'] != 'PARTITIONED':
                print 'BAD JOIN IMPLEMENTATION %s' % operator['id']
                return

        if operator['join_impl'] == 'BROADCAST':
            # http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/latest/topics/impala_perf_joins.html
            if left_child['num_rows'] * left_child['row_size'] < right_child['num_rows'] * right_child['row_size']:
                print 'BAD BROADCAST JOIN %s' % operator['id']
