import sys
import numpy
import plots
import pymongo
import argparse
from matplotlib import pyplot

db = pymongo.MongoClient().impala

argumentParser = argparse.ArgumentParser()
argumentParser.add_argument('--summary', action='store_true')
argumentParser.add_argument('argv', nargs=2)
args = argumentParser.parse_args()
tag = args.argv[0]
outputDir = args.argv[1]
plots.outputDir = outputDir

queries = db.queries.find({
    'tag': tag,
    'sql.type': {'$in': ['SelectStmt', 'InsertStmt', 'UnionStmt']}})

numJoins = []
numBroadcastJoins = []
numPartitionedJoins = []
numInnerJoins = []
numTables = []
numHdfsScans = []
numFromSubqueries = []
numOutputColumns = []
numGroupByColumns = []
numOrderByColumns = []
numLimits = []
numQueries = queries.count()
hdfsScanSize = []
runtime = []
timePctPerOperator = {}
sumTimePerOperator = {}

for query in queries:
    if not args.summary:
        operators = list(db.operators.find({'query_id': query['_id']}))
        for operator in operators:
            operator['diff_time'] = operator['max_time'] - operator['avg_time']
            operator['diff_time_pct'] = operator['diff_time'] / float(query['runtime'])
        operators = [operator for operator in operators if operator['diff_time_pct'] >= 0.01]
        operators.sort(key=lambda operator: operator['diff_time_pct'], reverse=True)
        pyplot.clf()
        left = numpy.arange(len(operators))
        height = [operator['diff_time_pct'] for operator in operators]
        pyplot.bar(left, height, align = 'center')
        pyplot.xlabel('Operator')
        pyplot.ylabel('Diff Time Pct')
        pyplot.xticks(left, [operator['name'] for operator in operators], rotation='vertical')
        pyplot.title('Diff Time Pct')
        pyplot.tight_layout()
        pyplot.savefig('%s/%s_diff_time_pct.png' % (outputDir, query['_id']))

    operators = db.operators.aggregate([
        {'$match': {'query_id': query['_id']}},
        {'$group': {'_id': '$name', 'sum_time': {'$sum': '$avg_time'}}},
    ])['result']

    codeGenTime = db.fragments.aggregate([
        {'$match': {'query_id': query['_id']}},
        {'$group': {'_id': None, 'sum_time': {'$sum': '$avg_code_gen.TotalTime'}}},
    ])['result'][0]['sum_time']

    hdfsTableSinkTime = db.fragments.aggregate([
        {'$match': {'query_id': query['_id']}},
        {'$group': {'_id': None, 'sum_time': {'$sum': '$avg_hdfs_table_sink.TotalTime'}}},
    ])['result'][0]['sum_time']

    sumTimeAllOperators = float(sum(operator['sum_time'] for operator in operators)) + \
            query['plan_time'] + query['fragment_start_time'] + \
            codeGenTime + hdfsTableSinkTime

    for operator in operators:
        operator['time_pct'] = operator['sum_time'] / sumTimeAllOperators
    # add special operators
    operators.append({
        '_id': 'Plan',
        'sum_time': query['plan_time'],
        'time_pct': query['plan_time'] / sumTimeAllOperators,
    })
    operators.append({
        '_id': 'Fragment Start',
        'sum_time': query['fragment_start_time'],
        'time_pct': query['fragment_start_time'] / sumTimeAllOperators,
    })
    operators.append({
        '_id': 'CodeGen',
        'sum_time': codeGenTime,
        'time_pct': codeGenTime / sumTimeAllOperators,
    })
    operators.append({
        '_id': 'HdfsTableSink',
        'sum_time': hdfsTableSinkTime,
        'time_pct': hdfsTableSinkTime / sumTimeAllOperators,
    })

    if not args.summary:
        operators.sort(key=lambda operator: operator['sum_time'], reverse=True)
        plots.stacked_bar([operator['sum_time'] / 1000000 for operator in operators],
            'Time', ['%s %sms' % (operator['_id'], operator['sum_time'] / 1000000) for operator in operators],
            'Operator Sum Time (ms)',
            '%s_stacked_time.png' % query['_id'])

    for operator in operators:
        if operator['_id'] not in timePctPerOperator:
            timePctPerOperator[operator['_id']] = []
        timePctPerOperator[operator['_id']].append(operator['time_pct'])

        if operator['_id'] not in sumTimePerOperator:
            sumTimePerOperator[operator['_id']] = []
        sumTimePerOperator[operator['_id']].append(operator['sum_time'])

    numJoins.append(
            db.operators.find({
                'query_id': query['_id'],
                'name': {'$in': ['HASH JOIN', 'CROSS JOIN']}
            }).count())

    numBroadcastJoins.append(
            db.operators.find({
                'query_id': query['_id'],
                'name': {'$in': ['HASH JOIN', 'CROSS JOIN']},
                'join_impl': 'BROADCAST'
            }).count())

    numPartitionedJoins.append(
            db.operators.find({
                'query_id': query['_id'],
                'name': {'$in': ['HASH JOIN', 'CROSS JOIN']},
                'join_impl': 'PARTITIONED'
            }).count())

    numInnerJoins.append(
            db.operators.find({
                'query_id': query['_id'],
                'name': 'HASH JOIN',
                'join_type': 'INNER JOIN'
            }).count())

    scanHdfs = db.operators.aggregate([
                {'$match': {'query_id': query['_id'], 'name': 'SCAN HDFS'}},
                {'$group': {'_id': None, 'size': {'$sum': '$size'}}},
            ])['result']
    if scanHdfs:
        hdfsScanSize.append(scanHdfs[0]['size'] / 1024 / 1024)
    else:
        hdfsScanSize.append(0)

    numTables.append(query['num_tables'])

    numHdfsScans.append(query['num_hdfs_scans'])

    runtime.append(query['runtime'] / 1000000000)

    sqlType = query['sql']['type']
    if sqlType in ['SelectStmt', 'UnionStmt']:
        numOutputColumns.append(query['sql']['num_output_columns'])
        numFromSubqueries.append(query['sql']['num_from_subqueries'])
        numGroupByColumns.append(query['sql']['num_group_by_columns'])
        numOrderByColumns.append(query['sql']['num_order_by_columns'])
        numLimits.append(query['sql']['num_limits'])
    elif sqlType == 'InsertStmt':
        assert query['sql']['query']['type'] in ['SelectStmt', 'UnionStmt']
        numOutputColumns.append(query['sql']['query']['num_output_columns'])
        numFromSubqueries.append(query['sql']['query']['num_from_subqueries'])
        numGroupByColumns.append(query['sql']['query']['num_group_by_columns'])
        numOrderByColumns.append(query['sql']['query']['num_order_by_columns'])
        numLimits.append(query['sql']['query']['num_limits'])

minNumJoins = min(numJoins)
maxNumJoins = max(numJoins)
avgNumJoins = sum(numJoins) / float(len(numJoins))
plots.hist(numJoins, minNumJoins, maxNumJoins,
        "Number of Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumJoins, maxNumJoins, avgNumJoins),
        "num_joins_hist.png")
plots.bar(numJoins, minNumJoins, maxNumJoins,
        "Number of Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumJoins, maxNumJoins, avgNumJoins),
        "num_joins_bar.png")

minNumBroadcastJoins = min(numBroadcastJoins)
maxNumBroadcastJoins = max(numBroadcastJoins)
avgNumBroadcastJoins = sum(numBroadcastJoins) / float(len(numBroadcastJoins))
plots.hist(numBroadcastJoins, minNumBroadcastJoins, maxNumBroadcastJoins,
        "Number of Broadcast Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumBroadcastJoins, maxNumBroadcastJoins, avgNumBroadcastJoins),
        "num_broadcast_joins_hist.png")
plots.bar(numBroadcastJoins, minNumBroadcastJoins, maxNumBroadcastJoins,
        "Number of Broadcast Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumBroadcastJoins, maxNumBroadcastJoins, avgNumBroadcastJoins),
        "num_broadcast_joins_bar.png")

minNumPartitionedJoins = min(numPartitionedJoins)
maxNumPartitionedJoins = max(numPartitionedJoins)
avgNumPartitionedJoins = sum(numPartitionedJoins) / float(len(numPartitionedJoins))
plots.hist(numPartitionedJoins, minNumPartitionedJoins, maxNumPartitionedJoins,
        "Number of Partitioned Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumPartitionedJoins, maxNumPartitionedJoins, avgNumPartitionedJoins),
        "num_partitioned_joins_hist.png")
plots.bar(numPartitionedJoins, minNumPartitionedJoins, maxNumPartitionedJoins,
        "Number of Partitioned Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumPartitionedJoins, maxNumPartitionedJoins, avgNumPartitionedJoins),
        "num_partitioned_joins_bar.png")

minNumInnerJoins = min(numInnerJoins)
maxNumInnerJoins = max(numInnerJoins)
avgNumInnerJoins = sum(numInnerJoins) / float(len(numInnerJoins))
plots.hist(numInnerJoins, minNumInnerJoins, maxNumInnerJoins,
        "Number of Inner Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumInnerJoins, maxNumInnerJoins, avgNumInnerJoins),
        "num_inner_joins_hist.png")
plots.bar(numInnerJoins, minNumInnerJoins, maxNumInnerJoins,
        "Number of Inner Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumInnerJoins, maxNumInnerJoins, avgNumInnerJoins),
        "num_inner_joins_bar.png")

minNumTables = min(numTables)
maxNumTables = max(numTables)
avgNumTables = sum(numTables) / float(len(numTables))
plots.hist(numTables, minNumTables, maxNumTables,
        "Number of Tables", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumTables, maxNumTables, avgNumTables),
        "num_tables_hist.png")
plots.bar(numTables, minNumTables, maxNumTables,
        "Number of Tables", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumTables, maxNumTables, avgNumTables),
        "num_tables_bar.png")

minNumHdfsScans = min(numHdfsScans)
maxNumHdfsScans = max(numHdfsScans)
avgNumHdfsScans = sum(numHdfsScans) / float(len(numHdfsScans))
plots.hist(numHdfsScans, minNumHdfsScans, maxNumHdfsScans,
        "Number of HDFS Scans", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumHdfsScans, maxNumHdfsScans, avgNumHdfsScans),
        "num_hdfs_scans_hist.png")
plots.bar(numHdfsScans, minNumHdfsScans, maxNumHdfsScans,
        "Number of HDFS Scans", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumHdfsScans, maxNumHdfsScans, avgNumHdfsScans),
        "num_hdfs_scans_bar.png")

minNumOutputColumns = min(numOutputColumns)
maxNumOutputColumns = max(numOutputColumns)
avgNumOutputColumns = sum(numOutputColumns) / float(len(numOutputColumns))
plots.hist(numOutputColumns, minNumOutputColumns, maxNumOutputColumns,
        "Number of Output Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumOutputColumns, maxNumOutputColumns, avgNumOutputColumns),
        "num_output_columns_hist.png", ylog=True)
plots.bar(numOutputColumns, minNumOutputColumns, maxNumOutputColumns,
        "Number of Output Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumOutputColumns, maxNumOutputColumns, avgNumOutputColumns),
        "num_output_columns_bar.png", ylog=True)

minNumGroupByColumns = min(numGroupByColumns)
maxNumGroupByColumns = max(numGroupByColumns)
avgNumGroupByColumns = sum(numGroupByColumns) / float(len(numGroupByColumns))
plots.hist(numGroupByColumns, minNumGroupByColumns, maxNumGroupByColumns,
        "Number of Group By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumGroupByColumns, maxNumGroupByColumns, avgNumGroupByColumns),
        "num_group_by_columns_hist.png")
plots.bar(numGroupByColumns, minNumGroupByColumns, maxNumGroupByColumns,
        "Number of Group By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumGroupByColumns, maxNumGroupByColumns, avgNumGroupByColumns),
        "num_group_by_columns_bar.png")

minNumOrderByColumns = min(numOrderByColumns)
maxNumOrderByColumns = max(numOrderByColumns)
avgNumOrderByColumns = sum(numOrderByColumns) / float(len(numOrderByColumns))
plots.hist(numOrderByColumns, minNumOrderByColumns, maxNumOrderByColumns,
        "Number of Order By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumOrderByColumns, maxNumOrderByColumns, avgNumOrderByColumns),
        "num_order_by_columns_hist.png")
plots.bar(numOrderByColumns, minNumOrderByColumns, maxNumOrderByColumns,
        "Number of Order By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumOrderByColumns, maxNumOrderByColumns, avgNumOrderByColumns),
        "num_order_by_columns_bar.png")

minHdfsScanSize = min(hdfsScanSize)
maxHdfsScanSize = max(hdfsScanSize)
avgHdfsScanSize = sum(hdfsScanSize) / float(len(hdfsScanSize))
plots.hist(hdfsScanSize, minHdfsScanSize, maxHdfsScanSize,
        "HDFS Scan Size (MB)", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minHdfsScanSize, maxHdfsScanSize, avgHdfsScanSize),
        "hdfs_scan_size.png")

minNumFromSubqueries = min(numFromSubqueries)
maxNumFromSubqueries = max(numFromSubqueries)
avgNumFromSubqueries = sum(numFromSubqueries) / float(len(numFromSubqueries))
plots.hist(numFromSubqueries, minNumFromSubqueries, maxNumFromSubqueries,
        "Number of From Subqueries", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumFromSubqueries, maxNumFromSubqueries, avgNumFromSubqueries),
        "num_from_subqueries_hist.png")
plots.bar(numFromSubqueries, minNumFromSubqueries, maxNumFromSubqueries,
        "Number of From Subqueries", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumFromSubqueries, maxNumFromSubqueries, avgNumFromSubqueries),
        "num_from_subqueries_bar.png")

minNumLimits = min(numLimits)
maxNumLimits = max(numLimits)
avgNumLimits = sum(numLimits) / float(len(numLimits))
plots.hist(numLimits, minNumLimits, maxNumLimits,
        "Number of Limits", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumLimits, maxNumLimits, avgNumLimits),
        "num_limits_hist.png")
plots.bar(numLimits, minNumLimits, maxNumLimits,
        "Number of Limits", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minNumLimits, maxNumLimits, avgNumLimits),
        "num_limits_bar.png")

minRuntime = min(runtime)
maxRuntime = max(runtime)
avgRuntime = sum(runtime) / float(len(runtime))
plots.hist(runtime, minRuntime, maxRuntime,
        "Runtime (s)", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minRuntime, maxRuntime, avgRuntime),
        "runtime.png", ylog=True)
plots.scatter(runtime,
        "Query", "Runtime (s)",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minRuntime, maxRuntime, avgRuntime),
        "runtime_scatter.png")
plots.scatter(runtime,
        "Query", "Runtime (s)",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (minRuntime, maxRuntime, avgRuntime),
        "runtime_scatter_ylog.png", ylog=True)

for name in timePctPerOperator.iterkeys():
    # if an operator doesn't exist in a query, its pct is 0
    timePctPerOperator[name].extend([0.0] * (numQueries - len(timePctPerOperator[name])))

for name in sumTimePerOperator.iterkeys():
    # if an operator doesn't exist in a query, its time is 0
    sumTimePerOperator[name].extend([0] * (numQueries - len(sumTimePerOperator[name])))

avgTimePctPerOperator = dict((name, (sum(pcts) / len(pcts))) for (name, pcts) in timePctPerOperator.items())
plots.pie(
    avgTimePctPerOperator.values(), avgTimePctPerOperator.keys(),
    "Avg of Operator Time Percent", "avg_time_pct_pie.png")

sumTimePerOperator = dict((name, sum(sumTimes)) for (name, sumTimes) in sumTimePerOperator.items())
sumTimeAllOperators = float(sum(sumTimePerOperator.itervalues()))
absTimePctPerOperator = dict((name, (time / sumTimeAllOperators)) for (name, time) in sumTimePerOperator.items())
plots.pie(
    absTimePctPerOperator.values(), absTimePctPerOperator.keys(),
    "Operator Time Percent", "abs_time_pct_pie.png")

sumTimePerOperator = sumTimePerOperator.items()
sumTimePerOperator.sort(key=lambda operator: operator[1], reverse=True)
plots.stacked_bar([operator[1] / 1000000 for operator in sumTimePerOperator],
        "Time", ['%s %sms' % (operator[0], operator[1] / 1000000) for operator in sumTimePerOperator],
        "Operator Sum Time (ms)",
        "stacked_time.png")

clusters = db.queries.find({'tag': tag}).distinct('cluster')
for cluster in clusters:
    queries = db.queries.find({'cluster': cluster}, ['start_time', 'end_time'])
    times = []
    for query in queries:
        times.append((query['start_time'], 1))
        times.append((query['end_time'], -1))
    times.sort()
    maxNumConcurrentQueries = 1
    currNumConcurrentQueries = 1
    sumQueryMicroseconds = 0
    sumNumConcurrentQueries = 0
    for i in xrange(1, len(times)):
        interval = times[i][0] - times[i-1][0]
        if currNumConcurrentQueries > 0:
            sumQueryMicroseconds += interval
            sumNumConcurrentQueries += interval * currNumConcurrentQueries
        currNumConcurrentQueries += times[i][1]
        maxNumConcurrentQueries = max(maxNumConcurrentQueries, currNumConcurrentQueries)
    # average number of concurrent queries in one microsecond
    avgNumConcurrentQueries = float(sumNumConcurrentQueries) / sumQueryMicroseconds
    print maxNumConcurrentQueries
    print avgNumConcurrentQueries

queries = db.queries.aggregate([
    {'$match': {'tag': tag}},
    {'$group': {'_id': '$sql.type', 'runtime': {'$sum': '$runtime'}, 'count': {'$sum': 1}}},
])['result']
queries.sort(key=lambda query: query['count'], reverse=True)
plots.stacked_bar([query['count'] for query in queries],
        "Count", ['%s %s' % (query['_id'], query['count']) for query in queries],
        "#Query",
        "stacked_query_count.png")
plots.stacked_bar([query['runtime'] / 1000000 for query in queries],
        "Time", ['%s %sms' % (query['_id'], query['runtime'] / 1000000) for query in queries],
        "Query Sum Time (ms)",
        "stacked_query_time.png")
