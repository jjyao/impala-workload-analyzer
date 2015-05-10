import sys
import numpy
import pymongo
from matplotlib import pyplot

def hist(data, minval, maxval, xlabel, ylabel, title, output):
    pyplot.clf()
    min_num_bins = 10
    max_num_bins = 10
    step = max(1, (maxval - minval) / max_num_bins)
    start = minval
    stop = max(start + step * (min_num_bins + 1), maxval + step)
    if isinstance(minval, int):
        bins = range(start, stop, step)
    else:
        bins = numpy.arange(start, stop, step)
    pyplot.hist(data, bins)
    pyplot.xlabel(xlabel)
    pyplot.ylabel(ylabel)
    pyplot.title(title)
    pyplot.tight_layout()
    pyplot.savefig('%s/%s' % (outputDir, output))

def bar(data, minval, maxval, xlabel, ylabel, title, output):
    pyplot.clf()
    height = [0] * (maxval + 1 - minval)
    for i in data:
        height[i - minval] += 1
    pyplot.bar(numpy.arange(minval, maxval + 1), height, align = 'center')
    pyplot.xlabel(xlabel)
    pyplot.ylabel(ylabel)
    pyplot.title(title)
    pyplot.tight_layout()
    pyplot.savefig('%s/%s' % (outputDir, output))

def pie(data, labels, title, output):
    pyplot.clf()
    patches, texts = pyplot.pie(data)
    labels = ['{0} - {1:1.2f} %'.format(label, pct * 100) for label, pct in zip(labels, data)]
    pyplot.legend(patches, labels, loc='center right', bbox_to_anchor=(0.1, 0.5))
    pyplot.title(title)
    pyplot.savefig('%s/%s' % (outputDir, output), bbox_inches='tight')

db = pymongo.MongoClient().impala

queries = db.queries.find({'tag': sys.argv[1]})
outputDir = sys.argv[2]

num_joins = []
num_broadcast_joins = []
num_partitioned_joins = []
num_inner_joins = []

num_tables = []
num_hdfs_scans = []

num_from_subqueries = []

num_output_columns = []
num_group_by_columns = []
num_order_by_columns = []

hdfs_scan_size = []

runtime = []

sum_time_pct = {}

num_limit = 0

num_queries = queries.count()

for query in queries:
    operators = db.operators.aggregate([
        {'$match': {'query_id': query['_id']}},
        {'$group': {'_id': '$name', 'avg_time': {'$sum': '$avg_time'}}},
    ])['result']
    sum_time = float(sum(operator['avg_time'] for operator in operators))
    for operator in operators:
        operator['time_pct'] = operator['avg_time'] / sum_time
    for operator in operators:
        if operator['_id'] not in sum_time_pct:
            sum_time_pct[operator['_id']] = operator['time_pct']
        else:
            sum_time_pct[operator['_id']] += operator['time_pct']

    num_joins.append(
            db.operators.find({'query_id': query['_id'], 'name': 'HASH JOIN'}).count())

    num_broadcast_joins.append(
            db.operators.find({'query_id': query['_id'], 'name': 'HASH JOIN', 'join_impl': 'BROADCAST'}).count())

    num_partitioned_joins.append(
            db.operators.find({'query_id': query['_id'], 'name': 'HASH JOIN', 'join_impl': 'PARTITIONED'}).count())

    num_inner_joins.append(
            db.operators.find({'query_id': query['_id'], 'name': 'HASH JOIN', 'join_type': 'INNER JOIN'}).count())

    hdfs_scan_size.append(
            db.operators.aggregate([
                {'$match': {'query_id': query['_id'], 'name': 'SCAN HDFS'}},
                {'$group': {'_id': None, 'size': {'$sum': '$size'}}},
            ])['result'][0]['size'] / 1024 / 1024)

    num_tables.append(query['num_tables'])

    num_hdfs_scans.append(query['num_hdfs_scans'])

    num_output_columns.append(query['num_output_columns'])

    num_from_subqueries.append(query['num_from_subqueries'])

    runtime.append(query['runtime'] / 1000000000)

    try:
        num_group_by_columns.append(query['num_group_by_columns'])
    except KeyError:
        num_group_by_columns.append(0)

    try:
        num_order_by_columns.append(query['num_order_by_columns'])
    except KeyError:
        num_order_by_columns.append(0)

    if 'limit' in query:
        num_limit += 1

min_num_joins = min(num_joins)
max_num_joins = max(num_joins)
avg_num_joins = sum(num_joins) / float(num_queries)
hist(num_joins, min_num_joins, max_num_joins,
        "Number of Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_joins, max_num_joins, avg_num_joins),
        "num_joins_hist.png")
bar(num_joins, min_num_joins, max_num_joins,
        "Number of Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_joins, max_num_joins, avg_num_joins),
        "num_joins_bar.png")

min_num_broadcast_joins = min(num_broadcast_joins)
max_num_broadcast_joins = max(num_broadcast_joins)
avg_num_broadcast_joins = sum(num_broadcast_joins) / float(num_queries)
hist(num_broadcast_joins, min_num_broadcast_joins, max_num_broadcast_joins,
        "Number of Broadcast Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_broadcast_joins, max_num_broadcast_joins, avg_num_broadcast_joins),
        "num_broadcast_joins_hist.png")
bar(num_broadcast_joins, min_num_broadcast_joins, max_num_broadcast_joins,
        "Number of Broadcast Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_broadcast_joins, max_num_broadcast_joins, avg_num_broadcast_joins),
        "num_broadcast_joins_bar.png")

min_num_partitioned_joins = min(num_partitioned_joins)
max_num_partitioned_joins = max(num_partitioned_joins)
avg_num_partitioned_joins = sum(num_partitioned_joins) / float(num_queries)
hist(num_partitioned_joins, min_num_partitioned_joins, max_num_partitioned_joins,
        "Number of Partitioned Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_partitioned_joins, max_num_partitioned_joins, avg_num_partitioned_joins),
        "num_partitioned_joins_hist.png")
bar(num_partitioned_joins, min_num_partitioned_joins, max_num_partitioned_joins,
        "Number of Partitioned Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_partitioned_joins, max_num_partitioned_joins, avg_num_partitioned_joins),
        "num_partitioned_joins_bar.png")

min_num_inner_joins = min(num_inner_joins)
max_num_inner_joins = max(num_inner_joins)
avg_num_inner_joins = sum(num_inner_joins) / float(num_queries)
hist(num_inner_joins, min_num_inner_joins, max_num_inner_joins,
        "Number of Inner Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_inner_joins, max_num_inner_joins, avg_num_inner_joins),
        "num_inner_joins_hist.png")
bar(num_inner_joins, min_num_inner_joins, max_num_inner_joins,
        "Number of Inner Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_inner_joins, max_num_inner_joins, avg_num_inner_joins),
        "num_inner_joins_bar.png")

min_num_tables = min(num_tables)
max_num_tables = max(num_tables)
avg_num_tables = sum(num_tables) / float(num_queries)
hist(num_tables, min_num_tables, max_num_tables,
        "Number of Tables", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_tables, max_num_tables, avg_num_tables),
        "num_tables_hist.png")
bar(num_tables, min_num_tables, max_num_tables,
        "Number of Tables", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_tables, max_num_tables, avg_num_tables),
        "num_tables_bar.png")

min_num_hdfs_scans = min(num_hdfs_scans)
max_num_hdfs_scans = max(num_hdfs_scans)
avg_num_hdfs_scans = sum(num_hdfs_scans) / float(num_queries)
hist(num_hdfs_scans, min_num_hdfs_scans, max_num_hdfs_scans,
        "Number of HDFS Scans", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_hdfs_scans, max_num_hdfs_scans, avg_num_hdfs_scans),
        "num_hdfs_scans_hist.png")
bar(num_hdfs_scans, min_num_hdfs_scans, max_num_hdfs_scans,
        "Number of HDFS Scans", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_hdfs_scans, max_num_hdfs_scans, avg_num_hdfs_scans),
        "num_hdfs_scans_bar.png")

min_num_output_columns = min(num_output_columns)
max_num_output_columns = max(num_output_columns)
avg_num_output_columns = sum(num_output_columns) / float(num_queries)
hist(num_output_columns, min_num_output_columns, max_num_output_columns,
        "Number of Output Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_output_columns, max_num_output_columns, avg_num_output_columns),
        "num_output_columns_hist.png")
bar(num_output_columns, min_num_output_columns, max_num_output_columns,
        "Number of Output Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_output_columns, max_num_output_columns, avg_num_output_columns),
        "num_output_columns_bar.png")

min_num_group_by_columns = min(num_group_by_columns)
max_num_group_by_columns = max(num_group_by_columns)
avg_num_group_by_columns = sum(num_group_by_columns) / float(num_queries)
hist(num_group_by_columns, min_num_group_by_columns, max_num_group_by_columns,
        "Number of Group By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_group_by_columns, max_num_group_by_columns, avg_num_group_by_columns),
        "num_group_by_columns_hist.png")
bar(num_group_by_columns, min_num_group_by_columns, max_num_group_by_columns,
        "Number of Group By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_group_by_columns, max_num_group_by_columns, avg_num_group_by_columns),
        "num_group_by_columns_bar.png")

min_num_order_by_columns = min(num_order_by_columns)
max_num_order_by_columns = max(num_order_by_columns)
avg_num_order_by_columns = sum(num_order_by_columns) / float(num_queries)
hist(num_order_by_columns, min_num_order_by_columns, max_num_order_by_columns,
        "Number of Order By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_order_by_columns, max_num_order_by_columns, avg_num_order_by_columns),
        "num_order_by_columns_hist.png")
bar(num_order_by_columns, min_num_order_by_columns, max_num_order_by_columns,
        "Number of Order By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_order_by_columns, max_num_order_by_columns, avg_num_order_by_columns),
        "num_order_by_columns_bar.png")

min_hdfs_scan_size = min(hdfs_scan_size)
max_hdfs_scan_size = max(hdfs_scan_size)
avg_hdfs_scan_size = sum(hdfs_scan_size) / float(num_queries)
hist(hdfs_scan_size, min_hdfs_scan_size, max_hdfs_scan_size,
        "HDFS Scan Size (MB)", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_hdfs_scan_size, max_hdfs_scan_size, avg_hdfs_scan_size),
        "hdfs_scan_size.png")

min_num_from_subqueries = min(num_from_subqueries)
max_num_from_subqueries = max(num_from_subqueries)
avg_num_from_subqueries = sum(num_from_subqueries) / float(num_queries)
hist(num_from_subqueries, min_num_from_subqueries, max_num_from_subqueries,
        "Number of From Subqueries", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_from_subqueries, max_num_from_subqueries, avg_num_from_subqueries),
        "num_from_subqueries_hist.png")
bar(num_from_subqueries, min_num_from_subqueries, max_num_from_subqueries,
        "Number of From Subqueries", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_from_subqueries, max_num_from_subqueries, avg_num_from_subqueries),
        "num_from_subqueries_bar.png")

min_runtime = min(runtime)
max_runtime = max(runtime)
avg_runtime = sum(runtime) / float(num_queries)
hist(runtime, min_runtime, max_runtime,
        "Runtime", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_runtime, max_runtime, avg_runtime),
        "runtime.png")

avg_time_pct = {name: (pct / num_queries) for name, pct in sum_time_pct.items()}
pie(avg_time_pct.values(), avg_time_pct.keys(), "Operator Avg Time Percent", "time_pct_pie.png")

print 'limit_pct %s%%' % (num_limit / float(num_queries) * 100)

clusters = db.queries.distinct('cluster')
for cluster in clusters:
    queries = db.queries.find({'cluster': cluster}, ['start_time', 'end_time'])
    times = []
    for query in queries:
        times.append((query['start_time'], 1))
        times.append((query['end_time'], -1))
    times.sort()
    max_num_concurrent_queries = 1
    cur_num_concurrent_queries = 1
    sum_num_query_seconds = 0
    sum_num_concurrent_queries = 0
    for i in xrange(1, len(times)):
        interval = times[i][0] - times[i-1][0]
        if cur_num_concurrent_queries > 0:
            sum_num_query_seconds += interval
            sum_num_concurrent_queries += interval * cur_num_concurrent_queries
        cur_num_concurrent_queries += times[i][1]
        max_num_concurrent_queries = max(max_num_concurrent_queries, cur_num_concurrent_queries)
    avg_num_concurrent_queries = float(sum_num_concurrent_queries) / sum_num_query_seconds
    print max_num_concurrent_queries
    print avg_num_concurrent_queries
