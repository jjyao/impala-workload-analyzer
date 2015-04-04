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
    pyplot.savefig(output)


db = pymongo.MongoClient().impala

queries = db.queries.find()

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

num_limit = 0

num_queries = queries.count()

for query in queries:
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
        "num_joins.png")

min_num_broadcast_joins = min(num_broadcast_joins)
max_num_broadcast_joins = max(num_broadcast_joins)
avg_num_broadcast_joins = sum(num_broadcast_joins) / float(num_queries)
hist(num_broadcast_joins, min_num_broadcast_joins, max_num_broadcast_joins,
        "Number of Broadcast Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_broadcast_joins, max_num_broadcast_joins, avg_num_broadcast_joins),
        "num_broadcast_joins.png")

min_num_partitioned_joins = min(num_partitioned_joins)
max_num_partitioned_joins = max(num_partitioned_joins)
avg_num_partitioned_joins = sum(num_partitioned_joins) / float(num_queries)
hist(num_partitioned_joins, min_num_partitioned_joins, max_num_partitioned_joins,
        "Number of Partitioned Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_partitioned_joins, max_num_partitioned_joins, avg_num_partitioned_joins),
        "num_partitioned_joins.png")

min_num_inner_joins = min(num_inner_joins)
max_num_inner_joins = max(num_inner_joins)
avg_num_inner_joins = sum(num_inner_joins) / float(num_queries)
hist(num_inner_joins, min_num_inner_joins, max_num_inner_joins,
        "Number of Inner Joins", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_inner_joins, max_num_inner_joins, avg_num_inner_joins),
        "num_inner_joins.png")

min_num_tables = min(num_tables)
max_num_tables = max(num_tables)
avg_num_tables = sum(num_tables) / float(num_queries)
hist(num_tables, min_num_tables, max_num_tables,
        "Number of Tables", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_tables, max_num_tables, avg_num_tables),
        "num_tables.png")

min_num_hdfs_scans = min(num_hdfs_scans)
max_num_hdfs_scans = max(num_hdfs_scans)
avg_num_hdfs_scans = sum(num_hdfs_scans) / float(num_queries)
hist(num_hdfs_scans, min_num_hdfs_scans, max_num_hdfs_scans,
        "Number of HDFS Scans", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_hdfs_scans, max_num_hdfs_scans, avg_num_hdfs_scans),
        "num_hdfs_scans.png")

min_num_output_columns = min(num_output_columns)
max_num_output_columns = max(num_output_columns)
avg_num_output_columns = sum(num_output_columns) / float(num_queries)
hist(num_output_columns, min_num_output_columns, max_num_output_columns,
        "Number of Output Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_output_columns, max_num_output_columns, avg_num_output_columns),
        "num_output_columns.png")

min_num_group_by_columns = min(num_group_by_columns)
max_num_group_by_columns = max(num_group_by_columns)
avg_num_group_by_columns = sum(num_group_by_columns) / float(num_queries)
hist(num_group_by_columns, min_num_group_by_columns, max_num_group_by_columns,
        "Number of Group By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_group_by_columns, max_num_group_by_columns, avg_num_group_by_columns),
        "num_group_by_columns.png")

min_num_order_by_columns = min(num_order_by_columns)
max_num_order_by_columns = max(num_order_by_columns)
avg_num_order_by_columns = sum(num_order_by_columns) / float(num_queries)
hist(num_order_by_columns, min_num_order_by_columns, max_num_order_by_columns,
        "Number of Order By Columns", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_num_order_by_columns, max_num_order_by_columns, avg_num_order_by_columns),
        "num_order_by_columns.png")

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
        "num_from_subqueries.png")

min_runtime = min(runtime)
max_runtime = max(runtime)
avg_runtime = sum(runtime) / float(num_queries)
hist(runtime, min_runtime, max_runtime,
        "Runtime", "Number of Queries",
        "$min = %s$ $max = %s$ $avg = %s$" %
        (min_runtime, max_runtime, avg_runtime),
        "runtime.png")

print 'percent_limit %s%%' % (num_limit / float(num_queries) * 100)
