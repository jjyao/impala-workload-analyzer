import sys
import pymongo

sum_num_joins = 0
max_num_joins = 0
min_num_joins = sys.maxint

sum_num_broadcast_joins = 0
max_num_broadcast_joins = 0
min_num_broadcast_joins = sys.maxint

sum_num_partitioned_joins = 0
max_num_partitioned_joins = 0
min_num_partitioned_joins = sys.maxint

sum_num_inner_joins = 0
max_num_inner_joins = 0
min_num_inner_joins = sys.maxint

sum_num_tables = 0
max_num_tables = 0
min_num_tables = sys.maxint

sum_num_hdfs_scans = 0
max_num_hdfs_scans = 0
min_num_hdfs_scans = sys.maxint

sum_num_output_columns = 0
max_num_output_columns = 0
min_num_output_columns = sys.maxint

sum_num_group_by_columns = 0
max_num_group_by_columns = 0
min_num_group_by_columns = sys.maxint

sum_num_order_by_columns = 0
max_num_order_by_columns = 0
min_num_order_by_columns = sys.maxint

sum_hdfs_scan_size = 0
max_hdfs_scan_size = 0
min_hdfs_scan_size = sys.maxint

sum_num_from_subqueries = 0
max_num_from_subqueries = 0
min_num_from_subqueries = sys.maxint

num_limit = 0

db = pymongo.MongoClient().impala

queries = db.queries.find()

for query in queries:
    num_joins = db.operators.find({'query_id': query['_id'], 'name': 'HASH JOIN'}).count()
    sum_num_joins += num_joins
    max_num_joins = max(max_num_joins, num_joins)
    min_num_joins = min(min_num_joins, num_joins)

    num_broadcast_joins = db.operators.find({'query_id': query['_id'], 'name': 'HASH JOIN', 'join_impl': 'BROADCAST'}).count()
    sum_num_broadcast_joins += num_broadcast_joins
    max_num_broadcast_joins = max(max_num_broadcast_joins, num_broadcast_joins)
    min_num_broadcast_joins = min(min_num_broadcast_joins, num_broadcast_joins)

    num_partitioned_joins = db.operators.find({'query_id': query['_id'], 'name': 'HASH JOIN', 'join_impl': 'PARTITIONED'}).count()
    sum_num_partitioned_joins += num_partitioned_joins
    max_num_partitioned_joins = max(max_num_partitioned_joins, num_partitioned_joins)
    min_num_partitioned_joins = min(min_num_partitioned_joins, num_partitioned_joins)

    num_inner_joins = db.operators.find({'query_id': query['_id'], 'name': 'HASH JOIN', 'join_type': 'INNER JOIN'}).count()
    sum_num_inner_joins += num_inner_joins
    max_num_inner_joins = max(max_num_inner_joins, num_inner_joins)
    min_num_inner_joins = min(min_num_inner_joins, num_inner_joins)

    hdfs_scan_size = db.operators.aggregate([
        {'$match': {'query_id': query['_id'], 'name': 'SCAN HDFS'}},
        {'$group': {'_id': None, 'size': {'$sum': '$size'}}},
    ])['result'][0]['size']
    sum_hdfs_scan_size += hdfs_scan_size
    max_hdfs_scan_size = max(max_hdfs_scan_size, hdfs_scan_size)
    min_hdfs_scan_size = min(min_hdfs_scan_size, hdfs_scan_size)

    num_tables = query['num_tables']
    sum_num_tables += num_tables
    max_num_tables = max(max_num_tables, num_tables)
    min_num_tables = min(min_num_tables, num_tables)

    num_hdfs_scans = query['num_hdfs_scans']
    sum_num_hdfs_scans += num_hdfs_scans
    max_num_hdfs_scans = max(max_num_hdfs_scans, num_hdfs_scans)
    min_num_hdfs_scans = min(min_num_hdfs_scans, num_hdfs_scans)

    num_output_columns = query['num_output_columns']
    sum_num_output_columns += num_output_columns
    max_num_output_columns = max(max_num_output_columns, num_output_columns)
    min_num_output_columns = min(min_num_output_columns, num_output_columns)

    num_from_subqueries = query['num_from_subqueries']
    sum_num_from_subqueries += num_from_subqueries
    max_num_from_subqueries = max(max_num_from_subqueries, num_from_subqueries)
    min_num_from_subqueries = min(min_num_from_subqueries, num_from_subqueries)

    try:
        num_group_by_columns = query['num_group_by_columns']
    except KeyError:
        num_group_by_columns = 0
    sum_num_group_by_columns += num_group_by_columns
    max_num_group_by_columns = max(max_num_group_by_columns, num_group_by_columns)
    min_num_group_by_columns = min(min_num_group_by_columns, num_group_by_columns)

    try:
        num_order_by_columns = query['num_order_by_columns']
    except KeyError:
        num_order_by_columns = 0
    sum_num_order_by_columns += num_order_by_columns
    max_num_order_by_columns = max(max_num_order_by_columns, num_order_by_columns)
    min_num_order_by_columns = min(min_num_order_by_columns, num_order_by_columns)

    if 'limit' in query:
        num_limit += 1

num_queries = queries.count()

avg_num_joins = sum_num_joins / float(num_queries)
print 'avg_num_joins %s' % avg_num_joins
print 'max_num_joins %s' % max_num_joins
print 'min_num_joins %s' % min_num_joins

avg_num_broadcast_joins = sum_num_broadcast_joins / float(num_queries)
print 'avg_num_broadcast_joins %s' % avg_num_broadcast_joins
print 'max_num_broadcast_joins %s' % max_num_broadcast_joins
print 'min_num_broadcast_joins %s' % min_num_broadcast_joins

avg_num_partitioned_joins = sum_num_partitioned_joins / float(num_queries)
print 'avg_num_partitioned_joins %s' % avg_num_partitioned_joins
print 'max_num_partitioned_joins %s' % max_num_partitioned_joins
print 'min_num_partitioned_joins %s' % min_num_partitioned_joins

avg_num_inner_joins = sum_num_inner_joins / float(num_queries)
print 'avg_num_inner_joins %s' % avg_num_inner_joins
print 'max_num_inner_joins %s' % max_num_inner_joins
print 'min_num_inner_joins %s' % min_num_inner_joins

avg_num_tables = sum_num_tables / float(num_queries)
print 'avg_num_tables %s' % avg_num_tables
print 'max_num_tables %s' % max_num_tables
print 'min_num_tables %s' % min_num_tables

avg_num_hdfs_scans = sum_num_hdfs_scans / float(num_queries)
print 'avg_num_hdfs_scans %s' % avg_num_hdfs_scans
print 'max_num_hdfs_scans %s' % max_num_hdfs_scans
print 'min_num_hdfs_scans %s' % min_num_hdfs_scans

avg_num_output_columns = sum_num_output_columns / float(num_queries)
print 'avg_num_output_columns %s' % avg_num_output_columns
print 'max_num_output_columns %s' % max_num_output_columns
print 'min_num_output_columns %s' % min_num_output_columns

avg_num_group_by_columns = sum_num_group_by_columns / float(num_queries)
print 'avg_num_group_by_columns %s' % avg_num_group_by_columns
print 'max_num_group_by_columns %s' % max_num_group_by_columns
print 'min_num_group_by_columns %s' % min_num_group_by_columns

avg_num_order_by_columns = sum_num_order_by_columns / float(num_queries)
print 'avg_num_order_by_columns %s' % avg_num_order_by_columns
print 'max_num_order_by_columns %s' % max_num_order_by_columns
print 'min_num_order_by_columns %s' % min_num_order_by_columns

avg_hdfs_scan_size = sum_hdfs_scan_size / float(num_queries)
print 'avg_hdfs_scan_size %s' % avg_hdfs_scan_size
print 'max_hdfs_scan_size %s' % max_hdfs_scan_size
print 'min_hdfs_scan_size %s' % min_hdfs_scan_size

avg_num_from_subqueries = sum_num_from_subqueries / float(num_queries)
print 'avg_num_from_subqueries %s' % avg_num_from_subqueries
print 'max_num_from_subqueries %s' % max_num_from_subqueries
print 'min_num_from_subqueries %s' % min_num_from_subqueries

print 'percent_limit %s%%' % (num_limit / float(num_queries) * 100)
