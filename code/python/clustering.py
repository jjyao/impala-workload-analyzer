import sys
import numpy
import pymongo
from matplotlib import pyplot
from sklearn import cluster
from sklearn import decomposition
from sklearn import preprocessing

db = pymongo.MongoClient().impala

queries = db.queries.find({
    'sql.type': {'$in': ['SelectStmt', 'InsertStmt', 'UnionStmt']}})

samples = []

for query in queries:
    print query['_id']
    sample = []
    sample.append(query['plan_time'] / 1000000)
    sample.append(query['fragment_start_time'] / 1000000)
    sample.append(query['runtime'] / 1000000000)

    codeGenTime = db.fragments.aggregate([
        {'$match': {'query_id': query['_id']}},
        {'$group': {'_id': None, 'sum_time': {'$sum': '$avg_code_gen.TotalTime'}}},
    ])['result'][0]['sum_time']
    sample.append(codeGenTime / 1000000)

    hdfsTableSinkTime = db.fragments.aggregate([
        {'$match': {'query_id': query['_id']}},
        {'$group': {'_id': None, 'sum_time': {'$sum': '$avg_hdfs_table_sink.TotalTime'}}},
    ])['result'][0]['sum_time']
    sample.append(hdfsTableSinkTime / 1000000000)

    operators = db.operators.aggregate([
        {'$match': {'query_id': query['_id']}},
        {'$group': {'_id': '$name', 'sum_time': {'$sum': '$avg_time'}}},
    ])['result']

    sumTimeAllOperators = float(sum(operator['sum_time'] for operator in operators)) + \
            query['plan_time'] + query['fragment_start_time'] + \
            codeGenTime + hdfsTableSinkTime
    sample.append(sumTimeAllOperators / 1000000000)

    numJoins = db.operators.find({
        'query_id': query['_id'],
        'name': {'$in': ['HASH JOIN', 'CROSS JOIN']}
    }).count()
    sample.append(numJoins)

    numBroadcastJoins = db.operators.find({
        'query_id': query['_id'],
        'name': {'$in': ['HASH JOIN', 'CROSS JOIN']},
        'join_impl': 'BROADCAST'
    }).count()
    sample.append(numBroadcastJoins)

    numPartitionedJoins = db.operators.find({
        'query_id': query['_id'],
        'name': {'$in': ['HASH JOIN', 'CROSS JOIN']},
        'join_impl': 'PARTITIONED'
    }).count()
    sample.append(numPartitionedJoins)

    numInnerJoins = db.operators.find({
        'query_id': query['_id'],
        'name': 'HASH JOIN',
        'join_type': 'INNER JOIN'
    }).count()
    sample.append(numInnerJoins)

    scanHdfs = db.operators.aggregate([
                {'$match': {'query_id': query['_id'], 'name': 'SCAN HDFS'}},
                {'$group': {'_id': None, 'size': {'$sum': '$size'}}},
            ])['result']
    if scanHdfs:
        sample.append(scanHdfs[0]['size'] / 1024 / 1024)
    else:
        sample.append(0)

    sample.append(query['num_hdfs_scans'])
    sample.append(query['num_tables'])

    sqlType = query['sql']['type']
    if sqlType == 'SelectStmt':
        sample.append(query['sql']['num_output_columns'])
        sample.append(query['sql']['num_from_subqueries'])
        sample.append(query['sql']['num_group_by_columns'])
        sample.append(query['sql']['num_order_by_columns'])

        if 'limit' in query['sql']:
            sample.append(query['sql']['limit'])
        else:
            sample.append(sys.maxint)
    elif sqlType == 'InsertStmt':
        assert query['sql']['query']['type'] == 'SelectStmt'
        sample.append(query['sql']['query']['num_output_columns'])
        sample.append(query['sql']['query']['num_from_subqueries'])
        sample.append(query['sql']['query']['num_group_by_columns'])
        sample.append(query['sql']['query']['num_order_by_columns'])

        if 'limit' in query['sql']['query']:
            sample.append(query['sql']['query']['limit'])
        else:
            sample.append(sys.maxint)
    elif sqlType == 'UnionStmt':
        sample.append(max(subquery['num_output_columns'] for subquery in query['sql']['queries']))
        sample.append(sum(subquery['num_from_subqueries'] for subquery in query['sql']['queries']))
        sample.append(sum(subquery['num_group_by_columns'] for subquery in query['sql']['queries']))
        sample.append(sum(subquery['num_order_by_columns'] for subquery in query['sql']['queries']))

        limits = []
        for subquery in query['sql']['queries']:
            if 'limit' in subquery:
                limits.append(subquery['limit'])
        if limits:
            sample.append(sum(limits))
        else:
            sample.append(sys.maxint)

    sample = sample[:-1]
    samples.append(sample)

samples = numpy.array(samples)

pca = decomposition.PCA(n_components=2)
pcaSamples = pca.fit(samples).transform(samples)
standardSamples = preprocessing.StandardScaler().fit_transform(pcaSamples)
print samples
print pcaSamples
print standardSamples
pyplot.clf()
pyplot.scatter(standardSamples[:, 0], standardSamples[:, 1])
pyplot.tight_layout()
pyplot.savefig('clustering.png')

clusters = cluster.DBSCAN(eps=0.2, min_samples=2).fit(standardSamples)
print clusters.labels_
