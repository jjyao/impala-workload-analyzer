import sys
import numpy
import pymongo
from matplotlib import pyplot
from scipy.spatial import distance
from sklearn import cluster
from sklearn import decomposition
from sklearn import preprocessing

db = pymongo.MongoClient().impala
tag = sys.argv[1]
outputDir = sys.argv[2]

queries = db.queries.find({
    'tag': tag,
    'sql.type': {'$in': ['SelectStmt', 'UnionStmt']}})

samples = []

for query in queries:
    sample = []

    sample.append(query['num_tables'])
    sample.append(
            query['sql']['num_from_subqueries'] +
            query['sql']['num_where_subqueries'] +
            query['sql']['num_with_subqueries'])
    sample.append(query['sql']['max_depth_subqueries'])
    sample.append(query['sql']['num_group_by_columns'])
    sample.append(query['sql']['num_order_by_columns'])
    sample.append(query['sql']['num_limits'])
    sample.append(
            query['sql']['num_where_in_predicates'] + 
            query['sql']['num_where_between_predicates'] +
            query['sql']['num_where_exists_predicates'] +
            query['sql']['num_where_is_null_predicates'] +
            sum(query['sql']['num_where_binary_predicates'].values()) +
            sum(query['sql']['num_where_like_predicates'].values()) +
            sum(query['sql']['num_having_binary_predicates'].values()) +
            query['sql']['num_using_columns'] +
            sum(query['sql']['num_on_binary_predicates'].values()) +
            query['sql']['num_on_between_predicates'])
    sample.append(
            query['sql']['num_where_function_call_exprs'] +
            query['sql']['num_where_case_exprs'] +
            query['sql']['num_where_arithmetic_exprs'] +
            query['sql']['num_where_cast_exprs'] +
            query['sql']['num_where_timestamp_arithmetic_exprs'] +
            query['sql']['num_having_function_call_exprs'] +
            query['sql']['num_on_function_call_exprs'])
    sample.append(
            query['sql']['num_select_case_exprs'] +
            query['sql']['num_select_arithmetic_exprs'] +
            query['sql']['num_select_cast_exprs'] +
            query['sql']['num_select_function_call_exprs'] +
            query['sql']['num_select_analytic_exprs'])
    sample.append(
            sum(query['sql']['num_select_binary_predicates'].values()) +
            query['sql']['num_select_is_null_predicates'])

    samples.append(sample)

samples = numpy.array(samples)

standardSamples = preprocessing.StandardScaler().fit_transform(samples)

K = range(1, 70)
estimators = [cluster.KMeans(n_clusters=k, max_iter=500, n_init=20).fit(standardSamples) for k in K]
centers = [estimator.cluster_centers_ for estimator in estimators]
euclideans = [distance.cdist(standardSamples, center, 'euclidean') for center in centers]
dist = [numpy.min(euclidean, axis=1) for euclidean in euclideans]
wcss = [sum(d**2) for d in dist]
tss = sum(distance.pdist(standardSamples)**2) / standardSamples.shape[0]
bss = tss - wcss
pyplot.clf()
pyplot.plot(K, bss / tss * 100, 'b*-')
pyplot.grid(True)
pyplot.xlabel('Number of clusters')
pyplot.ylabel('Percentage of variance explained')
pyplot.title('Elbow for KMeans clustering')
pyplot.savefig('%s/kmeans.png' % outputDir)

K = 10
estimator = cluster.KMeans(n_clusters=K, max_iter=500, n_init=20).fit(standardSamples)
labels = estimator.labels_
clusters = [[] for i in xrange(0, K)]
for i in xrange(0, len(labels)):
    clusters[labels[i]].append(samples[i])

for i in xrange(0, K):
    print len(clusters[i])
    print numpy.array_str(sum(clusters[i]) / float(len(clusters[i])), precision=8)
