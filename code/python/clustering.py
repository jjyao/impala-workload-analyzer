import sys
import numpy
import pymongo
from matplotlib import pyplot
from scipy.spatial import distance
from sklearn import cluster
from sklearn import decomposition
from sklearn import preprocessing

db = pymongo.MongoClient().impala

queries = db.queries.find({
    'tag': sys.argv[1],
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
            sum(query['sql']['num_where_like_predicates'].values()))
    sample.append(query['sql']['num_where_function_call'])
    sample.append(query['sql']['num_where_case'])

    samples.append(sample)

samples = numpy.array(samples)

standardSamples = preprocessing.StandardScaler().fit_transform(samples)

K = range(1, 70)
estimators = [cluster.KMeans(n_clusters=k).fit(standardSamples) for k in K]
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
pyplot.savefig('kmeans.png')

K = 10
estimator = cluster.KMeans(n_clusters=K).fit(standardSamples)
labels = estimator.labels_
clusters = [[] for i in xrange(0, K)]
for i in xrange(0, len(labels)):
    clusters[labels[i]].append(samples[i])

for i in xrange(0, K):
    print sum(clusters[i]) / float(len(clusters[i]))
