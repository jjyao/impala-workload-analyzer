import sys
import numpy
import pymongo
from matplotlib import pyplot

db = pymongo.MongoClient().impala

pcts = []

queries = db.queries.find({'tag': sys.argv[1]})
for query in queries:
    operators = db.operators.find({
        'query_id': query['_id'],
        'name': 'AGGREGATE',
        'agg_type': 'PRE'
    })

    for operator in operators:
        child = db.operators.find_one({
            'query_id': query['_id'],
            'parent_id': operator['id']
        })

        numInputRows = child['avg_counters']['RowsReturned'] * child['num_hosts']
        numOutputRows = operator['avg_counters']['RowsReturned'] * operator['num_hosts']

        if numInputRows < numOutputRows:
            print '#input < #output %s' % operator['_id']
            continue

        if numInputRows == 0:
            pcts.append(1.0)
        else:
            pcts.append(numOutputRows / float(numInputRows))

pyplot.clf()
bins = numpy.arange(0, 1.1, 0.1)
pyplot.hist(pcts, bins)
pyplot.xticks(bins)
pyplot.xlabel('#out/#in')
pyplot.ylabel('#Pre Aggs')
pyplot.title('Pre Agg Reduction')
pyplot.tight_layout()
pyplot.savefig('%s/%s' % (sys.argv[2], 'pre_aggs_reduction_hist.png'))
