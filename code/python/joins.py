import sys
import plots
import pymongo

def isWrongJoinImpl(operator, leftChild, rightChild):
    # check if impala uses the wrong join implementation
    broadcastJoinCost = min(leftChild['num_rows'] * leftChild['row_size'],
            rightChild['num_rows'] * rightChild['row_size']) * operator['num_hosts']
    partitionedJoinCost = leftChild['num_rows'] * leftChild['row_size'] + \
            rightChild['num_rows'] * rightChild['row_size']

    if broadcastJoinCost == partitionedJoinCost:
        return False
    elif broadcastJoinCost < partitionedJoinCost:
        if operator['join_impl'] != 'BROADCAST':
            return True
    else:
        if operator['join_impl'] != 'PARTITIONED':
            return True
    return False

def isWrongJoinOrder(operator, leftChild, rightChild):
    if operator['join_impl'] != 'BROADCAST':
        return False

    # http://www.cloudera.com/content/cloudera/en/documentation/cloudera-impala/latest/topics/impala_perf_joins.html
    if leftChild['num_rows'] * leftChild['row_size'] < rightChild['num_rows'] * rightChild['row_size']:
        return True
    else:
        return False

db = pymongo.MongoClient().impala
plots.outputDir = sys.argv[2]

numCorrectJoins = 0
numWrongJoinImpls = 0
numWrongJoinOrders = 0

timeCorrectJoins = 0
timeWrongJoinImpls = 0
timeWrongJoinOrders = 0

queries = db.queries.find({'tag': sys.argv[1]})
for query in queries:
    operators = db.operators.find({
        'query_id': query['_id'],
        'name': {'$in': ['HASH JOIN', 'CROSS JOIN']}
    })

    for operator in operators:
        leftChild = db.operators.find_one({
            'query_id': query['_id'],
            'id': operator['left_child_id']
        })
        rightChild = db.operators.find_one({
            'query_id': query['_id'],
            'id': operator['right_child_id']
        })

        if isWrongJoinImpl(operator, leftChild, rightChild):
            numWrongJoinImpls += 1
            timeWrongJoinImpls += operator['avg_time']
        elif isWrongJoinOrder(operator, leftChild, rightChild):
            numWrongJoinOrders += 1
            timeWrongJoinOrders += operator['avg_time']
        else:
            numCorrectJoins += 1
            timeCorrectJoins += operator['avg_time']

plots.stacked_bar(
    [
        numCorrectJoins,
        numWrongJoinImpls,
        numWrongJoinOrders,
    ],
    'Number of Joins',
    [
        'Correct Join %s' % numCorrectJoins,
        'Wrong Join Impl %s' % numWrongJoinImpls,
        'Wrong Join Order %s' % numWrongJoinOrders,
    ],
    'Join Correctness',
    'stacked_num_join_correctness.png'
)

plots.stacked_bar(
    [
        timeCorrectJoins / 1000000,
        timeWrongJoinImpls / 1000000,
        timeWrongJoinOrders / 1000000,
    ],
    'Time of Joins',
    [
        'Correct Join %sms' % (timeCorrectJoins / 1000000),
        'Wrong Join Impl %sms' % (timeWrongJoinImpls / 1000000),
        'Wrong Join Order %sms' % (timeWrongJoinOrders / 1000000),
    ],
    'Join Correctness',
    'stacked_time_join_correctness.png'
)
