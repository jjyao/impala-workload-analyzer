import sys
import pymongo

db = pymongo.MongoClient().impala
queries = db.queries.find({'tag': sys.argv[1]})
for query in queries:
    db.fragments.remove({'query_id': query['_id']})
    db.operators.remove({'query_id': query['_id']})
db.queries.remove({'tag': sys.argv[1]})
