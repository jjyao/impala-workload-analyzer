import sys
import pymongo
from asciitree import draw_tree
from bson.objectid import ObjectId

class Fragment(object):
    def __init__(self, fragment):
        self.fragment = fragment

    def children(self):
        children = []
        for operator in db.operators.find({'query_id': self.fragment['query_id'], 'fragment_id': self.fragment['id'], 'parent_id': None}):
            children.append(Operator(operator))
        return children

    def __str__(self):
        return "F%s" % self.fragment['id']

class Operator(object):
    def __init__(self, operator):
        self.operator = operator

    def children(self):
        children = []
        for operator in db.operators.find({'query_id': self.operator['query_id'], 'fragment_id': self.operator['fragment_id'], 'parent_id': self.operator['id']}):
            children.append(Operator(operator))
        for fragment in db.fragments.find({'query_id': self.operator['query_id'], 'exchange_id': self.operator['id']}):
            children.append(Fragment(fragment))
        return children

    def __str__(self):
        return "%s %s" % (self.operator['id'], self.operator['name'])

db = pymongo.MongoClient().impala
queryId = sys.argv[1]
root = Fragment(db.fragments.find_one({'query_id': ObjectId(queryId), 'exchange_id': None}))
print draw_tree(root, lambda n: n.children())
