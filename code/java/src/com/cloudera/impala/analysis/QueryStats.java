package com.cloudera.impala.analysis;

import java.util.HashMap;
import java.util.Map;

public class QueryStats {
    public int numOutputColumns;
    public int numFromSubqueries;
    public int numWhereSubqueries;
    public int numWithSubqueries;
    public int maxDepthSubqueries;
    public int numGroupByColumns;
    public int numOrderByColumns;
    public int numLimits;
    public Map<String, Integer> numWhereCompoundPredicates;
    public Map<String, Integer> numWhereBinaryPredicates;
    public Map<String, Integer> numWhereLikePredicates;
    public int numWhereInPredicates;
    public int numWhereBetweenPredicates;
    public int numWhereExistsPredicates;
    public int numWhereIsNullPredicates;
    public int numWhereFunctionCall;
    public int numWhereCase;

    public QueryStats() {
        numWhereCompoundPredicates = new HashMap<String, Integer>();
        for (CompoundPredicate.Operator operator : CompoundPredicate.Operator.values()) {
            numWhereCompoundPredicates.put(operator.toString(), 0);
        }

        numWhereBinaryPredicates = new HashMap<String, Integer>();
        for (BinaryPredicate.Operator operator : BinaryPredicate.Operator.values()) {
            numWhereBinaryPredicates.put(operator.getName(), 0);
        }

        numWhereLikePredicates = new HashMap<String, Integer>();
        for (LikePredicate.Operator operator : LikePredicate.Operator.values()) {
            numWhereLikePredicates.put(operator.toString(), 0);
        }
    }

    public void incCounter(Map<String, Integer> map, String key) {
        map.put(key, map.get(key) + 1);
    }

    public void merge(QueryStats other) {
        this.numFromSubqueries += other.numFromSubqueries;
        this.numWhereSubqueries += other.numWhereSubqueries;
        this.numWithSubqueries += other.numWithSubqueries;
        this.maxDepthSubqueries = Math.max(this.maxDepthSubqueries, other.maxDepthSubqueries);
        this.numGroupByColumns += other.numGroupByColumns;
        this.numOrderByColumns += other.numOrderByColumns;
        this.numLimits += other.numLimits;
        merge(this.numWhereCompoundPredicates, other.numWhereCompoundPredicates);
        merge(this.numWhereBinaryPredicates, other.numWhereBinaryPredicates);
        merge(this.numWhereLikePredicates, other.numWhereLikePredicates);
        this.numWhereInPredicates += other.numWhereInPredicates;
        this.numWhereBetweenPredicates += other.numWhereBetweenPredicates;
        this.numWhereExistsPredicates += other.numWhereExistsPredicates;
        this.numWhereIsNullPredicates += other.numWhereIsNullPredicates;
        this.numWhereFunctionCall += other.numWhereFunctionCall;
        this.numWhereCase += other.numWhereCase;
    }

    private void merge(Map<String, Integer> map1, Map<String, Integer> map2) {
        for (String key : map2.keySet()) {
            map1.put(key, map1.get(key) + map2.get(key));
        }
    }
}
