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
    public int numWhereFunctionCallExprs;
    public int numWhereCaseExprs;
    public int numWhereArithmeticExprs;
    public int numWhereCastExprs;
    public int numWhereTimestampArithmeticExprs;
    public int numSelectCaseExprs;
    public int numSelectArithmeticExprs;
    public int numSelectCastExprs;
    public int numSelectFunctionCallExprs;
    public int numSelectAnalyticExprs;
    public Map<String, Integer> numSelectCompoundPredicates;
    public Map<String, Integer> numSelectBinaryPredicates;
    public int numSelectIsNullPredicates;
    public Map<String, Integer> numHavingCompoundPredicates;
    public Map<String, Integer> numHavingBinaryPredicates;
    public int numHavingFunctionCallExprs;
    public int numUsingColumns;
    public Map<String, Integer> numOnCompoundPredicates;
    public Map<String, Integer> numOnBinaryPredicates;
    public int numOnBetweenPredicates;
    public int numOnFunctionCallExprs;

    public QueryStats() {
        numWhereCompoundPredicates = new HashMap<String, Integer>();
        for (CompoundPredicate.Operator operator : CompoundPredicate.Operator.values()) {
            numWhereCompoundPredicates.put(operator.toString(), 0);
        }

        numSelectCompoundPredicates = new HashMap<String, Integer>();
        for (CompoundPredicate.Operator operator : CompoundPredicate.Operator.values()) {
            numSelectCompoundPredicates.put(operator.toString(), 0);
        }

        numHavingCompoundPredicates = new HashMap<String, Integer>();
        for (CompoundPredicate.Operator operator : CompoundPredicate.Operator.values()) {
            numHavingCompoundPredicates.put(operator.toString(), 0);
        }

        numOnCompoundPredicates = new HashMap<String, Integer>();
        for (CompoundPredicate.Operator operator : CompoundPredicate.Operator.values()) {
            numOnCompoundPredicates.put(operator.toString(), 0);
        }

        numWhereBinaryPredicates = new HashMap<String, Integer>();
        for (BinaryPredicate.Operator operator : BinaryPredicate.Operator.values()) {
            numWhereBinaryPredicates.put(operator.getName(), 0);
        }

        numSelectBinaryPredicates = new HashMap<String, Integer>();
        for (BinaryPredicate.Operator operator : BinaryPredicate.Operator.values()) {
            numSelectBinaryPredicates.put(operator.getName(), 0);
        }

        numHavingBinaryPredicates = new HashMap<String, Integer>();
        for (BinaryPredicate.Operator operator : BinaryPredicate.Operator.values()) {
            numHavingBinaryPredicates.put(operator.getName(), 0);
        }

        numOnBinaryPredicates = new HashMap<String, Integer>();
        for (BinaryPredicate.Operator operator : BinaryPredicate.Operator.values()) {
            numOnBinaryPredicates.put(operator.getName(), 0);
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
        this.numWhereFunctionCallExprs += other.numWhereFunctionCallExprs;
        this.numWhereCaseExprs += other.numWhereCaseExprs;
        this.numWhereArithmeticExprs += other.numWhereArithmeticExprs;
        this.numWhereCastExprs += other.numWhereCastExprs;
        this.numWhereTimestampArithmeticExprs += other.numWhereTimestampArithmeticExprs;
        this.numSelectCaseExprs += other.numSelectCaseExprs;
        this.numSelectArithmeticExprs += other.numSelectArithmeticExprs;
        this.numSelectCastExprs += other.numSelectCastExprs;
        this.numSelectFunctionCallExprs += other.numSelectFunctionCallExprs;
        this.numSelectAnalyticExprs += other.numSelectAnalyticExprs;
        merge(this.numSelectCompoundPredicates, other.numSelectCompoundPredicates);
        merge(this.numSelectBinaryPredicates, other.numSelectBinaryPredicates);
        this.numSelectIsNullPredicates += other.numSelectIsNullPredicates;
        merge(this.numHavingCompoundPredicates, other.numHavingCompoundPredicates);
        merge(this.numHavingBinaryPredicates, other.numHavingBinaryPredicates);
        this.numHavingFunctionCallExprs += other.numHavingFunctionCallExprs;
        this.numUsingColumns += other.numUsingColumns;
        merge(this.numOnCompoundPredicates, other.numOnCompoundPredicates);
        merge(this.numOnBinaryPredicates, other.numOnBinaryPredicates);
        this.numOnFunctionCallExprs += other.numOnFunctionCallExprs;
        this.numOnBetweenPredicates += other.numOnBetweenPredicates;
    }

    private void merge(Map<String, Integer> map1, Map<String, Integer> map2) {
        for (String key : map2.keySet()) {
            map1.put(key, map1.get(key) + map2.get(key));
        }
    }
}
