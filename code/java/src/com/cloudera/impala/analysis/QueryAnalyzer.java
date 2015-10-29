package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.View;
import com.cloudera.impala.util.Visitor;
import com.mongodb.*;
import org.apache.commons.lang.reflect.FieldUtils;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryAnalyzer {
    public static Map<String, Object> analyzeStmt(Object stmt) {
        Map<String, Object> analyzedStmt = new HashMap<String, Object>();

        analyzedStmt.put("type", stmt.getClass().getSimpleName());

        return analyzedStmt;
    }

    public static Map<String, Object> analyzeInsertStmt(InsertStmt stmt) throws Exception {
        Map<String, Object> analyzedStmt = new HashMap<String, Object>();

        analyzedStmt.put("type", stmt.getClass().getSimpleName());

        analyzedStmt.put("overwrite", stmt.isOverwrite());

        analyzedStmt.put("query", analyzeQueryStmt(stmt.getQueryStmt()));

        return analyzedStmt;
    }

    public static QueryStats getQueryStats(QueryStmt stmt) {
        if (stmt instanceof SelectStmt) {
            return getSelectQueryStats((SelectStmt) stmt);
        } else {
            return getUnionQueryStats((UnionStmt) stmt);
        }
    }

    @SuppressWarnings("unchecked")
    public static QueryStats getUnionQueryStats(UnionStmt stmt) {
        final QueryStats queryStats = new QueryStats();

        if (stmt.hasWithClause()) {
            try {
                List<View> views = (List<View>) FieldUtils.readField(stmt.getWithClause(), "views_", true);
                for (View view : views) {
                    queryStats.numWithSubqueries++;
                    queryStats.merge(getQueryStats(view.getQueryStmt()));
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        if (stmt.hasOrderByClause())  {
            queryStats.numOrderByColumns += stmt.orderByElements_.size();
        }

        if (stmt.hasLimit()) {
            queryStats.numLimits++;
        }

        for (UnionStmt.UnionOperand operand: stmt.getOperands()) {
            QueryStats operandQueryStats = getQueryStats(operand.getQueryStmt());
            queryStats.numOutputColumns = operandQueryStats.numOutputColumns;
            queryStats.merge(operandQueryStats);
        }

        return queryStats;
    }

    @SuppressWarnings("unchecked")
    public static QueryStats getSelectQueryStats(SelectStmt stmt) {
        final QueryStats queryStats = new QueryStats();

        if (stmt.hasWithClause()) {
            try {
                List<View> views = (List<View>) FieldUtils.readField(stmt.getWithClause(), "views_", true);
                for (View view : views) {
                    queryStats.numWithSubqueries++;
                    queryStats.merge(getQueryStats(view.getQueryStmt()));
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        SelectList selectList = stmt.getSelectList();
        for (SelectListItem selectListItem : selectList.getItems()) {
            if (selectListItem.isStar()) {
                queryStats.numOutputColumns = -1;
            } else {
                if (queryStats.numOutputColumns != -1) {
                    queryStats.numOutputColumns++;
                }

                selectListItem.getExpr().accept(new Visitor<Expr>() {
                    @Override
                    public void visit(Expr expr) {
                        if (expr instanceof CaseExpr) {
                            queryStats.numSelectCaseExprs++;
                        } else if (expr instanceof ArithmeticExpr) {
                            queryStats.numSelectArithmeticExprs++;
                        } else if (expr instanceof CastExpr) {
                            queryStats.numSelectCastExprs++;
                        } else if (expr instanceof FunctionCallExpr) {
                            queryStats.numSelectFunctionCallExprs++;
                        } else if (expr instanceof AnalyticExpr) {
                            queryStats.numSelectAnalyticExprs++;
                        } else if (expr instanceof CompoundPredicate) {
                            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
                            queryStats.incCounter(queryStats.numSelectCompoundPredicates,
                                    compoundPredicate.getOp().toString());
                        } else if (expr instanceof BinaryPredicate) {
                            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
                            queryStats.incCounter(queryStats.numSelectBinaryPredicates,
                                    binaryPredicate.getOp().getName());
                        } else if (expr instanceof IsNullPredicate) {
                            queryStats.numSelectIsNullPredicates++;
                        } else if (expr instanceof LiteralExpr) {
                        } else if (expr instanceof SlotRef) {
                        } else {
                            System.out.println(expr.getClass().getName());
                        }
                    }
                });
            }
        }

        if (stmt.hasWhereClause()) {
            stmt.getWhereClause().accept(new Visitor<Expr>() {
                @SuppressWarnings("unchecked")
                @Override
                public void visit(Expr expr) {
                    if (expr instanceof CompoundPredicate) {
                        CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
                        queryStats.incCounter(queryStats.numWhereCompoundPredicates,
                                compoundPredicate.getOp().toString());
                    } else if (expr instanceof BinaryPredicate) {
                        BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
                        queryStats.incCounter(queryStats.numWhereBinaryPredicates,
                                binaryPredicate.getOp().getName());
                    } else if (expr instanceof LikePredicate) {
                        try {
                            LikePredicate likePredicate = (LikePredicate) expr;
                            queryStats.incCounter(queryStats.numWhereLikePredicates,
                                    FieldUtils.readField(likePredicate, "op_", true).toString());
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    } else if (expr instanceof InPredicate) {
                        queryStats.numWhereInPredicates++;
                    } else if (expr instanceof BetweenPredicate) {
                        BetweenPredicate betweenPredicate = (BetweenPredicate) expr;
                        queryStats.numWhereBetweenPredicates++;

                        // Children of BetweenPredicate are not populated until it's analyzed
                        // As we don't analyze BetweenPredicate, we have to populate its children manually
                        try {
                            betweenPredicate.addChildren(
                                    (List<Expr>) FieldUtils.readField(
                                            betweenPredicate, "originalChildren_", true));
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    } else if (expr instanceof ExistsPredicate) {
                        queryStats.numWhereExistsPredicates++;
                    } else if (expr instanceof IsNullPredicate) {
                        queryStats.numWhereIsNullPredicates++;
                    } else if (expr instanceof FunctionCallExpr) {
                        queryStats.numWhereFunctionCallExprs++;
                    } else if (expr instanceof CaseExpr) {
                        queryStats.numWhereCaseExprs++;
                    } else if (expr instanceof Subquery) {
                        Subquery subquery = (Subquery) expr;
                        queryStats.numWhereSubqueries++;
                        queryStats.merge(getQueryStats(subquery.getStatement()));
                    } else if (expr instanceof ArithmeticExpr) {
                        queryStats.numWhereArithmeticExprs++;
                    } else if (expr instanceof CastExpr) {
                        queryStats.numWhereCastExprs++;
                    } else if (expr instanceof TimestampArithmeticExpr) {
                        queryStats.numWhereTimestampArithmeticExprs++;
                    } else if (expr instanceof LiteralExpr) {
                    } else if (expr instanceof SlotRef) {
                    } else {
                        System.out.println(expr.getClass().getName());
                    }
                }
            });
        }

        if (stmt.havingClause_ != null) {
            stmt.havingClause_.accept(new Visitor<Expr>() {
                @Override
                public void visit(Expr expr) {
                    if (expr instanceof CompoundPredicate) {
                        CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
                        queryStats.incCounter(queryStats.numHavingCompoundPredicates,
                                compoundPredicate.getOp().toString());
                    } else if (expr instanceof BinaryPredicate) {
                        BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
                        queryStats.incCounter(queryStats.numHavingBinaryPredicates,
                                binaryPredicate.getOp().getName());
                    } else if (expr instanceof FunctionCallExpr) {
                        queryStats.numHavingFunctionCallExprs++;
                    } else if (expr instanceof LiteralExpr) {
                    } else if (expr instanceof SlotRef) {
                    } else {
                        System.out.println(expr.getClass().getName());
                    }
                }
            });
        }

        if (stmt.hasGroupByClause()) {
            queryStats.numGroupByColumns += stmt.groupingExprs_.size();
        }

        if (stmt.hasOrderByClause()) {
            queryStats.numOrderByColumns += stmt.orderByElements_.size();
        }

        if (stmt.hasLimit()) {
            queryStats.numLimits++;
        }

        for (TableRef tableRef : stmt.getTableRefs()) {
            if (tableRef instanceof InlineViewRef) {
                queryStats.numFromSubqueries++;
                QueryStats viewQueryStats = getQueryStats(((InlineViewRef) tableRef).getViewStmt());
                queryStats.merge(viewQueryStats);
            }

            if (tableRef.onClause_ != null) {
                tableRef.onClause_.accept(new Visitor<Expr>() {
                    @Override
                    public void visit(Expr expr) {
                        if (expr instanceof CompoundPredicate) {
                            CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
                            queryStats.incCounter(queryStats.numOnCompoundPredicates,
                                    compoundPredicate.getOp().toString());
                        } else if (expr instanceof BinaryPredicate) {
                            BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
                            queryStats.incCounter(queryStats.numOnBinaryPredicates,
                                    binaryPredicate.getOp().getName());
                        } else if (expr instanceof FunctionCallExpr) {
                            queryStats.numOnFunctionCallExprs++;
                        } else if (expr instanceof BetweenPredicate) {
                            BetweenPredicate betweenPredicate = (BetweenPredicate) expr;
                            queryStats.numOnBetweenPredicates++;

                            try {
                                betweenPredicate.addChildren(
                                        (List<Expr>) FieldUtils.readField(
                                                betweenPredicate, "originalChildren_", true));
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        } else if (expr instanceof SlotRef) {
                        } else if (expr instanceof LiteralExpr) {
                        } else {
                            System.out.println(expr.getClass().getName());
                        }
                    }
                });
            }

            if (tableRef.usingColNames_ != null) {
                queryStats.numUsingColumns += tableRef.usingColNames_.size();
            }
        }

        if (queryStats.numFromSubqueries > 0 || queryStats.numWhereSubqueries > 0) {
            queryStats.maxDepthSubqueries++;
        }

        return queryStats;
    }

    public static Map<String, Object> analyzeQueryStmt(QueryStmt stmt) {
        Map<String, Object> analyzedStmt = new HashMap<String, Object>();

        analyzedStmt.put("type", stmt.getClass().getSimpleName());

        QueryStats queryStats = getQueryStats(stmt);
        analyzedStmt.put("num_output_columns", queryStats.numOutputColumns);
        analyzedStmt.put("num_from_subqueries", queryStats.numFromSubqueries);
        analyzedStmt.put("num_where_subqueries", queryStats.numWhereSubqueries);
        analyzedStmt.put("num_with_subqueries", queryStats.numWithSubqueries);
        analyzedStmt.put("max_depth_subqueries", queryStats.maxDepthSubqueries);
        analyzedStmt.put("num_group_by_columns", queryStats.numGroupByColumns);
        analyzedStmt.put("num_order_by_columns", queryStats.numOrderByColumns);
        analyzedStmt.put("num_limits", queryStats.numLimits);
        analyzedStmt.put("num_where_compound_predicates", queryStats.numWhereCompoundPredicates);
        analyzedStmt.put("num_where_binary_predicates", queryStats.numWhereBinaryPredicates);
        analyzedStmt.put("num_where_like_predicates", queryStats.numWhereLikePredicates);
        analyzedStmt.put("num_where_in_predicates", queryStats.numWhereInPredicates);
        analyzedStmt.put("num_where_between_predicates", queryStats.numWhereBetweenPredicates);
        analyzedStmt.put("num_where_exists_predicates", queryStats.numWhereExistsPredicates);
        analyzedStmt.put("num_where_is_null_predicates", queryStats.numWhereIsNullPredicates);
        analyzedStmt.put("num_where_function_call_exprs", queryStats.numWhereFunctionCallExprs);
        analyzedStmt.put("num_where_case_exprs", queryStats.numWhereCaseExprs);
        analyzedStmt.put("num_where_arithmetic_exprs", queryStats.numWhereArithmeticExprs);
        analyzedStmt.put("num_where_cast_exprs", queryStats.numWhereCastExprs);
        analyzedStmt.put("num_where_timestamp_arithmetic_exprs", queryStats.numWhereTimestampArithmeticExprs);
        analyzedStmt.put("num_select_case_exprs", queryStats.numSelectCaseExprs);
        analyzedStmt.put("num_select_arithmetic_exprs", queryStats.numSelectArithmeticExprs);
        analyzedStmt.put("num_select_cast_exprs", queryStats.numSelectCastExprs);
        analyzedStmt.put("num_select_function_call_exprs", queryStats.numSelectFunctionCallExprs);
        analyzedStmt.put("num_select_analytic_exprs", queryStats.numSelectAnalyticExprs);
        analyzedStmt.put("num_select_compound_predicates", queryStats.numSelectCompoundPredicates);
        analyzedStmt.put("num_select_binary_predicates", queryStats.numSelectBinaryPredicates);
        analyzedStmt.put("num_select_is_null_predicates", queryStats.numSelectIsNullPredicates);
        analyzedStmt.put("num_having_compound_predicates", queryStats.numHavingCompoundPredicates);
        analyzedStmt.put("num_having_binary_predicates", queryStats.numHavingBinaryPredicates);
        analyzedStmt.put("num_having_function_call_exprs", queryStats.numHavingFunctionCallExprs);
        analyzedStmt.put("num_using_columns", queryStats.numUsingColumns);
        analyzedStmt.put("num_on_compound_predicates", queryStats.numOnCompoundPredicates);
        analyzedStmt.put("num_on_binary_predicates", queryStats.numOnBinaryPredicates);
        analyzedStmt.put("num_on_function_call_exprs", queryStats.numOnFunctionCallExprs);
        analyzedStmt.put("num_on_between_predicates", queryStats.numOnBetweenPredicates);

        Parameterizer parameterizer = new Parameterizer();
        analyzedStmt.put("parameterized_stmt", parameterizer.parameterizeQuery(stmt));

        return analyzedStmt;
    }

    public static void analyzeQuery(DBCollection queries, DBObject query) throws Exception {
        String sql = (String) ((DBObject) query.get("sql")).get("stmt");
        SqlScanner scanner = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(scanner);
        Object stmt = parser.parse().value;
        Map<String, Object> analyzedStmt;
        if (stmt instanceof QueryStmt) {
            analyzedStmt = analyzeQueryStmt((QueryStmt) stmt);
        } else if (stmt instanceof InsertStmt) {
            analyzedStmt = analyzeInsertStmt((InsertStmt) stmt);
        } else {
            analyzedStmt = analyzeStmt(stmt);
        }

        analyzedStmt.put("stmt", sql);
        queries.update(query, new BasicDBObject("$set", new BasicDBObject("sql", new BasicDBObject(analyzedStmt))));
    }

    public static void main(String[] args) throws Exception {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("impala");
        DBCollection queries = db.getCollection("queries");

        DBCursor cursor = queries.find(new BasicDBObject("tag", args[0]));
        try {
            while (cursor.hasNext()) {
                analyzeQuery(queries, cursor.next());
            }
        } finally {
            cursor.close();
        }

        mongoClient.close();
    }
}
