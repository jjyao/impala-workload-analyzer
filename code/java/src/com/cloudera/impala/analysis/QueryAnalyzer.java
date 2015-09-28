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
                break;
            } else {
                queryStats.numOutputColumns++;
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
                        queryStats.numWhereFunctionCall++;
                    } else if (expr instanceof CaseExpr) {
                        queryStats.numWhereCase++;
                    } else if (expr instanceof Subquery) {
                        Subquery subquery = (Subquery) expr;
                        queryStats.numWhereSubqueries++;
                        queryStats.merge(getQueryStats(subquery.getStatement()));
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
        analyzedStmt.put("num_where_function_call", queryStats.numWhereFunctionCall);
        analyzedStmt.put("num_where_case", queryStats.numWhereCase);

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
