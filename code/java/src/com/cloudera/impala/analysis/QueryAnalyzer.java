package com.cloudera.impala.analysis;

import com.mongodb.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryAnalyzer {
    public static Map<String, Object> analyzeQuery(Object stmt) {
        Map<String, Object> analyzedQuery = new HashMap<String, Object>();

        analyzedQuery.put("type", stmt.getClass().getSimpleName());

        return analyzedQuery;
    }

    public static Map<String, Object> analyzeInsertQuery(InsertStmt stmt) {
        Map<String, Object> analyzedQuery = new HashMap<String, Object>();

        analyzedQuery.put("type", stmt.getClass().getSimpleName());

        analyzedQuery.put("overwrite", stmt.isOverwrite());

        if (stmt.getQueryStmt() instanceof SelectStmt) {
            analyzedQuery.put("query", analyzeSelectQuery((SelectStmt) stmt.getQueryStmt()));
        }

        return analyzedQuery;
    }

    public static Map<String, Object> analyzeUnionQuery(UnionStmt stmt) {
        Map<String, Object> analyzedQuery = new HashMap<String, Object>();

        analyzedQuery.put("type", stmt.getClass().getSimpleName());

        List<Map<String, Object>> analyzedQueries = new ArrayList<Map<String, Object>>();
        for (UnionStmt.UnionOperand operand: stmt.getOperands()) {
            if (operand.getQueryStmt() instanceof SelectStmt) {
                analyzedQueries.add(analyzeSelectQuery((SelectStmt) operand.getQueryStmt()));
            }
        }
        analyzedQuery.put("queries", analyzedQueries);

        return analyzedQuery;
    }

    public static Map<String, Object> analyzeSelectQuery(SelectStmt stmt) {
        Map<String, Object> analyzedQuery = new HashMap<String, Object>();

        analyzedQuery.put("type", stmt.getClass().getSimpleName());

        SelectList selectList = stmt.getSelectList();
        analyzedQuery.put("num_output_columns", selectList.getItems().size());

        int num_from_subqueries = 0;
        for (TableRef tableRef : stmt.getTableRefs()) {
            if (tableRef instanceof InlineViewRef) {
                num_from_subqueries++;
            }
        }
        analyzedQuery.put("num_from_subqueries", num_from_subqueries);

        if (stmt.hasGroupByClause()) {
            analyzedQuery.put("num_group_by_columns", stmt.groupingExprs_.size());
        } else {
            analyzedQuery.put("num_group_by_columns", 0);
        }

        if (stmt.hasOrderByClause()) {
            analyzedQuery.put("num_order_by_columns", stmt.orderByElements_.size());
        } else {
            analyzedQuery.put("num_order_by_columns", 0);
        }

        if (stmt.hasLimit()) {
            analyzedQuery.put("limit", ((NumericLiteral) stmt.limitElement_.getLimitExpr()).getIntValue());
        }

        return analyzedQuery;
    }

    public static void analyzeQuery(DBCollection queries, DBObject query) throws Exception {
        String sql = (String) ((DBObject) query.get("sql")).get("stmt");
        SqlScanner scanner = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(scanner);
        Object stmt = parser.parse().value;
        Map<String, Object> analyzedQuery;
        if (stmt instanceof SelectStmt) {
            analyzedQuery = analyzeSelectQuery((SelectStmt) stmt);
        } else if (stmt instanceof InsertStmt) {
            analyzedQuery = analyzeInsertQuery((InsertStmt) stmt);
        } else if (stmt instanceof UnionStmt) {
            analyzedQuery = analyzeUnionQuery((UnionStmt) stmt);
        } else {
            analyzedQuery = analyzeQuery(stmt);
        }

        analyzedQuery.put("stmt", sql);
        queries.update(query, new BasicDBObject("$set", new BasicDBObject("sql", new BasicDBObject(analyzedQuery))));
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
