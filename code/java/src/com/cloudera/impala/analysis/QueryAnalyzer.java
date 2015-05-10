package com.cloudera.impala.analysis;

import com.mongodb.*;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class QueryAnalyzer {
    public static Map<String, Object> analyzeSelectQuery(SelectStmt stmt) {
        Map<String, Object> analyzedQuery = new HashMap<String, Object>();

        SelectList selectList = stmt.getSelectList();
        analyzedQuery.put("num_output_columns", selectList.getItems().size());

        /*
        Map<String, Set<String>> tableToColumnsMap = new HashMap<String, Set<String>>();
        for (SelectListItem item : selectList.getItems()) {
            String tableName = null;
            if (item.getTblName() != null) {
                tableName = item.getTblName().getTbl();
            }
            String columnName = null;
            Expr expr = item.getExpr();
            if (expr instanceof SlotRef) {
                columnName =((SlotRef)expr).getColumnName();
            } else if (expr instanceof FunctionCallExpr) {
                assert expr.getChildren().size() == 1;
                columnName = ((SlotRef)expr.getChild(0)).getColumnName();
            } else {
                continue;
            }

            if (tableName == null) {
                // this column name should be unique among all columns
                boolean exist = false;
                for (Set<String> columns : tableToColumnsMap.values()) {
                    if (columns.contains(columnName)) {
                        exist = true;
                        break;
                    }
                }

                if (exist) {
                    continue;
                }
            } else {
                if (tableToColumnsMap.containsKey(null) &&
                        tableToColumnsMap.get(null).contains(columnName)) {
                    // replace with the specific table
                    tableToColumnsMap.get(null).remove(columnName);
                }
            }

            if (!tableToColumnsMap.containsKey(tableName)) {
                tableToColumnsMap.put(tableName, new HashSet<String>());
            }

            tableToColumnsMap.get(tableName).add(columnName);
        }
        */

        int num_from_subqueries = 0;
        for (TableRef tableRef : stmt.getTableRefs()) {
            if (tableRef instanceof InlineViewRef) {
                num_from_subqueries++;
            }
        }
        analyzedQuery.put("num_from_subqueries", num_from_subqueries);

        if (stmt.hasGroupByClause()) {
            analyzedQuery.put("num_group_by_columns", stmt.groupingExprs_.size());
        }

        if (stmt.hasOrderByClause()) {
            analyzedQuery.put("num_order_by_columns", stmt.orderByElements_.size());
        }

        if (stmt.hasLimit()) {
            analyzedQuery.put("limit", ((NumericLiteral)stmt.limitElement_.getLimitExpr()).getIntValue());
        }

        return analyzedQuery;
    }

    public static void analyzeQuery(DBCollection queries, DBObject query) throws Exception {
        String sql = (String)query.get("sql");
        SqlScanner scanner = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(scanner);
        Object stmt = parser.parse().value;
        Map<String, Object> analyzedQuery;
        if (stmt instanceof SelectStmt) {
            analyzedQuery = analyzeSelectQuery((SelectStmt) stmt);
        } else {
            return;
        }

        queries.update(query, new BasicDBObject("$set", new BasicDBObject(analyzedQuery)));
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
