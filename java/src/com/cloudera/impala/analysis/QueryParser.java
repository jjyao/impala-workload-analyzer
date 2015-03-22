package com.cloudera.impala.analysis;

import com.mongodb.*;

import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class QueryParser {
    public static Map<String, Object> parseSelectQuery(SelectStmt stmt) {
        Map<String, Object> parsedQuery = new HashMap<String, Object>();

        Map<String, Set<String>> tableToColumnsMap = new HashMap<String, Set<String>>();

        SelectList selectList = stmt.getSelectList();
        parsedQuery.put("num_output_columns", selectList.getItems().size());
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

        if (stmt.hasGroupByClause()) {
            parsedQuery.put("num_group_by_columns", stmt.groupingExprs_.size());
        }

        if (stmt.hasOrderByClause()) {
            parsedQuery.put("num_order_by_columns", stmt.orderByElements_.size());
        }

        return parsedQuery;
    }

    public static void parseQuery(DBCollection queries, DBObject query) throws Exception {
        String sql = (String)query.get("sql");
        SqlScanner scanner = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(scanner);
        Object stmt = parser.parse().value;
        Map<String, Object> parsedQuery;
        if (stmt instanceof SelectStmt) {
            parsedQuery = parseSelectQuery((SelectStmt)stmt);
        } else {
            return;
        }

        queries.update(query, new BasicDBObject("$set", new BasicDBObject(parsedQuery)));
    }

    public static void main(String[] args) throws Exception {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("impala");
        DBCollection queries = db.getCollection("queries");

        DBCursor cursor = queries.find();
        try {
            while (cursor.hasNext()) {
                parseQuery(queries, cursor.next());
            }
        } finally {
            cursor.close();
        }

        mongoClient.close();
    }
}
