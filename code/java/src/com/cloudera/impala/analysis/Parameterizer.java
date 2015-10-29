package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.View;
import com.cloudera.impala.util.Visitor;
import org.apache.commons.lang.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Parameterizer {
    @SuppressWarnings("unchecked")
    public String parameterizeQuery(QueryStmt stmt) {
        if (stmt instanceof SelectStmt) {
            parameterizeSelectQuery((SelectStmt) stmt);
        } else {
            parameterizeUnionQuery((UnionStmt) stmt);
        }

        if (stmt.hasWithClause()) {
            try {
                List<View> views = (List<View>) FieldUtils.readField(stmt.getWithClause(), "views_", true);
                for (View view : views) {
                    parameterizeView(view);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        if (stmt.hasOrderByClause()) {
            for (OrderByElement orderByElement : stmt.orderByElements_) {
                parameterizeOrderByElement(orderByElement);
            }
        }

        if (stmt.hasLimit()) {
            parameterizeLimitElement(stmt.limitElement_);
        }

        return stmt.toSql();
    }

    public void parameterizeUnionQuery(UnionStmt stmt) {
        for (UnionStmt.UnionOperand operand: stmt.getOperands()) {
            parameterizeQuery(operand.getQueryStmt());
        }
    }

    public void parameterizeSelectQuery(SelectStmt stmt) {
        SelectList selectList = stmt.getSelectList();
        for (SelectListItem selectListItem : selectList.getItems()) {
            parameterizeSelectListItem(selectListItem);
        }

        for(TableRef tableRef : stmt.getTableRefs()) {
            parameterizeTableRef(tableRef);
        }

        if (stmt.hasWhereClause()) {
            parameterizeExpr(stmt.getWhereClause());
        }

        if (stmt.hasGroupByClause()) {
            for (Expr groupingExpr : stmt.groupingExprs_) {
                parameterizeExpr(groupingExpr);
            }
        }

        if (stmt.havingClause_ != null) {
            parameterizeExpr(stmt.havingClause_);
        }
    }

    public void parameterizeView(View view) {
        writeFinalField(view, "name_", "?");
        parameterizeQuery(view.getQueryStmt());
    }

    public void parameterizeLimitElement(LimitElement limitElement) {
        if (limitElement.getLimitExpr() != null) {
            parameterizeExpr(limitElement.getLimitExpr());
        }

        if (limitElement.getOffsetExpr() != null) {
            parameterizeExpr(limitElement.getOffsetExpr());
        }
    }

    public void parameterizeOrderByElement(OrderByElement orderByElement) {
        parameterizeExpr(orderByElement.getExpr());
    }

    public void parameterizeSelectListItem(SelectListItem selectListItem) {
        if (selectListItem.isStar()) {
            return;
        }

        selectListItem.setAlias(null);

        parameterizeExpr(selectListItem.getExpr());
    }

    public void parameterizeTableRef(TableRef tableRef) {
        tableRef.name_ = new TableName(null, "?");

        if (tableRef instanceof InlineViewRef) {
            writeFinalField(tableRef, "alias_", "?");

            InlineViewRef inlineViewRef = (InlineViewRef) tableRef;
            parameterizeQuery(inlineViewRef.getViewStmt());

            if (inlineViewRef.getExplicitColLabels() != null) {
                try {
                    FieldUtils.writeField(inlineViewRef, "explicitColLabels_",
                            new ArrayList<String>(Collections.nCopies(inlineViewRef.getExplicitColLabels().size(), "?")));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        } else {
            writeFinalField(tableRef, "alias_", null);
        }

        if (tableRef.usingColNames_ != null) {
            tableRef.usingColNames_ = new ArrayList<String>(Collections.nCopies(tableRef.usingColNames_.size(), "?"));
        }

        if (tableRef.onClause_ != null) {
            parameterizeExpr(tableRef.onClause_);
        }
    }

    public void parameterizeExpr(Expr expr) {
        expr.accept(new Visitor<Expr>() {
            @Override
            public void visit(Expr expr) {
                if (expr instanceof CaseExpr) {
                } else if (expr instanceof ArithmeticExpr) {
                } else if (expr instanceof TimestampArithmeticExpr) {
                } else if (expr instanceof CastExpr) {
                } else if (expr instanceof FunctionCallExpr) {
                } else if (expr instanceof AnalyticExpr) {
                } else if (expr instanceof CompoundPredicate) {
                } else if (expr instanceof BetweenPredicate) {
                } else if (expr instanceof InPredicate) {
                } else if (expr instanceof BinaryPredicate) {
                } else if (expr instanceof IsNullPredicate) {
                } else if (expr instanceof ExistsPredicate) {
                } else if (expr instanceof LikePredicate) {
                } else if (expr instanceof LiteralExpr) {
                    parameterizeLiteralExpr((LiteralExpr) expr);
                } else if (expr instanceof SlotRef) {
                    parameterizeSlotRef((SlotRef) expr);
                } else if (expr instanceof Subquery) {
                    Subquery subquery = (Subquery) expr;
                    parameterizeQuery(subquery.getStatement());
                } else {
                    System.out.println(expr.getClass().getName());
                }
            }
        });
    }

    public void parameterizeLiteralExpr(LiteralExpr literalExpr) {
        try {
            if (literalExpr instanceof BoolLiteral) {
                BoolLiteral boolLiteral = (BoolLiteral) literalExpr;
                writeFinalField(boolLiteral, "value_", false);
            } else if (literalExpr instanceof NumericLiteral) {
                NumericLiteral numericLiteral = (NumericLiteral) literalExpr;
                FieldUtils.writeField(numericLiteral, "value_", new BigDecimal(1), true);
            } else if (literalExpr instanceof StringLiteral) {
                StringLiteral stringLiteral = (StringLiteral) literalExpr;
                writeFinalField(stringLiteral, "value_", "?");
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public void parameterizeSlotRef(SlotRef slotRef) {
        try {
            FieldUtils.writeField(slotRef, "tblName_", null, true);
            FieldUtils.writeField(slotRef, "label_", "?", true);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public void writeFinalField(Object target, String fieldName, Object value) {
        try {
            Field field = FieldUtils.getField(target.getClass(), fieldName, true);

            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(target, value);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }
}
