package com.minisql.sql.ast;

import java.util.Collections;
import java.util.List;

/** 条件分割器，将 WHERE 条件拆分为 Region 扫描范围、谓词下推和残留过滤三部分 */
public class ConditionSplitter {

    private final String leftTable;
    private final String leftAlias;
    private final String rightTable;
    private final String rightAlias;

    public ConditionSplitter(String leftTable, String leftAlias,
                             String rightTable, String rightAlias) {
        this.leftTable = leftTable;
        this.leftAlias = leftAlias;
        this.rightTable = rightTable;
        this.rightAlias = rightAlias;
    }

    public SplitResult split(Condition condition) {
        if (condition == null) {
            return new SplitResult(null, null, null);
        }
        return doSplit(condition);
    }

    private SplitResult doSplit(Condition condition) {
        if (condition instanceof SimpleCondition) {
            return splitSimple((SimpleCondition) condition);
        }
        if (condition instanceof CompoundCondition) {
            return splitCompound((CompoundCondition) condition);
        }
        if (condition instanceof NotCondition) {
            return doSplit(((NotCondition) condition).getInner());
        }
        // 单列条件类型：根据列名限定符归入左/右表
        if (condition instanceof BetweenCondition) {
            return splitByColumn(((BetweenCondition) condition).getColumn(), condition);
        }
        if (condition instanceof InCondition) {
            return splitByColumn(((InCondition) condition).getColumn(), condition);
        }
        if (condition instanceof IsNullCondition) {
            return splitByColumn(((IsNullCondition) condition).getColumn(), condition);
        }
        // 子查询条件无法按表拆分，归入 cross
        return new SplitResult(null, null, condition);
    }

    /** 根据列名的表限定符将条件归入左表或右表 */
    private SplitResult splitByColumn(String column, Condition condition) {
        if (belongsToLeft(column)) return new SplitResult(condition, null, null);
        if (belongsToRight(column)) return new SplitResult(null, condition, null);
        // 未限定列名，默认归入左表
        return new SplitResult(condition, null, null);
    }

    private SplitResult splitSimple(SimpleCondition sc) {
        String col = sc.getColumn();
        String val = sc.getValue();

        boolean colIsLeft = belongsToLeft(col);
        boolean colIsRight = belongsToRight(col);
        boolean valIsLeft = sc.isValueColumnReference() && belongsToLeft(val);
        boolean valIsRight = sc.isValueColumnReference() && belongsToRight(val);

        boolean hasLeft = colIsLeft || valIsLeft;
        boolean hasRight = colIsRight || valIsRight;

        if (hasLeft && hasRight) {
            return new SplitResult(null, null, sc);
        } else if (hasLeft) {
            return new SplitResult(sc, null, null);
        } else if (hasRight) {
            return new SplitResult(null, sc, null);
        } else {
            // Unqualified columns on both sides of comparison, or can't determine.
            // If it's a valueColumnReference like "col1 = col2", both unqualified -> cross.
            // If it's "col = literal", unqualified -> can't determine, treat as left.
            if (sc.isValueColumnReference()) {
                return new SplitResult(null, null, sc);
            }
            return new SplitResult(sc, null, null);
        }
    }

    private SplitResult splitCompound(CompoundCondition cc) {
        if ("AND".equalsIgnoreCase(cc.getOperator())) {
            SplitResult leftResult = doSplit(cc.getLeft());
            SplitResult rightResult = doSplit(cc.getRight());

            Condition mergedLeft = mergeConditions(leftResult.leftOnly, rightResult.leftOnly);
            Condition mergedRight = mergeConditions(leftResult.rightOnly, rightResult.rightOnly);
            Condition mergedCross = mergeConditions(
                mergeConditions(leftResult.crossTable, rightResult.crossTable),
                null);  // cross is just all cross parts

            // Collect cross from both sides
            mergedCross = mergeConditions(leftResult.crossTable, rightResult.crossTable);

            return new SplitResult(mergedLeft, mergedRight, mergedCross);
        } else {
            // OR: if any sub-condition is cross-table, the whole thing is cross-table
            SplitResult leftResult = doSplit(cc.getLeft());
            SplitResult rightResult = doSplit(cc.getRight());

            if (leftResult.crossTable != null || rightResult.crossTable != null) {
                return new SplitResult(null, null, cc);
            }

            Condition mergedLeft = mergeConditions(leftResult.leftOnly, rightResult.leftOnly);
            Condition mergedRight = mergeConditions(leftResult.rightOnly, rightResult.rightOnly);

            // If one side has left and the other has right, the OR spans tables
            if (leftResult.leftOnly != null && rightResult.rightOnly != null) {
                return new SplitResult(null, null, cc);
            }
            if (leftResult.rightOnly != null && rightResult.leftOnly != null) {
                return new SplitResult(null, null, cc);
            }

            return new SplitResult(mergedLeft, mergedRight, null);
        }
    }

    private Condition mergeConditions(Condition a, Condition b) {
        if (a == null) return b;
        if (b == null) return a;
        return new CompoundCondition(a, b, "AND");
    }

    private boolean belongsToLeft(String column) {
        if (column == null) return false;
        return belongsTo(column, leftTable, leftAlias);
    }

    private boolean belongsToRight(String column) {
        if (column == null) return false;
        return belongsTo(column, rightTable, rightAlias);
    }

    private boolean belongsTo(String column, String tableName, String alias) {
        int dot = column.indexOf('.');
        if (dot < 0) return false;  // unqualified -> can't determine
        String qualifier = column.substring(0, dot);
        if (qualifier.equalsIgnoreCase(tableName)) return true;
        if (alias != null && qualifier.equalsIgnoreCase(alias)) return true;
        return false;
    }

    public static class SplitResult {
        public final Condition leftOnly;
        public final Condition rightOnly;
        public final Condition crossTable;

        public SplitResult(Condition leftOnly, Condition rightOnly, Condition crossTable) {
            this.leftOnly = leftOnly;
            this.rightOnly = rightOnly;
            this.crossTable = crossTable;
        }
    }
}
