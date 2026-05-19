package com.minisql.client.executor;

import com.minisql.sql.ast.BetweenCondition;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.InCondition;
import com.minisql.sql.ast.IsNullCondition;
import com.minisql.sql.ast.NotCondition;
import com.minisql.sql.ast.SimpleCondition;

/**
 * 条件序列化工具：将 AST 条件树转回 SQL WHERE 子句字符串，
 * 用于谓词下推到 RegionServer。
 *
 * <p>所有条件都尝试下推，由 RegionServer 决定哪些条件可以在存储层执行，
 * 剩余条件由 RegionServer 做行级过滤。
 */
public final class ConditionSerializer {

    private ConditionSerializer() {}

    /**
     * 将条件树序列化为 SQL 字符串。
     *
     * <p>示例输出：{@code (a = '1' AND b > '10')}
     * 字符串值用单引号包裹，列引用不加引号。
     */
    public static String toSql(Condition condition) {
        if (condition == null) return null;
        if (condition instanceof SimpleCondition) {
            SimpleCondition simple = (SimpleCondition) condition;
            if (simple.isValueColumnReference()) {
                return simple.getColumn() + " " + simple.getOperator() + " " + simple.getValue();
            }
            return simple.getColumn() + " " + simple.getOperator() + " '" + simple.getValue() + "'";
        }
        if (condition instanceof BetweenCondition) {
            BetweenCondition bc = (BetweenCondition) condition;
            String base = bc.getColumn() + (bc.isNegated() ? " NOT" : "") + " BETWEEN '"
                + bc.getLow() + "' AND '" + bc.getHigh() + "'";
            return base;
        }
        if (condition instanceof InCondition) {
            InCondition ic = (InCondition) condition;
            StringBuilder sb = new StringBuilder(ic.getColumn());
            if (ic.isNegated()) sb.append(" NOT");
            sb.append(" IN (");
            for (int i = 0; i < ic.getValues().size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append("'").append(ic.getValues().get(i)).append("'");
            }
            sb.append(")");
            return sb.toString();
        }
        if (condition instanceof IsNullCondition) {
            IsNullCondition inc = (IsNullCondition) condition;
            return inc.getColumn() + (inc.isNegated() ? " IS NOT NULL" : " IS NULL");
        }
        if (condition instanceof NotCondition) {
            return "NOT " + toSql(((NotCondition) condition).getInner());
        }
        if (condition instanceof CompoundCondition) {
            CompoundCondition compound = (CompoundCondition) condition;
            return "(" + toSql(compound.getLeft()) + " " + compound.getOperator()
                + " " + toSql(compound.getRight()) + ")";
        }
        return null;
    }
}
