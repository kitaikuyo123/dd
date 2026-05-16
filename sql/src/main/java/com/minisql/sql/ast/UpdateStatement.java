package com.minisql.sql.ast;

import java.util.List;
import java.util.stream.Collectors;

/**
 * UPDATE 语句
 */
public class UpdateStatement extends Statement {
    private String table;
    private List<Assignment> assignments;
    private Condition where;

    @Override
    public StatementType getType() {
        return StatementType.UPDATE;
    }

    // Getters and Setters
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public List<Assignment> getAssignments() { return assignments; }
    public void setAssignments(List<Assignment> assignments) { this.assignments = assignments; }
    public Condition getWhere() { return where; }
    public void setWhere(Condition where) { this.where = where; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(table);
        sb.append(" SET ");
        if (assignments != null) {
            sb.append(assignments.stream()
                .map(a -> a.getColumn() + " = " + formatValue(a.getValue()))
                .collect(Collectors.joining(", ")));
        }
        if (where != null) {
            sb.append(" WHERE ");
            sb.append(whereToString(where));
        }
        return sb.toString();
    }

}
