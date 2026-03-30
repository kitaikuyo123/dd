package com.minisql.sql.execution;

import com.minisql.sql.SQLParser;
import com.minisql.sql.ast.SelectStatement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionEvaluatorTest {

    private final ConditionEvaluator evaluator = new ConditionEvaluator();

    @Test
    void respectsAndOrPrecedence() {
        SelectStatement statement = (SelectStatement) new SQLParser(
            "SELECT * FROM users WHERE age > 18 OR age < 10 AND status = 'active'"
        ).parse();

        Row teenageInactive = new Row(new String[] {"age", "status"}, new Object[] {16, "inactive"});
        Row childActive = new Row(new String[] {"age", "status"}, new Object[] {8, "active"});
        Row adultInactive = new Row(new String[] {"age", "status"}, new Object[] {30, "inactive"});

        assertFalse(evaluator.evaluate(statement.getWhere(), teenageInactive));
        assertTrue(evaluator.evaluate(statement.getWhere(), childActive));
        assertTrue(evaluator.evaluate(statement.getWhere(), adultInactive));
    }

    @Test
    void supportsColumnToColumnComparison() {
        SelectStatement statement = (SelectStatement) new SQLParser(
            "SELECT * FROM users WHERE users.id = orders.user_id"
        ).parse();
        Row match = new Row(new String[] {"id", "user_id"}, new Object[] {1, 1});
        Row mismatch = new Row(new String[] {"id", "user_id"}, new Object[] {1, 2});

        assertTrue(evaluator.evaluate(statement.getWhere(), match));
        assertFalse(evaluator.evaluate(statement.getWhere(), mismatch));
    }
}
