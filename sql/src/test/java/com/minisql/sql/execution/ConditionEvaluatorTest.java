package com.minisql.sql.execution;

import com.minisql.sql.SQLParser;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.ast.SimpleCondition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionEvaluatorTest {

    private final ConditionEvaluator evaluator = new ConditionEvaluator();

    // ---- 原有测试 ----

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

    // ---- 补充测试：比较运算符 ----

    @Test
    void greaterThan() {
        Condition cond = new SimpleCondition("age", ">", "18");
        Row match = new Row(new String[] {"age"}, new Object[] {20});
        Row noMatch = new Row(new String[] {"age"}, new Object[] {10});

        assertTrue(evaluator.evaluate(cond, match));
        assertFalse(evaluator.evaluate(cond, noMatch));
    }

    @Test
    void lessThan() {
        Condition cond = new SimpleCondition("age", "<", "18");
        Row match = new Row(new String[] {"age"}, new Object[] {10});
        Row noMatch = new Row(new String[] {"age"}, new Object[] {20});

        assertTrue(evaluator.evaluate(cond, match));
        assertFalse(evaluator.evaluate(cond, noMatch));
    }

    @Test
    void greaterOrEqual() {
        Condition cond = new SimpleCondition("age", ">=", "18");
        Row equal = new Row(new String[] {"age"}, new Object[] {18});
        Row greater = new Row(new String[] {"age"}, new Object[] {20});
        Row less = new Row(new String[] {"age"}, new Object[] {10});

        assertTrue(evaluator.evaluate(cond, equal));
        assertTrue(evaluator.evaluate(cond, greater));
        assertFalse(evaluator.evaluate(cond, less));
    }

    @Test
    void lessOrEqual() {
        Condition cond = new SimpleCondition("age", "<=", "18");
        Row equal = new Row(new String[] {"age"}, new Object[] {18});
        Row less = new Row(new String[] {"age"}, new Object[] {10});
        Row greater = new Row(new String[] {"age"}, new Object[] {20});

        assertTrue(evaluator.evaluate(cond, equal));
        assertTrue(evaluator.evaluate(cond, less));
        assertFalse(evaluator.evaluate(cond, greater));
    }

    @Test
    void notEqual() {
        Condition cond = new SimpleCondition("status", "!=", "active");
        Row match = new Row(new String[] {"status"}, new Object[] {"inactive"});
        Row noMatch = new Row(new String[] {"status"}, new Object[] {"active"});

        assertTrue(evaluator.evaluate(cond, match));
        assertFalse(evaluator.evaluate(cond, noMatch));
    }

    @Test
    void notEqualAngleBracket() {
        Condition cond = new SimpleCondition("status", "<>", "active");
        Row match = new Row(new String[] {"status"}, new Object[] {"inactive"});

        assertTrue(evaluator.evaluate(cond, match));
    }

    // ---- LIKE ----

    @Test
    void likeOperator() {
        Condition cond = new SimpleCondition("name", "LIKE", "%ali%");
        Row match = new Row(new String[] {"name"}, new Object[] {"alice"});
        Row noMatch = new Row(new String[] {"name"}, new Object[] {"bob"});

        assertTrue(evaluator.evaluate(cond, match));
        assertFalse(evaluator.evaluate(cond, noMatch));
    }

    // ---- null / edge cases ----

    @Test
    void nullConditionReturnsTrue() {
        Row row = new Row(new String[] {"age"}, new Object[] {20});
        assertTrue(evaluator.evaluate(null, row));
    }

    @Test
    void nullColumnValueReturnsFalse() {
        Condition cond = new SimpleCondition("age", ">", "18");
        Row row = new Row(new String[] {"name"}, new Object[] {"alice"});  // age 列不存在

        assertFalse(evaluator.evaluate(cond, row));
    }

    // ---- 嵌套复合条件 ----

    @Test
    void nestedCompoundConditions() {
        // (age > 18 AND status = 'active') OR role = 'admin'
        Condition ageGt18 = new SimpleCondition("age", ">", "18");
        Condition statusActive = new SimpleCondition("status", "=", "active");
        Condition andCond = new CompoundCondition(ageGt18, statusActive, "AND");
        Condition roleAdmin = new SimpleCondition("role", "=", "admin");
        Condition orCond = new CompoundCondition(andCond, roleAdmin, "OR");

        // 满足 AND 部分
        Row adminRow = new Row(new String[] {"age", "status", "role"}, new Object[] {20, "active", "admin"});
        assertTrue(evaluator.evaluate(orCond, adminRow));

        // 不满足 AND 但满足 OR
        Row adminOnly = new Row(new String[] {"age", "status", "role"}, new Object[] {10, "inactive", "admin"});
        assertTrue(evaluator.evaluate(orCond, adminOnly));

        // 都不满足
        Row nobody = new Row(new String[] {"age", "status", "role"}, new Object[] {10, "inactive", "user"});
        assertFalse(evaluator.evaluate(orCond, nobody));
    }
}
