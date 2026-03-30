package com.minisql.regionserver;

import com.minisql.common.model.Column;
import com.minisql.common.model.Table;
import com.minisql.common.utils.RowKeySerializer;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.SimpleCondition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PredicatePushdownPlanner unit tests")
class PredicatePushdownPlannerTest {

    @Test
    @DisplayName("primary key equality becomes exact row_key range")
    void testPrimaryKeyEqualityPushdown() {
        Table table = createProductsTable();

        PredicatePushdownPlanner.PushdownPlan plan =
            PredicatePushdownPlanner.plan(table, new SimpleCondition("id", "=", "7"));

        assertTrue(plan.canPushDown());
        assertArrayEquals(RowKeySerializer.serialize(7, Column.ColumnType.INT), plan.getStartKey());
        assertArrayEquals(
            PredicatePushdownPlanner.nextLexicographicKey(RowKeySerializer.serialize(7, Column.ColumnType.INT)),
            plan.getEndKey());
    }

    @Test
    @DisplayName("primary key range AND conditions are intersected")
    void testPrimaryKeyRangeIntersection() {
        Table table = createProductsTable();

        PredicatePushdownPlanner.PushdownPlan plan = PredicatePushdownPlanner.plan(
            table,
            new CompoundCondition(
                new SimpleCondition("id", ">=", "10"),
                new SimpleCondition("id", "<", "20"),
                "AND"));

        assertTrue(plan.canPushDown());
        assertArrayEquals(RowKeySerializer.serialize(10, Column.ColumnType.INT), plan.getStartKey());
        assertArrayEquals(RowKeySerializer.serialize(20, Column.ColumnType.INT), plan.getEndKey());
    }

    @Test
    @DisplayName("non primary key equality becomes storage predicate")
    void testNonPrimaryKeyEqualityPushdown() {
        Table table = createProductsTable();

        PredicatePushdownPlanner.PushdownPlan plan =
            PredicatePushdownPlanner.plan(table, new SimpleCondition("name", "=", "A"));

        assertTrue(plan.canPushDown());
        assertNull(plan.getStartKey());
        assertNull(plan.getEndKey());
        assertEquals(1, plan.getColumnPredicates().size());
        assertEquals("name", plan.getColumnPredicates().get(0).getQualifier());
        assertEquals("=", plan.getColumnPredicates().get(0).getOperator());
    }

    @Test
    @DisplayName("OR conditions stay in fallback path")
    void testOrConditionDoesNotPushDown() {
        Table table = createProductsTable();

        PredicatePushdownPlanner.PushdownPlan plan = PredicatePushdownPlanner.plan(
            table,
            new CompoundCondition(
                new SimpleCondition("id", "=", "1"),
                new SimpleCondition("id", "=", "2"),
                "OR"));

        assertFalse(plan.canPushDown());
    }

    @Test
    @DisplayName("primary key range and column predicate can be combined")
    void testMixedPrimaryAndColumnPushdown() {
        Table table = createProductsTable();

        PredicatePushdownPlanner.PushdownPlan plan = PredicatePushdownPlanner.plan(
            table,
            new CompoundCondition(
                new SimpleCondition("id", ">=", "5"),
                new SimpleCondition("price", "<", "100"),
                "AND"));

        assertTrue(plan.canPushDown());
        assertArrayEquals(RowKeySerializer.serialize(5, Column.ColumnType.INT), plan.getStartKey());
        assertNull(plan.getEndKey());
        assertEquals(1, plan.getColumnPredicates().size());
        assertEquals("price", plan.getColumnPredicates().get(0).getQualifier());
        assertEquals("<", plan.getColumnPredicates().get(0).getOperator());
    }

    private Table createProductsTable() {
        Table table = new Table("products");
        table.addColumn(new Column("id", Column.ColumnType.INT));
        table.addColumn(new Column("name", Column.ColumnType.STRING));
        table.addColumn(new Column("price", Column.ColumnType.INT));
        table.setPrimaryKey("id");
        return table;
    }
}
