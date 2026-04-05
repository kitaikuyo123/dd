package com.minisql.sql.execution;

import com.minisql.common.model.Table;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.SimpleCondition;
import com.minisql.sql.execution.operators.AggregateOperator;
import com.minisql.sql.execution.operators.FilterOperator;
import com.minisql.sql.execution.operators.JoinOperator;
import com.minisql.sql.execution.operators.LimitOperator;
import com.minisql.sql.execution.operators.LocalScanOperator;
import com.minisql.sql.execution.operators.ProjectOperator;
import com.minisql.sql.execution.operators.RemoteScanOperator;
import com.minisql.sql.execution.operators.SortOperator;
import com.minisql.sql.execution.operators.UnionOperator;
import com.minisql.storage.StorageEngine;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts logical plans into physical operators.
 */
public class PlanConverter implements QueryPlan.PlanVisitor {

    private final ExecutionContext context;
    private final ConditionEvaluator conditionEvaluator;
    private Operator currentOperator;

    public PlanConverter(ExecutionContext context) {
        this.context = context;
        this.conditionEvaluator = new ConditionEvaluator();
    }

    public Operator convert(QueryPlan.PlanNode node) {
        node.accept(this);
        return currentOperator;
    }

    @Override
    public void visit(QueryPlan.ScanNode node) {
        StorageEngine storage = context.getStorageEngine(node.getTableName());
        Table tableSchema = context.getTableMetadata(node.getTableName());
        if (storage == null) {
            throw new RuntimeException("StorageEngine not found for table: " + node.getTableName());
        }
        currentOperator = new LocalScanOperator(storage, tableSchema, node.getStartKey(), node.getEndKey());
    }

    @Override
    public void visit(QueryPlan.RemoteScanNode node) {
        Table tableSchema = context.getTableMetadata(node.getTableName());
        currentOperator = new RemoteScanOperator(
            node.getTableName(),
            node.getRegionId(),
            node.getServerHost(),
            node.getServerPort(),
            node.getStartKey(),
            node.getEndKey(),
            tableSchema,
            null
        );
    }

    @Override
    public void visit(QueryPlan.FilterNode node) {
        node.getChildren().get(0).accept(this);
        Operator child = currentOperator;
        currentOperator = new FilterOperator(child, row -> conditionEvaluator.evaluate(node.getCondition(), row));
    }

    @Override
    public void visit(QueryPlan.ProjectNode node) {
        node.getChildren().get(0).accept(this);
        currentOperator = node.isSelectAll() ? currentOperator : new ProjectOperator(currentOperator, node.getColumns());
    }

    @Override
    public void visit(QueryPlan.JoinNode node) {
        node.getChildren().get(0).accept(this);
        Operator left = currentOperator;
        node.getChildren().get(1).accept(this);
        Operator right = currentOperator;
        currentOperator = new JoinOperator(left, right, node.getJoinType(), resolveJoinCondition(node.getJoinCondition()));
    }

    @Override
    public void visit(QueryPlan.AggregateNode node) {
        node.getChildren().get(0).accept(this);
        currentOperator = new AggregateOperator(currentOperator, node.getAggregates(), node.getGroupByColumns());
    }

    @Override
    public void visit(QueryPlan.SortNode node) {
        node.getChildren().get(0).accept(this);
        Operator child = currentOperator;
        List<SortOperator.SortKey> sortKeys = new ArrayList<>();
        for (QueryPlan.SortKey key : node.getSortKeys()) {
            sortKeys.add(new SortOperator.SortKey(key.getColumn(), key.isAscending()));
        }
        currentOperator = new SortOperator(child, sortKeys);
    }

    @Override
    public void visit(QueryPlan.LimitNode node) {
        node.getChildren().get(0).accept(this);
        currentOperator = new LimitOperator(currentOperator, node.getLimit(), node.getOffset());
    }

    @Override
    public void visit(QueryPlan.UnionNode node) {
        List<Operator> children = new ArrayList<>();
        for (QueryPlan.PlanNode child : node.getChildren()) {
            child.accept(this);
            children.add(currentOperator);
        }
        currentOperator = new UnionOperator(children);
    }

    private JoinOperator.JoinCondition resolveJoinCondition(Condition condition) {
        if (!(condition instanceof SimpleCondition)) {
            throw new IllegalArgumentException("Join condition must be a simple column comparison");
        }

        SimpleCondition simple = (SimpleCondition) condition;
        if (!simple.isValueColumnReference()) {
            throw new IllegalArgumentException("Join condition must compare one column to another column");
        }

        return new JoinOperator.JoinCondition(
            simple.getColumn(),
            simple.getValue(),
            mapJoinOperator(simple.getOperator())
        );
    }

    private JoinOperator.JoinOperatorType mapJoinOperator(String operator) {
        switch (operator) {
            case "=":
            case "==":
                return JoinOperator.JoinOperatorType.EQUALS;
            case "!=":
            case "<>":
                return JoinOperator.JoinOperatorType.NOT_EQUALS;
            case "<":
                return JoinOperator.JoinOperatorType.LESS_THAN;
            case ">":
                return JoinOperator.JoinOperatorType.GREATER_THAN;
            case "<=":
                return JoinOperator.JoinOperatorType.LESS_EQUAL;
            case ">=":
                return JoinOperator.JoinOperatorType.GREATER_EQUAL;
            default:
                throw new IllegalArgumentException("Unsupported join operator: " + operator);
        }
    }
}
