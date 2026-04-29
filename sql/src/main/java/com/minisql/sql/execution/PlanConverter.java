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
 * 逻辑计划到物理算子的转换器
 *
 * 采用访问者模式遍历逻辑计划树，将每个逻辑节点转换为对应的火山模型算子。
 * 转换规则:
 *   ScanNode       -> LocalScanOperator（本地存储扫描）
 *   RemoteScanNode -> RemoteScanOperator（远程 Region 扫描）
 *   FilterNode     -> FilterOperator（条件过滤）
 *   ProjectNode    -> ProjectOperator（列投影）
 *   JoinNode       -> JoinOperator（连接，自动选择 Hash 或嵌套循环策略）
 *   AggregateNode  -> AggregateOperator（聚合计算）
 *   SortNode       -> SortOperator（排序）
 *   LimitNode      -> LimitOperator（分页截断）
 *   UnionNode      -> UnionOperator（合并多个子查询结果）
 */
public class PlanConverter implements QueryPlan.PlanVisitor {

    /** 执行上下文，提供存储引擎和表元数据的访问 */
    private final ExecutionContext context;
    /** 条件求值器，用于构建 FilterOperator 的断言函数 */
    private final ConditionEvaluator conditionEvaluator;
    /** 当前转换产生的物理算子 */
    private Operator currentOperator;

    public PlanConverter(ExecutionContext context) {
        this.context = context;
        this.conditionEvaluator = new ConditionEvaluator();
    }

    /**
     * 将逻辑计划根节点转换为物理算子树
     *
     * @return 物理算子树的根算子
     */
    public Operator convert(QueryPlan.PlanNode node) {
        node.accept(this);
        return currentOperator;
    }

    /** 将 ScanNode 转换为本地存储扫描算子 */
    @Override
    public void visit(QueryPlan.ScanNode node) {
        StorageEngine storage = context.getStorageEngine(node.getTableName());
        Table tableSchema = context.getTableMetadata(node.getTableName());
        if (storage == null) {
            throw new RuntimeException("StorageEngine not found for table: " + node.getTableName());
        }
        currentOperator = new LocalScanOperator(storage, tableSchema, node.getStartKey(), node.getEndKey());
    }

    /** 将 RemoteScanNode 转换为远程 Region 扫描算子 */
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

    /** 将 FilterNode 转换为过滤算子，利用 ConditionEvaluator 构建断言函数 */
    @Override
    public void visit(QueryPlan.FilterNode node) {
        node.getChildren().get(0).accept(this);
        Operator child = currentOperator;
        currentOperator = new FilterOperator(child, row -> conditionEvaluator.evaluate(node.getCondition(), row));
    }

    /** 将 ProjectNode 转换为投影算子，SELECT * 时跳过投影直接透传 */
    @Override
    public void visit(QueryPlan.ProjectNode node) {
        node.getChildren().get(0).accept(this);
        currentOperator = node.isSelectAll() ? currentOperator : new ProjectOperator(currentOperator, node.getColumns());
    }

    /** 将 JoinNode 转换为连接算子，解析条件并自动选择执行策略 */
    @Override
    public void visit(QueryPlan.JoinNode node) {
        node.getChildren().get(0).accept(this);
        Operator left = currentOperator;
        node.getChildren().get(1).accept(this);
        Operator right = currentOperator;
        currentOperator = new JoinOperator(left, right, node.getJoinType(), resolveJoinCondition(node.getJoinCondition()));
    }

    /** 将 AggregateNode 转换为聚合算子 */
    @Override
    public void visit(QueryPlan.AggregateNode node) {
        node.getChildren().get(0).accept(this);
        currentOperator = new AggregateOperator(currentOperator, node.getAggregates(), node.getGroupByColumns());
    }

    /** 将 SortNode 转换为排序算子 */
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

    /** 将 LimitNode 转换为分页截断算子 */
    @Override
    public void visit(QueryPlan.LimitNode node) {
        node.getChildren().get(0).accept(this);
        currentOperator = new LimitOperator(currentOperator, node.getLimit(), node.getOffset());
    }

    /** 将 UnionNode 转换为合并算子，收集所有子计划的结果 */
    @Override
    public void visit(QueryPlan.UnionNode node) {
        List<Operator> children = new ArrayList<>();
        for (QueryPlan.PlanNode child : node.getChildren()) {
            child.accept(this);
            children.add(currentOperator);
        }
        currentOperator = new UnionOperator(children);
    }

    /**
     * 将 AST 条件节点解析为 JoinCondition
     * 仅支持 SimpleCondition 且右侧必须为列引用（即列间比较）
     */
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

    /** 将 SQL 比较运算符字符串映射为 JoinOperatorType 枚举 */
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
