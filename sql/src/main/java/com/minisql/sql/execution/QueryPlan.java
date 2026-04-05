package com.minisql.sql.execution;

import com.minisql.sql.ast.Condition;

import java.util.List;

/**
 * 查询计划
 * 描述分布式查询的执行策略
 */
public class QueryPlan {

    // 查询计划节点类型
    public enum NodeType {
        SCAN,           // 表扫描
        FILTER,         // 条件过滤
        PROJECT,        // 列投影
        JOIN,           // 表连接
        AGGREGATE,      // 聚合
        SORT,           // 排序
        LIMIT,          // 限制返回数量
        UNION,          // 合并多个子查询结果
        REMOTE_SCAN     // 远程Region扫描
    }

    // 连接类型
    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL
    }

    // 聚合类型
    public enum AggregateType {
        COUNT,
        SUM,
        AVG,
        MAX,
        MIN
    }

    private PlanNode root;

    public QueryPlan() {
    }

    public QueryPlan(PlanNode root) {
        this.root = root;
    }

    public PlanNode getRoot() {
        return root;
    }

    public void setRoot(PlanNode root) {
        this.root = root;
    }

    /**
     * 查询计划节点基类
     */
    public static abstract class PlanNode {
        protected NodeType type;
        protected List<PlanNode> children;

        public NodeType getType() {
            return type;
        }

        public List<PlanNode> getChildren() {
            return children;
        }

        public abstract void accept(PlanVisitor visitor);
    }

    /**
     * 扫描节点
     */
    public static class ScanNode extends PlanNode {
        private String tableName;
        private byte[] startKey;
        private byte[] endKey;
        private List<String> columnFamilies;
        private List<String> qualifiers;

        public ScanNode(String tableName) {
            this.type = NodeType.SCAN;
            this.tableName = tableName;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }

        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
        public byte[] getStartKey() { return startKey; }
        public void setStartKey(byte[] startKey) { this.startKey = startKey; }
        public byte[] getEndKey() { return endKey; }
        public void setEndKey(byte[] endKey) { this.endKey = endKey; }
        public List<String> getColumnFamilies() { return columnFamilies; }
        public void setColumnFamilies(List<String> columnFamilies) { this.columnFamilies = columnFamilies; }
        public List<String> getQualifiers() { return qualifiers; }
        public void setQualifiers(List<String> qualifiers) { this.qualifiers = qualifiers; }
    }

    /**
     * 远程扫描节点（用于分布式查询）
     */
    public static class RemoteScanNode extends ScanNode {
        private String regionId;
        private String serverHost;
        private int serverPort;

        public RemoteScanNode(String tableName, String regionId, String serverHost, int serverPort) {
            super(tableName);
            this.type = NodeType.REMOTE_SCAN;
            this.regionId = regionId;
            this.serverHost = serverHost;
            this.serverPort = serverPort;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }

        public String getRegionId() { return regionId; }
        public void setRegionId(String regionId) { this.regionId = regionId; }
        public String getServerHost() { return serverHost; }
        public void setServerHost(String serverHost) { this.serverHost = serverHost; }
        public int getServerPort() { return serverPort; }
        public void setServerPort(int serverPort) { this.serverPort = serverPort; }
    }

    /**
     * 过滤节点
     */
    public static class FilterNode extends PlanNode {
        private Condition condition;

        public FilterNode(PlanNode child, Condition condition) {
            this.type = NodeType.FILTER;
            this.children = java.util.Collections.singletonList(child);
            this.condition = condition;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }

        public Condition getCondition() { return condition; }
        public void setCondition(Condition condition) { this.condition = condition; }
    }

    /**
     * 投影节点（选择列）
     */
    public static class ProjectNode extends PlanNode {
        private List<String> columns;
        private boolean selectAll;

        public ProjectNode(PlanNode child, List<String> columns, boolean selectAll) {
            this.type = NodeType.PROJECT;
            this.children = java.util.Collections.singletonList(child);
            this.columns = columns;
            this.selectAll = selectAll;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }

        public List<String> getColumns() { return columns; }
        public void setColumns(List<String> columns) { this.columns = columns; }
        public boolean isSelectAll() { return selectAll; }
        public void setSelectAll(boolean selectAll) { this.selectAll = selectAll; }
    }

    /**
     * 连接节点
     */
    public static class JoinNode extends PlanNode {
        private JoinType joinType;
        private Condition joinCondition;
        private String leftTable;
        private String rightTable;

        public JoinNode(PlanNode left, PlanNode right, JoinType joinType, Condition joinCondition) {
            this.type = NodeType.JOIN;
            this.children = java.util.Arrays.asList(left, right);
            this.joinType = joinType;
            this.joinCondition = joinCondition;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }

        public JoinType getJoinType() { return joinType; }
        public void setJoinType(JoinType joinType) { this.joinType = joinType; }
        public Condition getJoinCondition() { return joinCondition; }
        public void setJoinCondition(Condition joinCondition) { this.joinCondition = joinCondition; }
        public String getLeftTable() { return leftTable; }
        public void setLeftTable(String leftTable) { this.leftTable = leftTable; }
        public String getRightTable() { return rightTable; }
        public void setRightTable(String rightTable) { this.rightTable = rightTable; }
    }

    /**
     * 聚合节点
     */
    public static class AggregateNode extends PlanNode {
        private List<AggregateExpr> aggregates;
        private List<String> groupByColumns;

        public AggregateNode(PlanNode child, List<AggregateExpr> aggregates, List<String> groupByColumns) {
            this.type = NodeType.AGGREGATE;
            this.children = java.util.Collections.singletonList(child);
            this.aggregates = aggregates;
            this.groupByColumns = groupByColumns;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }

        public List<AggregateExpr> getAggregates() { return aggregates; }
        public void setAggregates(List<AggregateExpr> aggregates) { this.aggregates = aggregates; }
        public List<String> getGroupByColumns() { return groupByColumns; }
        public void setGroupByColumns(List<String> groupByColumns) { this.groupByColumns = groupByColumns; }
    }

    /**
     * 聚合表达式
     */
    public static class AggregateExpr {
        private AggregateType type;
        private String column;
        private String alias;

        public AggregateExpr(AggregateType type, String column) {
            this.type = type;
            this.column = column;
        }

        public AggregateType getType() { return type; }
        public void setType(AggregateType type) { this.type = type; }
        public String getColumn() { return column; }
        public void setColumn(String column) { this.column = column; }
        public String getAlias() { return alias; }
        public void setAlias(String alias) { this.alias = alias; }
    }

    /**
     * 排序节点
     */
    public static class SortNode extends PlanNode {
        private List<SortKey> sortKeys;

        public SortNode(PlanNode child, List<SortKey> sortKeys) {
            this.type = NodeType.SORT;
            this.children = java.util.Collections.singletonList(child);
            this.sortKeys = sortKeys;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }

        public List<SortKey> getSortKeys() { return sortKeys; }
        public void setSortKeys(List<SortKey> sortKeys) { this.sortKeys = sortKeys; }
    }

    /**
     * 排序键
     */
    public static class SortKey {
        private String column;
        private boolean ascending;

        public SortKey(String column, boolean ascending) {
            this.column = column;
            this.ascending = ascending;
        }

        public String getColumn() { return column; }
        public void setColumn(String column) { this.column = column; }
        public boolean isAscending() { return ascending; }
        public void setAscending(boolean ascending) { this.ascending = ascending; }
    }

    /**
     * 限制节点
     */
    public static class LimitNode extends PlanNode {
        private int limit;
        private int offset;

        public LimitNode(PlanNode child, int limit, int offset) {
            this.type = NodeType.LIMIT;
            this.children = java.util.Collections.singletonList(child);
            this.limit = limit;
            this.offset = offset;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }

        public int getLimit() { return limit; }
        public void setLimit(int limit) { this.limit = limit; }
        public int getOffset() { return offset; }
        public void setOffset(int offset) { this.offset = offset; }
    }

    /**
     * 合并节点（合并多个Region的结果）
     */
    public static class UnionNode extends PlanNode {
        public UnionNode(List<PlanNode> children) {
            this.type = NodeType.UNION;
            this.children = children;
        }

        @Override
        public void accept(PlanVisitor visitor) {
            visitor.visit(this);
        }
    }

    /**
     * 计划访问者接口
     */
    public interface PlanVisitor {
        void visit(ScanNode node);
        void visit(RemoteScanNode node);
        void visit(FilterNode node);
        void visit(ProjectNode node);
        void visit(JoinNode node);
        void visit(AggregateNode node);
        void visit(SortNode node);
        void visit(LimitNode node);
        void visit(UnionNode node);
    }

    /**
     * 打印查询计划
     */
    public String printPlan() {
        StringBuilder sb = new StringBuilder();
        printNode(root, 0, sb);
        return sb.toString();
    }

    private void printNode(PlanNode node, int indent, StringBuilder sb) {
        if (node == null) return;

        for (int i = 0; i < indent; i++) sb.append("  ");
        sb.append(node.getType().toString());

        if (node instanceof ScanNode) {
            ScanNode scan = (ScanNode) node;
            sb.append(" [table=").append(scan.getTableName()).append("]");
        } else if (node instanceof RemoteScanNode) {
            RemoteScanNode remote = (RemoteScanNode) node;
            sb.append(" [table=").append(remote.getTableName())
              .append(", region=").append(remote.getRegionId())
              .append(", server=").append(remote.getServerHost())
              .append(":").append(remote.getServerPort()).append("]");
        } else if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            sb.append(" [type=").append(join.getJoinType()).append("]");
        } else if (node instanceof AggregateNode) {
            sb.append(" [aggregates=").append(((AggregateNode) node).getAggregates().size()).append("]");
        } else if (node instanceof LimitNode) {
            LimitNode limit = (LimitNode) node;
            sb.append(" [limit=").append(limit.getLimit())
              .append(", offset=").append(limit.getOffset()).append("]");
        }
        sb.append("\n");

        if (node.getChildren() != null) {
            for (PlanNode child : node.getChildren()) {
                printNode(child, indent + 1, sb);
            }
        }
    }
}
