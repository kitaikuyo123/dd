package com.minisql.sql.execution;

import com.minisql.sql.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * 查询计划构建器
 * 将 AST 转换为可执行的查询计划
 */
public class QueryPlanBuilder {

    /**
     * 从 SELECT 语句构建查询计划
     */
    public QueryPlan build(SelectStatement stmt) {
        QueryPlan.PlanNode root = null;

        // 1. 处理 JOIN
        if (stmt.getJoinTable() != null) {
            root = buildJoinPlan(stmt);
        } else {
            // 单表扫描
            root = new QueryPlan.ScanNode(stmt.getTable());
        }

        // 2. 添加 WHERE 过滤
        if (stmt.getWhere() != null) {
            root = new QueryPlan.FilterNode(root, stmt.getWhere());
        }

        // 3. 添加聚合（如果有 GROUP BY 或聚合函数）
        // 注意：这里简化处理，实际应该从 SELECT 列中检测聚合函数

        // 4. 添加投影
        root = new QueryPlan.ProjectNode(root, stmt.getColumns(), stmt.isSelectAll());

        // 5. 添加 ORDER BY
        if (stmt.getOrderBy() != null && !stmt.getOrderBy().isEmpty()) {
            List<QueryPlan.SortKey> sortKeys = new ArrayList<>();
            for (SelectStatement.OrderByElement element : stmt.getOrderBy()) {
                sortKeys.add(new QueryPlan.SortKey(element.getColumn(), element.isAscending()));
            }
            root = new QueryPlan.SortNode(root, sortKeys);
        }

        // 6. 添加 LIMIT
        if (stmt.getLimit() != null) {
            int limit = stmt.getLimit();
            int offset = stmt.getOffset() != null ? stmt.getOffset() : 0;
            root = new QueryPlan.LimitNode(root, limit, offset);
        }

        return new QueryPlan(root);
    }

    /**
     * 构建 JOIN 查询计划
     */
    private QueryPlan.PlanNode buildJoinPlan(SelectStatement stmt) {
        QueryPlan.ScanNode left = new QueryPlan.ScanNode(stmt.getTable());
        QueryPlan.ScanNode right = new QueryPlan.ScanNode(stmt.getJoinTable());

        // 解析 JOIN 条件
        Condition joinCondition = stmt.getJoinCondition();

        return new QueryPlan.JoinNode(left, right, QueryPlan.JoinType.INNER, joinCondition);
    }

    /**
     * 为分布式查询构建计划
     * 添加远程扫描节点
     */
    public QueryPlan buildDistributedPlan(SelectStatement stmt,
                                           List<RegionInfo> regions) {
        if (regions == null || regions.isEmpty()) {
            return build(stmt);
        }

        List<QueryPlan.PlanNode> remoteScans = new ArrayList<>();

        for (RegionInfo region : regions) {
            QueryPlan.RemoteScanNode remoteScan = new QueryPlan.RemoteScanNode(
                stmt.getTable(),
                region.getRegionId(),
                region.getServerHost(),
                region.getServerPort()
            );
            remoteScan.setStartKey(region.getStartKey());
            remoteScan.setEndKey(region.getEndKey());
            remoteScans.add(remoteScan);
        }

        // 合并多个 Region 的结果
        QueryPlan.PlanNode root = new QueryPlan.UnionNode(remoteScans);

        // 添加过滤
        if (stmt.getWhere() != null) {
            root = new QueryPlan.FilterNode(root, stmt.getWhere());
        }

        // 添加投影
        root = new QueryPlan.ProjectNode(root, stmt.getColumns(), stmt.isSelectAll());

        // 添加 ORDER BY
        if (stmt.getOrderBy() != null && !stmt.getOrderBy().isEmpty()) {
            List<QueryPlan.SortKey> sortKeys = new ArrayList<>();
            for (SelectStatement.OrderByElement element : stmt.getOrderBy()) {
                sortKeys.add(new QueryPlan.SortKey(element.getColumn(), element.isAscending()));
            }
            root = new QueryPlan.SortNode(root, sortKeys);
        }

        // 添加 LIMIT
        if (stmt.getLimit() != null) {
            int limit = stmt.getLimit();
            int offset = stmt.getOffset() != null ? stmt.getOffset() : 0;
            root = new QueryPlan.LimitNode(root, limit, offset);
        }

        return new QueryPlan(root);
    }

    /**
     * 构建带有聚合的查询计划
     */
    public QueryPlan buildAggregatePlan(SelectStatement stmt,
                                         List<QueryPlan.AggregateExpr> aggregates,
                                         List<String> groupByColumns,
                                         List<RegionInfo> regions) {
        QueryPlan basePlan = buildDistributedPlan(stmt, regions);

        // 在合并后的结果上添加聚合
        QueryPlan.PlanNode root = new QueryPlan.AggregateNode(
            basePlan.getRoot(),
            aggregates,
            groupByColumns
        );

        return new QueryPlan(root);
    }

    /**
     * Region 信息
     */
    public static class RegionInfo {
        private String regionId;
        private String serverHost;
        private int serverPort;
        private byte[] startKey;
        private byte[] endKey;

        public RegionInfo(String regionId, String serverHost, int serverPort) {
            this.regionId = regionId;
            this.serverHost = serverHost;
            this.serverPort = serverPort;
        }

        // Getters and Setters
        public String getRegionId() { return regionId; }
        public void setRegionId(String regionId) { this.regionId = regionId; }
        public String getServerHost() { return serverHost; }
        public void setServerHost(String serverHost) { this.serverHost = serverHost; }
        public int getServerPort() { return serverPort; }
        public void setServerPort(int serverPort) { this.serverPort = serverPort; }
        public byte[] getStartKey() { return startKey; }
        public void setStartKey(byte[] startKey) { this.startKey = startKey; }
        public byte[] getEndKey() { return endKey; }
        public void setEndKey(byte[] endKey) { this.endKey = endKey; }
    }
}
