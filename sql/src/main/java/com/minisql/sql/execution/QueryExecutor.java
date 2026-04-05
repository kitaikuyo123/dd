package com.minisql.sql.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 查询执行器
 * 执行查询计划并返回结果
 */
public class QueryExecutor {

    private final ExecutionContext context;
    private final PlanConverter planConverter;

    public QueryExecutor() {
        this(new ExecutionContext());
    }

    public QueryExecutor(ExecutionContext context) {
        this.context = context;
        this.planConverter = new PlanConverter(context);
    }

    /**
     * 获取执行上下文
     */
    public ExecutionContext getExecutionContext() {
        return context;
    }

    /**
     * 执行查询计划
     */
    public QueryResult execute(QueryPlan plan) throws IOException {
        Operator root = planConverter.convert(plan.getRoot());
        return execute(root);
    }

    /**
     * 执行算子树
     */
    public QueryResult execute(Operator root) throws IOException {
        List<Row> rows = new ArrayList<>();
        String[] columns = root.getOutputColumns();

        root.open();
        try {
            while (root.hasMore()) {
                Row row = root.nextRow();
                if (row != null) {
                    rows.add(row);
                }
            }
        } finally {
            root.close();
        }

        return new QueryResult(columns, rows);
    }

    /**
     * 查询结果
     */
    public static class QueryResult {
        private final String[] columns;
        private final List<Row> rows;

        public QueryResult(String[] columns, List<Row> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        public String[] getColumns() {
            return columns;
        }

        public List<Row> getRows() {
            return rows;
        }

        public int getRowCount() {
            return rows.size();
        }

        public void print() {
            // 打印列头
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) System.out.print(" | ");
                System.out.print(columns[i]);
            }
            System.out.println();
            System.out.println("-".repeat(50));

            // 打印数据
            for (Row row : rows) {
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) System.out.print(" | ");
                    Object value = row.getValue(i);
                    System.out.print(value != null ? value : "NULL");
                }
                System.out.println();
            }
            System.out.println("Total rows: " + rows.size());
        }
    }
}
