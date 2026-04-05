package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * 过滤算子
 * 根据条件过滤输入数据
 */
public class FilterOperator extends Operator {

    private final Operator child;
    private final Predicate predicate;
    private Row nextRow;
    private boolean opened;

    public FilterOperator(Operator child, Predicate predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    @Override
    public void open() throws IOException {
        child.open();
        opened = true;
        prefetch();
    }

    private void prefetch() throws IOException {
        nextRow = null;
        while (child.hasMore()) {
            Row row = child.nextRow();
            if (predicate.evaluate(row)) {
                nextRow = row;
                break;
            }
        }
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            return false;
        }
        return nextRow != null;
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            throw new IOException("Operator is closed");
        }
        if (nextRow == null) {
            throw new NoSuchElementException();
        }
        Row result = nextRow;
        prefetch();
        return result;
    }

    @Override
    public void close() throws IOException {
        opened = false;
        nextRow = null;
        child.close();
    }

    @Override
    public void reset() throws IOException {
        close();
        open();
    }

    @Override
    public String[] getOutputColumns() {
        return child.getOutputColumns();
    }

    /**
     * 谓词接口
     */
    @FunctionalInterface
    public interface Predicate {
        boolean evaluate(Row row);
    }
}
