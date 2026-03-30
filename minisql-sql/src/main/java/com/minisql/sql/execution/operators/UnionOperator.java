package com.minisql.sql.execution.operators;

import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 合并算子
 * 合并多个子算子的结果（用于合并多个 Region 的查询结果）
 */
public class UnionOperator extends Operator {

    private final List<Operator> children;
    private Iterator<Operator> childIterator;
    private Operator currentChild;
    private boolean opened;

    public UnionOperator(List<Operator> children) {
        this.children = children;
    }

    @Override
    public void open() throws IOException {
        childIterator = children.iterator();
        opened = true;

        // 打开第一个子算子
        if (childIterator.hasNext()) {
            currentChild = childIterator.next();
            currentChild.open();
        }
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            open();
        }

        while (currentChild != null) {
            if (currentChild.hasMore()) {
                return currentChild.nextRow();
            }

            // 当前子算子耗尽，切换到下一个
            currentChild.close();
            if (childIterator.hasNext()) {
                currentChild = childIterator.next();
                currentChild.open();
            } else {
                currentChild = null;
            }
        }

        return null;
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            open();
        }

        while (currentChild != null) {
            if (currentChild.hasMore()) {
                return true;
            }

            // 当前子算子耗尽，切换到下一个
            currentChild.close();
            if (childIterator.hasNext()) {
                currentChild = childIterator.next();
                currentChild.open();
            } else {
                currentChild = null;
            }
        }

        return false;
    }

    @Override
    public void close() throws IOException {
        opened = false;
        for (Operator child : children) {
            child.close();
        }
        childIterator = null;
        currentChild = null;
    }

    @Override
    public void reset() throws IOException {
        close();
        open();
    }

    @Override
    public String[] getOutputColumns() {
        // 返回第一个子算子的列名
        if (!children.isEmpty()) {
            return children.get(0).getOutputColumns();
        }
        return new String[0];
    }
}
