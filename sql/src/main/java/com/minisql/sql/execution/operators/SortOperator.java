package com.minisql.sql.execution.operators;

import com.minisql.common.utils.ValueComparator;
import com.minisql.sql.execution.Operator;
import com.minisql.sql.execution.Row;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;

/**
 * 排序算子 — 实现 ORDER BY
 *
 * <p>支持外部排序（Spill-to-Disk）：当内存行数超过 {@code maxInMemoryRows} 时，
 * 将排好序的数据写入临时文件，最后多路归并，避免 OOM。
 */
public class SortOperator extends Operator {

    private static final org.slf4j.Logger log =
        org.slf4j.LoggerFactory.getLogger(SortOperator.class);

    private final Operator child;
    private final List<SortKey> sortKeys;

    // 最大内存行数（超过后 Spill-to-Disk）
    private final int maxInMemoryRows;

    private List<Row> sortedRows;
    private Iterator<Row> iterator;
    private boolean opened;

    // Spill-to-Disk 相关
    private final List<Path> spillFiles = new ArrayList<>();
    private final List<ObjectInputStream> spillReaders = new ArrayList<>();

    public SortOperator(Operator child, List<SortKey> sortKeys) {
        this(child, sortKeys, 10000);
    }

    public SortOperator(Operator child, List<SortKey> sortKeys, int maxInMemoryRows) {
        this.child = child;
        this.sortKeys = sortKeys;
        this.maxInMemoryRows = Math.max(100, maxInMemoryRows);
    }

    @Override
    public void open() throws IOException {
        child.open();
        opened = true;

        String[] columns = child.getOutputColumns();
        int[] keyIndices = new int[sortKeys.size()];
        boolean[] ascending = new boolean[sortKeys.size()];
        for (int i = 0; i < sortKeys.size(); i++) {
            keyIndices[i] = findColumnIndex(columns, sortKeys.get(i).getColumn(), true);
            ascending[i] = sortKeys.get(i).isAscending();
        }

        Comparator<Row> rowComparator = (r1, r2) -> {
            for (int i = 0; i < keyIndices.length; i++) {
                int idx = keyIndices[i];
                int cmp = ValueComparator.compare(r1.getValue(idx), r2.getValue(idx));
                if (!ascending[i]) cmp = -cmp;
                if (cmp != 0) return cmp;
            }
            return 0;
        };

        // Phase 1: collect rows, spill to disk when threshold is reached
        sortedRows = new ArrayList<>(Math.min(maxInMemoryRows, 1024));
        while (child.hasMore()) {
            sortedRows.add(child.nextRow());
            if (sortedRows.size() >= maxInMemoryRows) {
                spillToDisk(rowComparator);
                sortedRows.clear();
            }
        }

        if (spillFiles.isEmpty()) {
            // Everything fits in memory — single in-memory sort
            sortedRows.sort(rowComparator);
            iterator = sortedRows.iterator();
        } else {
            // Flush remaining rows as the final run
            if (!sortedRows.isEmpty()) {
                spillToDisk(rowComparator);
                sortedRows.clear();
            }
            // Phase 2: multi-way merge of spilled runs
            iterator = new MergingIterator(spillFiles, rowComparator);
        }
    }

    /**
     * Sort the current in-memory buffer and spill it to a temporary file.
     */
    private void spillToDisk(Comparator<Row> comparator) throws IOException {
        sortedRows.sort(comparator);
        Path tempFile = Files.createTempFile("sort_spill_", ".tmp");
        try (ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(
                Files.newOutputStream(tempFile)))) {
            // Write column names for deserialization
            oos.writeObject(child.getOutputColumns());
            for (Row row : sortedRows) {
                oos.writeObject(row.getValues());
            }
        }
        spillFiles.add(tempFile);
        log.debug("Spilled {} rows to {}", sortedRows.size(), tempFile);
    }

    @Override
    public Row nextRow() throws IOException {
        if (!opened) {
            open();
        }
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public boolean hasMore() throws IOException {
        if (!opened) {
            open();
        }
        return iterator.hasNext();
    }

    @Override
    public void close() throws IOException {
        opened = false;
        sortedRows = null;
        iterator = null;

        // Close all spill readers
        for (ObjectInputStream ois : spillReaders) {
            try { ois.close(); } catch (Exception ignored) {}
        }
        spillReaders.clear();

        // Delete all spill files
        for (Path p : spillFiles) {
            try { Files.deleteIfExists(p); } catch (Exception ignored) {}
        }
        spillFiles.clear();

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
     * Multi-way merge iterator for sorted runs stored in spill files.
     */
    private static class MergingIterator implements Iterator<Row> {
        private final PriorityQueue<SpillPeek> queue;

        MergingIterator(List<Path> spillFiles, Comparator<Row> comparator) throws IOException {
            this.queue = new PriorityQueue<>(
                spillFiles.size(),
                (a, b) -> comparator.compare(a.row, b.row)
            );
            for (Path file : spillFiles) {
                ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(
                        Files.newInputStream(file)));
                try {
                    String[] columns = (String[]) ois.readObject();
                    Object[] values = (Object[]) ois.readObject();
                    queue.offer(new SpillPeek(new Row(columns, values), ois));
                } catch (Exception e) {
                    // Empty spill file
                    ois.close();
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public Row next() {
            SpillPeek peek = queue.poll();
            Row result = peek.row;
            try {
                // Try reading next row from the same spill file
                Object[] values = (Object[]) peek.ois.readObject();
                peek.row = new Row(null, values); // columns set from first read
                queue.offer(peek);
            } catch (EOFException e) {
                // No more rows in this spill file
                try { peek.ois.close(); } catch (Exception ignored) {}
            } catch (Exception e) {
                throw new RuntimeException("Error during merge sort read", e);
            }
            return result;
        }

        private static class SpillPeek {
            Row row;
            final ObjectInputStream ois;

            SpillPeek(Row row, ObjectInputStream ois) {
                this.row = row;
                this.ois = ois;
            }
        }
    }

    /**
     * 排序键
     */
    public static class SortKey {
        private final String column;
        private final boolean ascending;

        public SortKey(String column, boolean ascending) {
            this.column = column;
            this.ascending = ascending;
        }

        public String getColumn() {
            return column;
        }

        public boolean isAscending() {
            return ascending;
        }
    }
}
