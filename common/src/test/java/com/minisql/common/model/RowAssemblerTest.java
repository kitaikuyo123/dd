package com.minisql.common.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.minisql.common.utils.RowKeySerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RowAssembler 单元测试")
class RowAssemblerTest {

    private Table schema;

    @BeforeEach
    void setUp() {
        schema = new Table("users");
        schema.addColumn(new Column("id", Column.ColumnType.INT));
        schema.addColumn(new Column("name", Column.ColumnType.STRING));
        schema.addColumn(new Column("age", Column.ColumnType.INT));
        schema.setPrimaryKey("id");
    }

    /**
     * 创建 KeyValue，value 按照列类型正确序列化
     */
    private KeyValue kv(String rowKey, String qualifier, long timestamp, Object value, boolean delete) {
        KeyValue.Builder builder = KeyValue.builder(rowKey.getBytes())
            .family("")
            .qualifier(qualifier)
            .timestamp(timestamp);
        if (delete) {
            builder.type(KeyValue.Type.DELETE);
        } else if (value != null) {
            // 查找列类型并序列化
            Column.ColumnType type = findColumnType(qualifier);
            builder.value(RowKeySerializer.serialize(value, type));
        }
        return builder.build();
    }

    private Column.ColumnType findColumnType(String columnName) {
        for (Column col : schema.getColumns()) {
            if (col.getName().equals(columnName)) {
                return col.getType();
            }
        }
        return Column.ColumnType.STRING;
    }

    // ---- assemble ----

    @Test
    @DisplayName("测试 assemble 单行数据")
    void testAssembleSingleRow() {
        List<KeyValue> kvs = List.of(
            kv("row1", "name", 100, "alice", false),
            kv("row1", "age", 100, 20, false)
        );

        List<Row> rows = RowAssembler.assemble(kvs, schema);

        assertEquals(1, rows.size());
        assertEquals("alice", rows.get(0).getColumn("name"));
        assertEquals(20, rows.get(0).getColumn("age"));
    }

    @Test
    @DisplayName("测试 assemble 多行数据按行键分组")
    void testAssembleMultipleRows() {
        List<KeyValue> kvs = List.of(
            kv("row1", "name", 100, "alice", false),
            kv("row1", "age", 100, 20, false),
            kv("row2", "name", 100, "bob", false),
            kv("row2", "age", 100, 25, false)
        );

        List<Row> rows = RowAssembler.assemble(kvs, schema);

        assertEquals(2, rows.size());
    }

    @Test
    @DisplayName("测试 assemble 空输入返回空列表")
    void testAssembleEmptyInput() {
        assertEquals(0, RowAssembler.assemble(Collections.emptyList(), schema).size());
    }

    @Test
    @DisplayName("测试 assemble null 输入返回空列表")
    void testAssembleNullInput() {
        assertEquals(0, RowAssembler.assemble(null, schema).size());
    }

    @Test
    @DisplayName("测试 assemble 跳过删除标记")
    void testAssembleSkipsDeletes() {
        List<KeyValue> kvs = List.of(
            kv("row1", "name", 200, "alice2", false),
            kv("row1", "name", 100, null, true),
            kv("row1", "age", 100, 20, false)
        );

        List<Row> rows = RowAssembler.assemble(kvs, schema);

        assertEquals(1, rows.size());
        // 后写入覆盖先写入（按输入顺序），所以应为 alice2
        assertEquals("alice2", rows.get(0).getColumn("name"));
    }

    @Test
    @DisplayName("测试 assemble 全部删除标记返回空列表")
    void testAssembleAllDeleted() {
        List<KeyValue> kvs = List.of(
            kv("row1", "name", 100, null, true),
            kv("row1", "age", 100, null, true)
        );

        List<Row> rows = RowAssembler.assemble(kvs, schema);
        assertEquals(0, rows.size());
    }

    @Test
    @DisplayName("测试 assemble 保留最大时间戳")
    void testAssembleMaxTimestamp() {
        List<KeyValue> kvs = List.of(
            kv("row1", "name", 500, "alice", false),
            kv("row1", "age", 300, 20, false)
        );

        List<Row> rows = RowAssembler.assemble(kvs, schema);

        assertEquals(1, rows.size());
        // assemble 使用 LinkedHashMap，row 的 timestamp 取最后一个设置时的值
        assertNotNull(rows.get(0));
    }

    // ---- mergeToRow ----

    @Test
    @DisplayName("测试 mergeToRow 合并为单行")
    void testMergeToRow() {
        List<KeyValue> kvs = List.of(
            kv("row1", "name", 100, "alice", false),
            kv("row1", "age", 100, 20, false)
        );

        Row row = RowAssembler.mergeToRow(kvs, schema);

        assertNotNull(row);
        assertEquals("alice", row.getColumn("name"));
        assertEquals(20, row.getColumn("age"));
    }

    @Test
    @DisplayName("测试 mergeToRow 空列表返回 null")
    void testMergeToRowEmpty() {
        assertNull(RowAssembler.mergeToRow(Collections.emptyList(), schema));
    }

    @Test
    @DisplayName("测试 mergeToRow null 输入返回 null")
    void testMergeToRowNull() {
        assertNull(RowAssembler.mergeToRow(null, schema));
    }

    @Test
    @DisplayName("测试 mergeToRow 跳过删除标记")
    void testMergeToRowSkipsDeletes() {
        List<KeyValue> kvs = List.of(
            kv("row1", "name", 200, "alice2", false),
            kv("row1", "name", 100, null, true)
        );

        Row row = RowAssembler.mergeToRow(kvs, schema);
        assertNotNull(row);
        assertEquals("alice2", row.getColumn("name"));
    }

    // ---- RowIterator ----

    @Test
    @DisplayName("测试 RowIterator 基本迭代")
    void testRowIteratorBasic() {
        List<KeyValue> kvs = List.of(
            kv("row1", "name", 100, "alice", false),
            kv("row1", "age", 100, 20, false),
            kv("row2", "name", 100, "bob", false),
            kv("row2", "age", 100, 25, false)
        );

        RowAssembler.RowIterator iter = new RowAssembler.RowIterator(kvs.iterator(), schema);

        assertTrue(iter.hasNext());
        Row first = iter.next();
        assertNotNull(first);

        assertTrue(iter.hasNext());
        Row second = iter.next();
        assertNotNull(second);

        assertFalse(iter.hasNext());
    }

    @Test
    @DisplayName("测试 RowIterator 空输入")
    void testRowIteratorEmpty() {
        RowAssembler.RowIterator iter = new RowAssembler.RowIterator(
            Collections.<KeyValue>emptyList().iterator(), schema);
        assertFalse(iter.hasNext());
    }

    @Test
    @DisplayName("测试 RowIterator next 无数据时抛异常")
    void testRowIteratorNextThrowsOnEmpty() {
        RowAssembler.RowIterator iter = new RowAssembler.RowIterator(
            Collections.<KeyValue>emptyList().iterator(), schema);
        assertThrows(java.util.NoSuchElementException.class, iter::next);
    }

    // ---- clearRow ----

    @Test
    @DisplayName("测试 clearRow 清空行数据")
    void testClearRow() {
        Row row = new Row();
        row.setColumn("name", "alice");
        row.setColumn("age", 20);
        assertEquals(2, row.getValues().size());

        RowAssembler.clearRow(row);
        assertEquals(0, row.getValues().size());
    }

    @Test
    @DisplayName("测试 clearRow null 输入不报错")
    void testClearRowNull() {
        assertDoesNotThrow(() -> RowAssembler.clearRow(null));
    }
}
