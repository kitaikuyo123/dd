package com.minisql.storage;

import com.minisql.common.model.KeyValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("KeyValueVisibilityResolver 单元测试")
class KeyValueVisibilityResolverTest {

    // ---- 原有测试 ----

    @Test
    @DisplayName("DELETE 标记后较新的 PUT 覆盖旧 PUT，多 qualifier 保留最新")
    void keepsLatestVisibleCellsAcrossDeletes() {
        KeyValueVisibilityResolver resolver = new KeyValueVisibilityResolver();
        List<KeyValue> values = List.of(
            kv("row1", "", "", 300, true, null),
            kv("row1", "", "name", 400, false, "alice2".getBytes()),
            kv("row1", "", "name", 200, false, "alice1".getBytes()),
            kv("row1", "", "price", 100, false, "18".getBytes())
        );

        List<KeyValue> resolved = resolver.resolveLatestValues(values);

        assertEquals(1, resolved.size());
        assertEquals("name", resolved.get(0).getQualifier());
        assertEquals("alice2", new String(resolved.get(0).getValue()));
    }

    // ---- 补充测试 ----

    @Test
    @DisplayName("全部 DELETE 标记的行应无可见数据")
    void allDeletedReturnsEmpty() {
        KeyValueVisibilityResolver resolver = new KeyValueVisibilityResolver();
        List<KeyValue> values = List.of(
            kv("row1", "", "name", 300, true, null),
            kv("row1", "", "age", 200, true, null)
        );

        List<KeyValue> resolved = resolver.resolveLatestValues(values);

        assertTrue(resolved.isEmpty());
    }

    @Test
    @DisplayName("空输入返回空列表")
    void emptyInputReturnsEmpty() {
        KeyValueVisibilityResolver resolver = new KeyValueVisibilityResolver();
        List<KeyValue> resolved = resolver.resolveLatestValues(Collections.emptyList());
        assertTrue(resolved.isEmpty());
    }

    @Test
    @DisplayName("单个 PUT 可见")
    void singlePutVisible() {
        KeyValueVisibilityResolver resolver = new KeyValueVisibilityResolver();
        List<KeyValue> values = List.of(
            kv("row1", "", "name", 100, false, "alice".getBytes())
        );

        List<KeyValue> resolved = resolver.resolveLatestValues(values);

        assertEquals(1, resolved.size());
        assertEquals("name", resolved.get(0).getQualifier());
        assertEquals("alice", new String(resolved.get(0).getValue()));
    }

    @Test
    @DisplayName("多行 KeyValue 正确按行分组解析")
    void multiRowResolution() {
        KeyValueVisibilityResolver resolver = new KeyValueVisibilityResolver();
        List<KeyValue> values = List.of(
            kv("row1", "", "name", 100, false, "alice".getBytes()),
            kv("row1", "", "age", 100, false, "20".getBytes()),
            kv("row2", "", "name", 100, false, "bob".getBytes()),
            kv("row2", "", "age", 100, false, "25".getBytes())
        );

        List<KeyValue> resolved = resolver.resolveLatestValues(values);

        assertEquals(4, resolved.size());
    }

    @Test
    @DisplayName("行级 DELETE 标记隐藏所有较早的列（输入按 timestamp 降序）")
    void rowDeleteHidesOlderColumns() {
        KeyValueVisibilityResolver resolver = new KeyValueVisibilityResolver();
        // 输入按 timestamp 降序：最新的 PUT 先出现，使用 putIfAbsent 保留首个（最新）版本
        List<KeyValue> values = List.of(
            kv("row1", "", "name", 300, false, "new".getBytes()),   // 比 DELETE 新，可见
            kv("row1", "", "", 200, true, null),                      // 行级 DELETE
            kv("row1", "", "name", 100, false, "old".getBytes())    // 比 DELETE 旧，被 putIfAbsent 忽略
        );

        List<KeyValue> resolved = resolver.resolveLatestValues(values);

        assertEquals(1, resolved.size());
        assertEquals("new", new String(resolved.get(0).getValue()));
    }

    @Test
    @DisplayName("列级 DELETE 标记只隐藏对应列的较旧版本（输入按 timestamp 降序）")
    void columnDeleteHidesOnlyTargetColumn() {
        KeyValueVisibilityResolver resolver = new KeyValueVisibilityResolver();
        // 输入按 timestamp 降序：DELETE 先出现建立过滤条件，后续 PUT 若 timestamp <= DELETE 则被过滤
        List<KeyValue> values = List.of(
            kv("row1", "", "name", 200, true, null),        // name 列 DELETE at 200
            kv("row1", "", "age", 100, false, "20".getBytes()),   // age PUT，无 DELETE，可见
            kv("row1", "", "name", 100, false, "alice".getBytes()) // name PUT at 100 <= DELETE 200，被过滤
        );

        List<KeyValue> resolved = resolver.resolveLatestValues(values);

        assertEquals(1, resolved.size());
        assertEquals("age", resolved.get(0).getQualifier());
    }

    private KeyValue kv(String rowKey, String family, String qualifier, long timestamp, boolean delete, byte[] value) {
        KeyValue kv = new KeyValue.Builder(rowKey.getBytes())
            .family(family)
            .qualifier(qualifier)
            .timestamp(timestamp)
            .value(value)
            .build();
        kv.setType(delete ? KeyValue.Type.DELETE : KeyValue.Type.PUT);
        return kv;
    }
}
