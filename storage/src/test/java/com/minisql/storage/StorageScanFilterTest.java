package com.minisql.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("StorageScanFilter 单元测试")
class StorageScanFilterTest {

    // ---- 构造函数与 getter ----

    @Nested
    @DisplayName("构造函数与字段访问")
    class ConstructorAndGetters {

        @Test
        @DisplayName("构造函数正确保存所有字段")
        void storesAllFields() {
            byte[] start = "start".getBytes();
            byte[] end = "end".getBytes();
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "=", "val".getBytes());
            List<StorageColumnPredicate> preds = List.of(pred);
            List<String> quals = List.of("name", "age");

            StorageScanFilter filter = new StorageScanFilter(start, end, preds, quals);

            assertArrayEquals(start, filter.getStartKey());
            assertArrayEquals(end, filter.getEndKey());
            assertEquals(1, filter.getColumnPredicates().size());
            assertEquals(2, filter.getProjectedQualifiers().size());
        }

        @Test
        @DisplayName("columnPredicates 为 null 时返回空列表")
        void nullColumnPredicatesReturnsEmptyList() {
            StorageScanFilter filter = new StorageScanFilter(
                new byte[]{0}, new byte[]{1}, null, null);

            assertTrue(filter.getColumnPredicates().isEmpty());
            assertTrue(filter.getProjectedQualifiers().isEmpty());
        }

        @Test
        @DisplayName("getStartKey 返回防御性拷贝")
        void startKeyDefensiveCopy() {
            byte[] original = new byte[]{1, 2, 3};
            StorageScanFilter filter = new StorageScanFilter(original, new byte[]{4}, null, null);

            byte[] got = filter.getStartKey();
            got[0] = 99;

            assertEquals(1, filter.getStartKey()[0]);
        }

        @Test
        @DisplayName("getEndKey 返回防御性拷贝")
        void endKeyDefensiveCopy() {
            byte[] original = new byte[]{4, 5, 6};
            StorageScanFilter filter = new StorageScanFilter(new byte[]{1}, original, null, null);

            byte[] got = filter.getEndKey();
            got[0] = 99;

            assertEquals(4, filter.getEndKey()[0]);
        }

        @Test
        @DisplayName("构造时对 startKey 做拷贝，修改原始不影响 filter")
        void constructorCopiesStartKey() {
            byte[] original = new byte[]{1, 2, 3};
            StorageScanFilter filter = new StorageScanFilter(original, new byte[]{4}, null, null);

            original[0] = 99;

            assertEquals(1, filter.getStartKey()[0]);
        }

        @Test
        @DisplayName("构造时对 endKey 做拷贝，修改原始不影响 filter")
        void constructorCopiesEndKey() {
            byte[] original = new byte[]{4, 5, 6};
            StorageScanFilter filter = new StorageScanFilter(new byte[]{1}, original, null, null);

            original[0] = 99;

            assertEquals(4, filter.getEndKey()[0]);
        }

        @Test
        @DisplayName("columnPredicates 返回不可变列表")
        void columnPredicatesIsUnmodifiable() {
            StorageColumnPredicate pred = new StorageColumnPredicate("col", "=", "v".getBytes());
            StorageScanFilter filter = new StorageScanFilter(
                new byte[]{0}, new byte[]{1}, new ArrayList<>(List.of(pred)), null);

            assertThrows(UnsupportedOperationException.class,
                () -> filter.getColumnPredicates().add(pred));
        }

        @Test
        @DisplayName("projectedQualifiers 返回不可变列表")
        void projectedQualifiersIsUnmodifiable() {
            StorageScanFilter filter = new StorageScanFilter(
                new byte[]{0}, new byte[]{1}, null, new ArrayList<>(List.of("a")));

            assertThrows(UnsupportedOperationException.class,
                () -> filter.getProjectedQualifiers().add("b"));
        }

        @Test
        @DisplayName("startKey 和 endKey 为 null 时 getter 返回 null")
        void nullKeysReturnNull() {
            StorageScanFilter filter = new StorageScanFilter(null, null, null, null);

            assertNull(filter.getStartKey());
            assertNull(filter.getEndKey());
        }
    }

    // ---- hasColumnPredicates ----

    @Nested
    @DisplayName("hasColumnPredicates")
    class HasColumnPredicates {

        @Test
        @DisplayName("有谓词时返回 true")
        void returnsTrueWhenPredicatesPresent() {
            StorageColumnPredicate pred = new StorageColumnPredicate("c", "=", "v".getBytes());
            StorageScanFilter filter = new StorageScanFilter(
                null, null, List.of(pred), null);

            assertTrue(filter.hasColumnPredicates());
        }

        @Test
        @DisplayName("无谓词时返回 false")
        void returnsFalseWhenNoPredicates() {
            StorageScanFilter filter = new StorageScanFilter(null, null, null, null);

            assertFalse(filter.hasColumnPredicates());
        }

        @Test
        @DisplayName("空列表时返回 false")
        void returnsFalseWhenEmptyList() {
            StorageScanFilter filter = new StorageScanFilter(
                null, null, Collections.emptyList(), null);

            assertFalse(filter.hasColumnPredicates());
        }
    }

    // ---- hasProjectedQualifiers ----

    @Nested
    @DisplayName("hasProjectedQualifiers")
    class HasProjectedQualifiers {

        @Test
        @DisplayName("有投影限定符时返回 true")
        void returnsTrueWhenQualifiersPresent() {
            StorageScanFilter filter = new StorageScanFilter(
                null, null, null, List.of("name"));

            assertTrue(filter.hasProjectedQualifiers());
        }

        @Test
        @DisplayName("无投影限定符时返回 false")
        void returnsFalseWhenNoQualifiers() {
            StorageScanFilter filter = new StorageScanFilter(null, null, null, null);

            assertFalse(filter.hasProjectedQualifiers());
        }

        @Test
        @DisplayName("空列表时返回 false")
        void returnsFalseWhenEmptyList() {
            StorageScanFilter filter = new StorageScanFilter(
                null, null, null, Collections.emptyList());

            assertFalse(filter.hasProjectedQualifiers());
        }
    }

    // ---- Builder ----

    @Nested
    @DisplayName("Builder 模式")
    class BuilderPattern {

        @Test
        @DisplayName("builder() 创建新 Builder 实例")
        void builderCreatesNewInstance() {
            StorageScanFilter.Builder b1 = StorageScanFilter.builder();
            StorageScanFilter.Builder b2 = StorageScanFilter.builder();

            assertNotSame(b1, b2);
        }

        @Test
        @DisplayName("newBuilder() 是 builder() 的别名")
        void newBuilderIsAliasForBuilder() {
            StorageScanFilter.Builder b1 = StorageScanFilter.builder();
            StorageScanFilter.Builder b2 = StorageScanFilter.newBuilder();

            assertNotNull(b1);
            assertNotNull(b2);
            assertNotSame(b1, b2);
        }

        @Test
        @DisplayName("Builder 方法可链式调用")
        void builderMethodsAreChainable() {
            StorageScanFilter.Builder builder = StorageScanFilter.builder();

            StorageScanFilter.Builder result = builder
                .startKey(new byte[]{0})
                .endKey(new byte[]{1})
                .columnPredicates(null)
                .projectedQualifiers(null);

            assertSame(builder, result);
        }

        @Test
        @DisplayName("Builder 构建出完整的 StorageScanFilter")
        void builderBuildsCompleteFilter() {
            byte[] start = "s".getBytes();
            byte[] end = "e".getBytes();
            StorageColumnPredicate pred = new StorageColumnPredicate("c", "=", "v".getBytes());
            List<StorageColumnPredicate> preds = List.of(pred);
            List<String> quals = List.of("q1", "q2");

            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(start)
                .endKey(end)
                .columnPredicates(preds)
                .projectedQualifiers(quals)
                .build();

            assertArrayEquals(start, filter.getStartKey());
            assertArrayEquals(end, filter.getEndKey());
            assertEquals(preds, filter.getColumnPredicates());
            assertEquals(quals, filter.getProjectedQualifiers());
        }

        @Test
        @DisplayName("Builder 默认谓词和投影为空列表")
        void builderDefaultsToEmptyLists() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(null)
                .endKey(null)
                .build();

            assertTrue(filter.getColumnPredicates().isEmpty());
            assertTrue(filter.getProjectedQualifiers().isEmpty());
            assertFalse(filter.hasColumnPredicates());
            assertFalse(filter.hasProjectedQualifiers());
        }

        @Test
        @DisplayName("Builder.startKey 做防御性拷贝")
        void builderStartKeyDefensiveCopy() {
            byte[] original = new byte[]{1, 2, 3};
            StorageScanFilter.Builder builder = StorageScanFilter.builder().startKey(original);

            original[0] = 99;

            StorageScanFilter filter = builder.build();
            assertEquals(1, filter.getStartKey()[0]);
        }

        @Test
        @DisplayName("Builder.endKey 做防御性拷贝")
        void builderEndKeyDefensiveCopy() {
            byte[] original = new byte[]{4, 5, 6};
            StorageScanFilter.Builder builder = StorageScanFilter.builder().endKey(original);

            original[0] = 99;

            StorageScanFilter filter = builder.build();
            assertEquals(4, filter.getEndKey()[0]);
        }

        @Test
        @DisplayName("同一 Builder 可多次构建不同的 filter")
        void builderCanBeReused() {
            StorageScanFilter.Builder builder = StorageScanFilter.builder();

            StorageScanFilter f1 = builder.startKey("a".getBytes()).build();
            StorageScanFilter f2 = builder.startKey("b".getBytes()).build();

            assertArrayEquals("a".getBytes(), f1.getStartKey());
            assertArrayEquals("b".getBytes(), f2.getStartKey());
        }

        @Test
        @DisplayName("build 不传 columnPredicates 默认空列表")
        void buildWithoutColumnPredicates() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(null)
                .endKey(null)
                .build();

            assertNotNull(filter.getColumnPredicates());
            assertTrue(filter.getColumnPredicates().isEmpty());
        }

        @Test
        @DisplayName("build 不传 projectedQualifiers 默认空列表")
        void buildWithoutProjectedQualifiers() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(null)
                .endKey(null)
                .build();

            assertNotNull(filter.getProjectedQualifiers());
            assertTrue(filter.getProjectedQualifiers().isEmpty());
        }
    }

    // ---- copy 工具方法间接测试 ----

    @Nested
    @DisplayName("copy 辅助方法")
    class CopyHelper {

        @Test
        @DisplayName("startKey 和 endKey 为 null 时 getter 不抛异常")
        void nullKeysHandledGracefully() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(null)
                .endKey(null)
                .build();

            assertNull(filter.getStartKey());
            assertNull(filter.getEndKey());
        }

        @Test
        @DisplayName("空字节数组作为 key")
        void emptyByteArrayKey() {
            StorageScanFilter filter = StorageScanFilter.builder()
                .startKey(new byte[0])
                .endKey(new byte[0])
                .build();

            assertNotNull(filter.getStartKey());
            assertNotNull(filter.getEndKey());
            assertEquals(0, filter.getStartKey().length);
            assertEquals(0, filter.getEndKey().length);
        }
    }
}
