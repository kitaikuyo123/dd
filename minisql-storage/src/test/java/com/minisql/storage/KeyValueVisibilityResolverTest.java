package com.minisql.storage;

import com.minisql.common.model.KeyValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeyValueVisibilityResolverTest {

    @Test
    void keepsLatestVisibleCellsAcrossDeletes() {
        KeyValueVisibilityResolver resolver = new KeyValueVisibilityResolver();
        List<KeyValue> values = List.of(
            keyValue("row1", "", "", 300, true, null),
            keyValue("row1", "", "name", 400, false, "alice2".getBytes()),
            keyValue("row1", "", "name", 200, false, "alice1".getBytes()),
            keyValue("row1", "", "price", 100, false, "18".getBytes())
        );

        List<KeyValue> resolved = resolver.resolveLatestValues(values);

        assertEquals(1, resolved.size());
        assertEquals("name", resolved.get(0).getQualifier());
        assertEquals("alice2", new String(resolved.get(0).getValue()));
    }

    private KeyValue keyValue(String rowKey, String family, String qualifier, long timestamp, boolean delete, byte[] value) {
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
