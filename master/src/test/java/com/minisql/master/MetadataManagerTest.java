package com.minisql.master;

import com.minisql.common.model.Column;
import com.minisql.common.model.Region;
import com.minisql.common.model.ServerId;
import com.minisql.common.model.Table;
import com.minisql.master.state.MetadataManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MetadataManager tests")
class MetadataManagerTest {

    @Test
    @DisplayName("createTable stores table metadata")
    void testCreateTable() {
        MetadataManager manager = new MetadataManager();
        Table table = new Table("users");
        table.addColumn(new Column("id", Column.ColumnType.INT));
        table.addColumn(new Column("name", Column.ColumnType.VARCHAR, 255));
        table.setPrimaryKey("id");

        manager.createTable(table);

        assertNotNull(manager.getTable("users"));
        assertTrue(manager.tableExists("users"));
    }

    @Test
    @DisplayName("deleteTable removes table metadata")
    void testDeleteTable() {
        MetadataManager manager = new MetadataManager();
        manager.createTable(new Table("users"));

        manager.deleteTable("users");

        assertNull(manager.getTable("users"));
        assertFalse(manager.tableExists("users"));
    }

    @Test
    @DisplayName("removeRegion removes region from in-memory metadata")
    void testRemoveRegionRemovesRegionFromMemory() {
        MetadataManager manager = new MetadataManager();
        Region region = new Region("region-1", "users", "a".getBytes(), "z".getBytes());

        manager.registerRegionForTable(region, new ServerId("localhost", 16020));
        assertNotNull(manager.getRegion("region-1"));
        assertEquals(1, manager.getRegionsForTable("users").size());

        manager.removeRegion("region-1");

        assertNull(manager.getRegion("region-1"));
        assertTrue(manager.getRegionsForTable("users").isEmpty());
    }
}
