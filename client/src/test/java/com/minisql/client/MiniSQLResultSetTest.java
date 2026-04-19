package com.minisql.client;

import com.minisql.sql.execution.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MiniSQLResultSet tests")
class MiniSQLResultSetTest {

    private MiniSQLResultSet rs;

    private static Row row(String col1, Object val1) {
        Row r = new Row();
        r.addColumn(col1, val1);
        return r;
    }

    private static Row row(String col1, Object val1, String col2, Object val2) {
        Row r = new Row();
        r.addColumn(col1, val1);
        r.addColumn(col2, val2);
        return r;
    }

    private static Row row(String col1, Object val1, String col2, Object val2,
                           String col3, Object val3) {
        Row r = new Row();
        r.addColumn(col1, val1);
        r.addColumn(col2, val2);
        r.addColumn(col3, val3);
        return r;
    }

    // --------------------------------------------------------------- next()

    @Nested
    @DisplayName("next() cursor movement")
    class NextCursor {

        @BeforeEach
        void initCursor() {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id", "name", "score"));
            rs.addRow(row("id", 1, "name", "alice", "score", 95));
            rs.addRow(row("id", 2, "name", "bob", "score", 88));
            rs.addRow(row("id", 3, "name", "charlie", "score", 70));
        }

        @Test
        @DisplayName("next returns true and advances through all rows")
        void advancesThroughAllRows() throws SQLException {
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next()); // past the last row
        }

        @Test
        @DisplayName("isBeforeFirst is true before first next()")
        void isBeforeFirst() throws SQLException {
            assertTrue(rs.isBeforeFirst());
            rs.next();
            assertFalse(rs.isBeforeFirst());
        }

        @Test
        @DisplayName("isFirst is true only on the first row")
        void isFirst() throws SQLException {
            assertFalse(rs.isFirst());
            rs.next();
            assertTrue(rs.isFirst());
            rs.next();
            assertFalse(rs.isFirst());
        }

        @Test
        @DisplayName("isLast is true only on the last row")
        void isLast() throws SQLException {
            rs.next();
            assertFalse(rs.isLast());
            rs.next();
            assertFalse(rs.isLast());
            rs.next();
            assertTrue(rs.isLast());
        }

        @Test
        @DisplayName("isAfterLast is false when on last row")
        void isAfterLast() throws SQLException {
            assertFalse(rs.isAfterLast());
            rs.next();
            rs.next();
            assertTrue(rs.next()); // last row
            assertFalse(rs.isAfterLast()); // still on last row, not past it
        }

        @Test
        @DisplayName("getRow returns 1-based row number")
        void getRow() throws SQLException {
            rs.next();
            assertEquals(1, rs.getRow());
            rs.next();
            assertEquals(2, rs.getRow());
            rs.next();
            assertEquals(3, rs.getRow());
        }
    }

    // -------------------------------------------------------- empty ResultSet

    @Nested
    @DisplayName("Empty ResultSet")
    class EmptyResultSet {

        @BeforeEach
        void setUp() {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
        }

        @Test
        @DisplayName("next returns false immediately with no rows")
        void nextReturnsFalseOnEmpty() throws SQLException {
            assertFalse(rs.next());
        }

        @Test
        @DisplayName("first returns false on empty result set")
        void firstReturnsFalse() throws SQLException {
            assertFalse(rs.first());
        }

        @Test
        @DisplayName("last returns false on empty result set")
        void lastReturnsFalse() throws SQLException {
            assertFalse(rs.last());
        }
    }

    // ------------------------------------------------ getString/getInt/etc.

    @Nested
    @DisplayName("Getter methods")
    class GetterMethods {

        private Row fiveColRow() {
            Row r = new Row();
            r.addColumn("id", 1);
            r.addColumn("name", "alice");
            r.addColumn("score", 95);
            r.addColumn("salary", 50000.0);
            r.addColumn("active", "true");
            return r;
        }

        @BeforeEach
        void initResultSet() {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id", "name", "score", "salary", "active"));
            rs.addRow(fiveColRow());
        }

        @Test
        @DisplayName("getString by index returns string representation")
        void getStringByIndex() throws SQLException {
            rs.next();
            assertEquals("1", rs.getString(1));
            assertEquals("alice", rs.getString(2));
        }

        @Test
        @DisplayName("getString by label returns value")
        void getStringByLabel() throws SQLException {
            rs.next();
            assertEquals("alice", rs.getString("name"));
        }

        @Test
        @DisplayName("getInt by index returns parsed integer")
        void getIntByIndex() throws SQLException {
            rs.next();
            assertEquals(1, rs.getInt(1));
        }

        @Test
        @DisplayName("getInt by label returns parsed integer")
        void getIntByLabel() throws SQLException {
            rs.next();
            assertEquals(95, rs.getInt("score"));
        }

        @Test
        @DisplayName("getLong by index returns parsed long")
        void getLongByIndex() throws SQLException {
            rs.next();
            assertEquals(1L, rs.getLong(1));
        }

        @Test
        @DisplayName("getLong by label returns parsed long")
        void getLongByLabel() throws SQLException {
            rs.next();
            assertEquals(95L, rs.getLong("score"));
        }

        @Test
        @DisplayName("getDouble by index returns parsed double")
        void getDoubleByIndex() throws SQLException {
            rs.next();
            assertEquals(50000.0, rs.getDouble(4), 0.001);
        }

        @Test
        @DisplayName("getDouble by label returns parsed double")
        void getDoubleByLabel() throws SQLException {
            rs.next();
            assertEquals(50000.0, rs.getDouble("salary"), 0.001);
        }

        @Test
        @DisplayName("getObject by index returns the raw value")
        void getObjectByIndex() throws SQLException {
            rs.next();
            assertEquals(1, rs.getObject(1));
            assertEquals("alice", rs.getObject(2));
        }

        @Test
        @DisplayName("getObject by label returns the raw value")
        void getObjectByLabel() throws SQLException {
            rs.next();
            assertEquals(95, rs.getObject("score"));
        }

        @Test
        @DisplayName("getString returns null when row value is null")
        void getStringReturnsNullForNullValue() throws SQLException {
            Row r = new Row();
            r.addColumn("id", 1);
            r.addColumn("name", null);
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id", "name"));
            rs.addRow(r);
            rs.next();

            assertNull(rs.getString("name"));
        }

        @Test
        @DisplayName("getInt returns 0 when row value is null")
        void getIntReturnsZeroForNullValue() throws SQLException {
            Row r = new Row();
            r.addColumn("id", null);
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.addRow(r);
            rs.next();

            assertEquals(0, rs.getInt("id"));
        }

        @Test
        @DisplayName("getLong returns 0 when row value is null")
        void getLongReturnsZeroForNullValue() throws SQLException {
            Row r = new Row();
            r.addColumn("id", null);
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.addRow(r);
            rs.next();

            assertEquals(0L, rs.getLong("id"));
        }

        @Test
        @DisplayName("getDouble returns 0.0 when row value is null")
        void getDoubleReturnsZeroForNullValue() throws SQLException {
            Row r = new Row();
            r.addColumn("val", null);
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("val"));
            rs.addRow(r);
            rs.next();

            assertEquals(0.0, rs.getDouble("val"), 0.001);
        }
    }

    // ----------------------------------------------- out-of-bounds / errors

    @Nested
    @DisplayName("Error and edge cases")
    class ErrorCases {

        @Test
        @DisplayName("getString throws before next() is called")
        void getStringBeforeNextThrows() {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.addRow(row("id", 1));

            assertThrows(SQLException.class, () -> rs.getString("id"));
        }

        @Test
        @DisplayName("getString with invalid column index throws SQLException")
        void getStringInvalidIndex() throws SQLException {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.addRow(row("id", 1));
            rs.next();

            assertThrows(SQLException.class, () -> rs.getString(0));
            assertThrows(SQLException.class, () -> rs.getString(5));
        }

        @Test
        @DisplayName("getString with non-existent column label returns null")
        void getStringInvalidLabel() throws SQLException {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.addRow(row("id", 1));
            rs.next();

            assertNull(rs.getString("nonexistent"));
        }
    }

    // -------------------------------------------------------- findColumn

    @Nested
    @DisplayName("findColumn")
    class FindColumn {

        @BeforeEach
        void setUp() {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id", "name", "score"));
        }

        @Test
        @DisplayName("findColumn returns 1-based index for existing column")
        void returnsCorrectIndex() throws SQLException {
            assertEquals(1, rs.findColumn("id"));
            assertEquals(2, rs.findColumn("name"));
            assertEquals(3, rs.findColumn("score"));
        }

        @Test
        @DisplayName("findColumn is case-insensitive")
        void caseInsensitive() throws SQLException {
            assertEquals(1, rs.findColumn("ID"));
            assertEquals(2, rs.findColumn("NAME"));
            assertEquals(3, rs.findColumn("Score"));
        }

        @Test
        @DisplayName("findColumn throws for non-existent column")
        void throwsForMissingColumn() {
            assertThrows(SQLException.class, () -> rs.findColumn("missing"));
        }
    }

    // ----------------------------------------------------------- close

    @Nested
    @DisplayName("close behavior")
    class Close {

        @Test
        @DisplayName("close marks the ResultSet as closed")
        void closeSetsClosedFlag() throws SQLException {
            rs = new MiniSQLResultSet();
            assertFalse(rs.isClosed());
            rs.close();
            assertTrue(rs.isClosed());
        }

        @Test
        @DisplayName("next throws after close")
        void nextThrowsAfterClose() throws SQLException {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.addRow(row("id", 1));
            rs.close();

            assertThrows(SQLException.class, () -> rs.next());
        }

        @Test
        @DisplayName("getString throws after close")
        void getStringThrowsAfterClose() throws SQLException {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.addRow(row("id", 1));
            rs.close();

            assertThrows(SQLException.class, () -> rs.getString("id"));
        }

        @Test
        @DisplayName("findColumn throws after close")
        void findColumnThrowsAfterClose() throws SQLException {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.close();

            assertThrows(SQLException.class, () -> rs.findColumn("id"));
        }

        @Test
        @DisplayName("getMetaData throws after close")
        void getMetaDataThrowsAfterClose() throws SQLException {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.close();

            assertThrows(SQLException.class, () -> rs.getMetaData());
        }
    }

    // -------------------------------------------------------- getMetaData

    @Nested
    @DisplayName("getMetaData")
    class GetMetaData {

        @Test
        @DisplayName("getMetaData returns MiniSQLResultSetMetaData with correct column count")
        void returnsCorrectMetaData() throws SQLException {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id", "name", "score"));
            rs.setColumnTypes(Arrays.asList("INT", "VARCHAR", "DOUBLE"));

            ResultSetMetaData md = rs.getMetaData();
            assertNotNull(md);
            assertEquals(3, md.getColumnCount());
            assertEquals("id", md.getColumnLabel(1));
            assertEquals("name", md.getColumnLabel(2));
            assertEquals("score", md.getColumnLabel(3));
        }
    }

    // --------------------------------------------- cursor positioning

    @Nested
    @DisplayName("Cursor positioning methods")
    class CursorPositioning {

        @BeforeEach
        void setUp() {
            rs = new MiniSQLResultSet();
            rs.setColumnNames(Arrays.asList("id"));
            rs.addRow(row("id", 1));
            rs.addRow(row("id", 2));
            rs.addRow(row("id", 3));
        }

        @Test
        @DisplayName("first moves cursor to the first row")
        void first() throws SQLException {
            assertTrue(rs.first());
            assertEquals("1", rs.getString("id"));
        }

        @Test
        @DisplayName("last moves cursor to the last row")
        void last() throws SQLException {
            assertTrue(rs.last());
            assertEquals("3", rs.getString("id"));
        }

        @Test
        @DisplayName("beforeFirst resets cursor before the first row")
        void beforeFirst() throws SQLException {
            rs.next();
            rs.beforeFirst();
            assertTrue(rs.isBeforeFirst());
        }

        @Test
        @DisplayName("previous moves cursor back one row")
        void previous() throws SQLException {
            rs.next(); // row 1
            rs.next(); // row 2
            assertTrue(rs.previous());
            assertEquals("1", rs.getString("id"));
        }

        @Test
        @DisplayName("previous returns false when already at first row")
        void previousAtFirstRow() throws SQLException {
            rs.next(); // row 1
            assertFalse(rs.previous());
        }
    }
}
