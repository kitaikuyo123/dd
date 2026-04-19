package com.minisql.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MiniSQLResultSetMetaData tests")
class MiniSQLResultSetMetaDataTest {

    private MiniSQLResultSetMetaData md;

    @BeforeEach
    void setUp() {
        md = new MiniSQLResultSetMetaData(
                Arrays.asList("id", "name", "score", "salary", "active", "created_at"),
                Arrays.asList("INT", "VARCHAR", "DOUBLE", "BIGINT", "BOOLEAN", "TIMESTAMP"),
                "users"
        );
    }

    // -------------------------------------------------------- getColumnCount

    @Nested
    @DisplayName("getColumnCount")
    class GetColumnCount {

        @Test
        @DisplayName("returns the number of columns")
        void returnsColumnCount() throws SQLException {
            assertEquals(6, md.getColumnCount());
        }

        @Test
        @DisplayName("returns 0 when column names list is null")
        void returnsZeroForNullList() throws SQLException {
            MiniSQLResultSetMetaData empty = new MiniSQLResultSetMetaData(null, null, null);
            assertEquals(0, empty.getColumnCount());
        }

        @Test
        @DisplayName("returns 0 when column names list is empty")
        void returnsZeroForEmptyList() throws SQLException {
            MiniSQLResultSetMetaData empty = new MiniSQLResultSetMetaData(
                    Collections.emptyList(), Collections.emptyList(), "t");
            assertEquals(0, empty.getColumnCount());
        }
    }

    // ------------------------------------------------------ getColumnLabel

    @Nested
    @DisplayName("getColumnLabel / getColumnName")
    class GetColumnLabel {

        @Test
        @DisplayName("getColumnLabel returns correct 1-based column name")
        void returnsLabel() throws SQLException {
            assertEquals("id", md.getColumnLabel(1));
            assertEquals("name", md.getColumnLabel(2));
            assertEquals("score", md.getColumnLabel(3));
        }

        @Test
        @DisplayName("getColumnName delegates to getColumnLabel")
        void columnNameSameAsLabel() throws SQLException {
            assertEquals(md.getColumnLabel(1), md.getColumnName(1));
            assertEquals(md.getColumnLabel(3), md.getColumnName(3));
        }

        @Test
        @DisplayName("getColumnLabel throws for index 0")
        void throwsForIndexZero() {
            assertThrows(SQLException.class, () -> md.getColumnLabel(0));
        }

        @Test
        @DisplayName("getColumnLabel throws for index beyond column count")
        void throwsForIndexBeyondCount() {
            assertThrows(SQLException.class, () -> md.getColumnLabel(7));
        }

        @Test
        @DisplayName("getColumnLabel throws for negative index")
        void throwsForNegativeIndex() {
            assertThrows(SQLException.class, () -> md.getColumnLabel(-1));
        }
    }

    // ---------------------------------------------------- getColumnTypeName

    @Nested
    @DisplayName("getColumnTypeName")
    class GetColumnTypeName {

        @Test
        @DisplayName("returns the declared type name for each column")
        void returnsTypeName() throws SQLException {
            assertEquals("INT", md.getColumnTypeName(1));
            assertEquals("VARCHAR", md.getColumnTypeName(2));
            assertEquals("DOUBLE", md.getColumnTypeName(3));
            assertEquals("BIGINT", md.getColumnTypeName(4));
            assertEquals("BOOLEAN", md.getColumnTypeName(5));
            assertEquals("TIMESTAMP", md.getColumnTypeName(6));
        }

        @Test
        @DisplayName("returns VARCHAR when column types list is empty")
        void returnsVarcharWhenTypesEmpty() throws SQLException {
            MiniSQLResultSetMetaData m = new MiniSQLResultSetMetaData(
                    Arrays.asList("col1"), Collections.emptyList(), "t");

            assertEquals("VARCHAR", m.getColumnTypeName(1));
        }

        @Test
        @DisplayName("throws for invalid index")
        void throwsForInvalidIndex() {
            assertThrows(SQLException.class, () -> md.getColumnTypeName(0));
            assertThrows(SQLException.class, () -> md.getColumnTypeName(99));
        }
    }

    // ------------------------------------------------------- getColumnType

    @Nested
    @DisplayName("getColumnType")
    class GetColumnType {

        @Test
        @DisplayName("INT maps to Types.INTEGER")
        void intType() throws SQLException {
            assertEquals(Types.INTEGER, md.getColumnType(1));
        }

        @Test
        @DisplayName("VARCHAR maps to Types.VARCHAR")
        void varcharType() throws SQLException {
            assertEquals(Types.VARCHAR, md.getColumnType(2));
        }

        @Test
        @DisplayName("DOUBLE maps to Types.DOUBLE")
        void doubleType() throws SQLException {
            assertEquals(Types.DOUBLE, md.getColumnType(3));
        }

        @Test
        @DisplayName("BIGINT maps to Types.BIGINT")
        void bigintType() throws SQLException {
            assertEquals(Types.BIGINT, md.getColumnType(4));
        }

        @Test
        @DisplayName("BOOLEAN maps to Types.BOOLEAN")
        void booleanType() throws SQLException {
            assertEquals(Types.BOOLEAN, md.getColumnType(5));
        }

        @Test
        @DisplayName("TIMESTAMP maps to Types.TIMESTAMP")
        void timestampType() throws SQLException {
            assertEquals(Types.TIMESTAMP, md.getColumnType(6));
        }

        @Test
        @DisplayName("returns Types.VARCHAR when column types list is empty")
        void defaultVarcharWhenTypesEmpty() throws SQLException {
            MiniSQLResultSetMetaData m = new MiniSQLResultSetMetaData(
                    Arrays.asList("col1"), Collections.emptyList(), "t");

            assertEquals(Types.VARCHAR, m.getColumnType(1));
        }

        @Test
        @DisplayName("returns Types.VARCHAR for unrecognized type names")
        void defaultVarcharForUnknownType() throws SQLException {
            MiniSQLResultSetMetaData m = new MiniSQLResultSetMetaData(
                    Arrays.asList("col1"), Arrays.asList("UNKNOWN_TYPE"), "t");

            assertEquals(Types.VARCHAR, m.getColumnType(1));
        }

        @Test
        @DisplayName("type lookup is case-insensitive")
        void caseInsensitive() throws SQLException {
            MiniSQLResultSetMetaData m = new MiniSQLResultSetMetaData(
                    Arrays.asList("a", "b"), Arrays.asList("int", "varchar"), "t");

            assertEquals(Types.INTEGER, m.getColumnType(1));
            assertEquals(Types.VARCHAR, m.getColumnType(2));
        }

        @Test
        @DisplayName("throws for invalid column index")
        void throwsForInvalidIndex() {
            assertThrows(SQLException.class, () -> md.getColumnType(0));
            assertThrows(SQLException.class, () -> md.getColumnType(99));
        }

        @Test
        @DisplayName("null type name falls back to VARCHAR")
        void nullTypeNameFallsBackToVarchar() throws SQLException {
            MiniSQLResultSetMetaData m = new MiniSQLResultSetMetaData(
                    Arrays.asList("col1"), Arrays.asList((String) null), "t");

            assertEquals(Types.VARCHAR, m.getColumnType(1));
        }
    }

    // ---------------------------------------------------------- getTableName

    @Nested
    @DisplayName("getTableName")
    class GetTableName {

        @Test
        @DisplayName("returns the table name")
        void returnsTableName() throws SQLException {
            assertEquals("users", md.getTableName(1));
        }

        @Test
        @DisplayName("returns empty string when table name is null")
        void returnsEmptyWhenNull() throws SQLException {
            MiniSQLResultSetMetaData m = new MiniSQLResultSetMetaData(
                    Arrays.asList("col1"), Arrays.asList("INT"), null);

            assertEquals("", m.getTableName(1));
        }
    }

    // ------------------------------------------------ additional type mappings

    @Nested
    @DisplayName("Additional type mappings")
    class AdditionalTypeMappings {

        private MiniSQLResultSetMetaData make(String type) {
            return new MiniSQLResultSetMetaData(
                    Arrays.asList("col"), Arrays.asList(type), "t");
        }

        @Test
        @DisplayName("INTEGER maps to Types.INTEGER")
        void integer() throws SQLException {
            assertEquals(Types.INTEGER, make("INTEGER").getColumnType(1));
        }

        @Test
        @DisplayName("LONG maps to Types.BIGINT")
        void longMapping() throws SQLException {
            assertEquals(Types.BIGINT, make("LONG").getColumnType(1));
        }

        @Test
        @DisplayName("FLOAT maps to Types.FLOAT")
        void floatMapping() throws SQLException {
            assertEquals(Types.FLOAT, make("FLOAT").getColumnType(1));
        }

        @Test
        @DisplayName("CHAR maps to Types.CHAR")
        void charMapping() throws SQLException {
            assertEquals(Types.CHAR, make("CHAR").getColumnType(1));
        }

        @Test
        @DisplayName("STRING maps to Types.VARCHAR")
        void stringMapping() throws SQLException {
            assertEquals(Types.VARCHAR, make("STRING").getColumnType(1));
        }

        @Test
        @DisplayName("TEXT maps to Types.VARCHAR")
        void textMapping() throws SQLException {
            assertEquals(Types.VARCHAR, make("TEXT").getColumnType(1));
        }

        @Test
        @DisplayName("DATE maps to Types.DATE")
        void dateMapping() throws SQLException {
            assertEquals(Types.DATE, make("DATE").getColumnType(1));
        }

        @Test
        @DisplayName("TIME maps to Types.TIME")
        void timeMapping() throws SQLException {
            assertEquals(Types.TIME, make("TIME").getColumnType(1));
        }

        @Test
        @DisplayName("BLOB maps to Types.BLOB")
        void blobMapping() throws SQLException {
            assertEquals(Types.BLOB, make("BLOB").getColumnType(1));
        }

        @Test
        @DisplayName("BINARY maps to Types.BINARY")
        void binaryMapping() throws SQLException {
            assertEquals(Types.BINARY, make("BINARY").getColumnType(1));
        }

        @Test
        @DisplayName("BYTE maps to Types.TINYINT")
        void byteMapping() throws SQLException {
            assertEquals(Types.TINYINT, make("BYTE").getColumnType(1));
        }

        @Test
        @DisplayName("SHORT maps to Types.SMALLINT")
        void shortMapping() throws SQLException {
            assertEquals(Types.SMALLINT, make("SHORT").getColumnType(1));
        }

        @Test
        @DisplayName("DECIMAL maps to Types.DECIMAL")
        void decimalMapping() throws SQLException {
            assertEquals(Types.DECIMAL, make("DECIMAL").getColumnType(1));
        }

        @Test
        @DisplayName("NUMERIC maps to Types.NUMERIC")
        void numericMapping() throws SQLException {
            assertEquals(Types.NUMERIC, make("NUMERIC").getColumnType(1));
        }

        @Test
        @DisplayName("REAL maps to Types.REAL")
        void realMapping() throws SQLException {
            assertEquals(Types.REAL, make("REAL").getColumnType(1));
        }
    }

    // ---------------------------------------------------- isSigned

    @Nested
    @DisplayName("isSigned")
    class IsSigned {

        @Test
        @DisplayName("INT column is signed")
        void intIsSigned() throws SQLException {
            assertTrue(md.isSigned(1));
        }

        @Test
        @DisplayName("BIGINT column is signed")
        void bigintIsSigned() throws SQLException {
            assertTrue(md.isSigned(4));
        }

        @Test
        @DisplayName("DOUBLE column is signed")
        void doubleIsSigned() throws SQLException {
            assertTrue(md.isSigned(3));
        }

        @Test
        @DisplayName("VARCHAR column is not signed")
        void varcharIsNotSigned() throws SQLException {
            assertFalse(md.isSigned(2));
        }

        @Test
        @DisplayName("BOOLEAN column is not signed")
        void booleanIsNotSigned() throws SQLException {
            assertFalse(md.isSigned(5));
        }
    }

    // ------------------------------------------------- getColumnClassName

    @Nested
    @DisplayName("getColumnClassName")
    class GetColumnClassName {

        @Test
        @DisplayName("INT returns Integer class name")
        void intClass() throws SQLException {
            assertEquals(Integer.class.getName(), md.getColumnClassName(1));
        }

        @Test
        @DisplayName("VARCHAR returns String class name")
        void varcharClass() throws SQLException {
            assertEquals(String.class.getName(), md.getColumnClassName(2));
        }

        @Test
        @DisplayName("DOUBLE returns Double class name")
        void doubleClass() throws SQLException {
            assertEquals(Double.class.getName(), md.getColumnClassName(3));
        }

        @Test
        @DisplayName("BIGINT returns Long class name")
        void bigintClass() throws SQLException {
            assertEquals(Long.class.getName(), md.getColumnClassName(4));
        }

        @Test
        @DisplayName("BOOLEAN returns Boolean class name")
        void booleanClass() throws SQLException {
            assertEquals(Boolean.class.getName(), md.getColumnClassName(5));
        }

        @Test
        @DisplayName("TIMESTAMP returns java.sql.Date class name")
        void timestampClass() throws SQLException {
            assertEquals(java.sql.Date.class.getName(), md.getColumnClassName(6));
        }
    }

    // ------------------------------------------- unwrap / isWrapperFor

    @Nested
    @DisplayName("unwrap and isWrapperFor")
    class Unwrap {

        @Test
        @DisplayName("isWrapperFor returns true for ResultSetMetaData")
        void isWrapperForCorrectType() throws SQLException {
            assertTrue(md.isWrapperFor(java.sql.ResultSetMetaData.class));
        }

        @Test
        @DisplayName("isWrapperFor returns false for unrelated interface")
        void isWrapperForWrongType() throws SQLException {
            assertFalse(md.isWrapperFor(java.sql.Connection.class));
        }

        @Test
        @DisplayName("unwrap returns same instance for correct type")
        void unwrapCorrectType() throws SQLException {
            java.sql.ResultSetMetaData unwrapped = md.unwrap(java.sql.ResultSetMetaData.class);
            assertSame(md, unwrapped);
        }

        @Test
        @DisplayName("unwrap throws for unsupported type")
        void unwrapThrowsForWrongType() {
            assertThrows(SQLException.class, () -> md.unwrap(java.sql.Connection.class));
        }
    }
}
