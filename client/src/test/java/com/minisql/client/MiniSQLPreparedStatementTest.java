package com.minisql.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MiniSQLPreparedStatement tests")
class MiniSQLPreparedStatementTest {

    private MiniSQLPreparedStatement ps;

    @BeforeEach
    void setUp() {
        ps = new MiniSQLPreparedStatement(null, "SELECT * FROM users WHERE id = ? AND name = ?");
    }

    // ------------------------------------------------------------------ helpers

    private String invokeBuildSql(MiniSQLPreparedStatement stmt) throws Exception {
        Method m = MiniSQLPreparedStatement.class.getDeclaredMethod("buildSql");
        m.setAccessible(true);
        return (String) m.invoke(stmt);
    }

    private String invokeFormatValue(MiniSQLPreparedStatement stmt, Object value) throws Exception {
        Method m = MiniSQLPreparedStatement.class.getDeclaredMethod("formatValue", Object.class);
        m.setAccessible(true);
        return (String) m.invoke(stmt, value);
    }

    // ---------------------------------------------------------------- buildSql

    @Nested
    @DisplayName("buildSql parameter substitution")
    class BuildSql {

        @Test
        @DisplayName("returns original SQL when no parameters are set")
        void noParameters() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null,
                    "SELECT * FROM users");

            assertEquals("SELECT * FROM users", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("substitutes a single int parameter, unset ones become NULL")
        void singleIntParameter() throws Exception {
            ps.setInt(1, 42);
            // 第二个 ? 未设置，buildSql 会替换为 NULL
            assertEquals("SELECT * FROM users WHERE id = 42 AND name = NULL", invokeBuildSql(ps));
        }

        @Test
        @DisplayName("substitutes all positional parameters")
        void allParameters() throws Exception {
            ps.setInt(1, 7);
            ps.setString(2, "alice");

            assertEquals("SELECT * FROM users WHERE id = 7 AND name = 'alice'", invokeBuildSql(ps));
        }

        @Test
        @DisplayName("NULL parameter renders as unquoted NULL keyword")
        void nullParameter() throws Exception {
            ps.setNull(1, java.sql.Types.INTEGER);

            assertEquals("SELECT * FROM users WHERE id = NULL AND name = NULL", invokeBuildSql(ps));
        }

        @Test
        @DisplayName("substitutes multiple parameters of different types")
        void mixedTypes() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null,
                    "INSERT INTO t VALUES (?, ?, ?, ?, ?)");
            stmt.setInt(1, 1);
            stmt.setString(2, "hello");
            stmt.setLong(3, 999999L);
            stmt.setDouble(4, 3.14);
            stmt.setBoolean(5, true);

            assertEquals("INSERT INTO t VALUES (1, 'hello', 999999, 3.14, 1)", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("handles consecutive placeholders with no text between")
        void consecutivePlaceholders() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "?,?,?");
            stmt.setInt(1, 10);
            stmt.setString(2, "x");
            stmt.setBoolean(3, false);

            assertEquals("10,'x',0", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("preserves text after the last placeholder")
        void textAfterLastPlaceholder() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null,
                    "SELECT * FROM t WHERE a = ? ORDER BY id");
            stmt.setInt(1, 5);

            assertEquals("SELECT * FROM t WHERE a = 5 ORDER BY id", invokeBuildSql(stmt));
        }
    }

    // ------------------------------------------------------------- formatValue

    @Nested
    @DisplayName("formatValue type rendering")
    class FormatValue {

        @Test
        @DisplayName("Integer renders as plain number")
        void integerValue() throws Exception {
            assertEquals("42", invokeFormatValue(ps, 42));
        }

        @Test
        @DisplayName("Long renders as plain number")
        void longValue() throws Exception {
            assertEquals("999999", invokeFormatValue(ps, 999999L));
        }

        @Test
        @DisplayName("Double renders as decimal string")
        void doubleValue() throws Exception {
            assertEquals("3.14", invokeFormatValue(ps, 3.14));
        }

        @Test
        @DisplayName("String is wrapped in single quotes")
        void stringValue() throws Exception {
            assertEquals("'hello world'", invokeFormatValue(ps, "hello world"));
        }

        @Test
        @DisplayName("String with single quotes is escaped by doubling")
        void stringWithQuotes() throws Exception {
            assertEquals("'it''s'", invokeFormatValue(ps, "it's"));
        }

        @Test
        @DisplayName("Boolean true renders as 1")
        void booleanTrue() throws Exception {
            assertEquals("1", invokeFormatValue(ps, true));
        }

        @Test
        @DisplayName("Boolean false renders as 0")
        void booleanFalse() throws Exception {
            assertEquals("0", invokeFormatValue(ps, false));
        }

        @Test
        @DisplayName("Float renders as decimal string")
        void floatValue() throws Exception {
            assertEquals("2.5", invokeFormatValue(ps, 2.5f));
        }

        @Test
        @DisplayName("Unknown object type is wrapped in quotes via toString")
        void unknownType() throws Exception {
            assertEquals("'some-object'", invokeFormatValue(ps, new Object() {
                @Override
                public String toString() {
                    return "some-object";
                }
            }));
        }
    }

    // --------------------------------------------------------- setter methods

    @Nested
    @DisplayName("Parameter setter methods")
    class Setters {

        @Test
        @DisplayName("setInt stores parameter and formats as number")
        void setIntParameter() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT * FROM t WHERE id = ?");
            stmt.setInt(1, 100);

            assertEquals("SELECT * FROM t WHERE id = 100", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("setString stores parameter and formats as quoted string")
        void setStringParameter() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT * FROM t WHERE name = ?");
            stmt.setString(1, "bob");

            assertEquals("SELECT * FROM t WHERE name = 'bob'", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("setLong stores parameter and formats as number")
        void setLongParameter() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT * FROM t WHERE id = ?");
            stmt.setLong(1, 123456789L);

            assertEquals("SELECT * FROM t WHERE id = 123456789", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("setDouble stores parameter and formats as decimal")
        void setDoubleParameter() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT * FROM t WHERE v = ?");
            stmt.setDouble(1, 2.718);

            assertEquals("SELECT * FROM t WHERE v = 2.718", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("setBoolean true stores parameter and formats as 1")
        void setBooleanTrueParameter() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT ?");
            stmt.setBoolean(1, true);

            assertEquals("SELECT 1", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("setBoolean false stores parameter and formats as 0")
        void setBooleanFalseParameter() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT ?");
            stmt.setBoolean(1, false);

            assertEquals("SELECT 0", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("setNull stores null and renders as NULL")
        void setNullParameter() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT * FROM t WHERE v = ?");
            stmt.setNull(1, java.sql.Types.VARCHAR);

            assertEquals("SELECT * FROM t WHERE v = NULL", invokeBuildSql(stmt));
        }

        @Test
        @DisplayName("setObject stores arbitrary value")
        void setObjectParameter() throws Exception {
            MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT * FROM t WHERE id = ?");
            stmt.setObject(1, 99);

            assertEquals("SELECT * FROM t WHERE id = 99", invokeBuildSql(stmt));
        }
    }

    // ------------------------------------------------------ clearParameters

    @Nested
    @DisplayName("clearParameters")
    class ClearParameters {

        @Test
        @DisplayName("clearParameters removes all set values and reverts to original SQL")
        void clearsAllParameters() throws Exception {
            ps.setInt(1, 10);
            ps.setString(2, "test");
            ps.clearParameters();

            assertEquals("SELECT * FROM users WHERE id = ? AND name = ?", invokeBuildSql(ps));
        }
    }

    // --------------------------------------------------- parameter overwriting

    @Test
    @DisplayName("setting the same index twice overwrites the previous value")
    void overwritingParameter() throws Exception {
        MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT * FROM t WHERE id = ?");
        stmt.setInt(1, 1);
        stmt.setInt(1, 99);

        assertEquals("SELECT * FROM t WHERE id = 99", invokeBuildSql(stmt));
    }

    // ----------------------------------------------- non-sequential indices

    @Test
    @DisplayName("parameter indices are resolved by sequential occurrence, not by key")
    void parameterIndexIsSequential() throws Exception {
        MiniSQLPreparedStatement stmt = new MiniSQLPreparedStatement(null, "SELECT ?, ?");
        stmt.setInt(2, 20);
        stmt.setInt(1, 10);

        assertEquals("SELECT 10, 20", invokeBuildSql(stmt));
    }
}
