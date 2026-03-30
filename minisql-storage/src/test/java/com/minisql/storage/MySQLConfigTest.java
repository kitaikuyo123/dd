package com.minisql.storage;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@DisplayName("MySQLConfig tests")
class MySQLConfigTest {

    private static final String MYSQL_URL_ENV = "MINISQL_TEST_MYSQL_URL";
    private static final String MYSQL_USER_ENV = "MINISQL_TEST_MYSQL_USER";
    private static final String MYSQL_PASSWORD_ENV = "MINISQL_TEST_MYSQL_PASSWORD";

    @Test
    @DisplayName("builder applies explicit values")
    void testBuilderPattern() {
        MySQLConfig config = MySQLConfig.builder("jdbc:mysql://localhost:3306/test", "user", "pass")
            .maxPoolSize(10)
            .connectionTimeout(20000L)
            .autoCreateSchema(false)
            .build();

        assertEquals("jdbc:mysql://localhost:3306/test", config.getJdbcUrl());
        assertEquals("user", config.getUsername());
        assertEquals("pass", config.getPassword());
        assertEquals(10, config.getMaxPoolSize());
        assertEquals(20000L, config.getConnectionTimeout());
        assertFalse(config.isAutoCreateSchema());
    }

    @Test
    @DisplayName("builder uses documented defaults")
    void testDefaultValues() {
        MySQLConfig config = MySQLConfig.builder("jdbc:mysql://localhost:3306/test", "user", "pass")
            .build();

        assertEquals(20, config.getMaxPoolSize());
        assertEquals(30000L, config.getConnectionTimeout());
        assertTrue(config.isAutoCreateSchema());
    }

    @Test
    @DisplayName("createDataSource requires explicit integration env")
    void testCreateDataSource() {
        assumeTrue(hasMySqlIntegrationEnv(),
            () -> "Skipping MySQL datasource integration test because "
                + MYSQL_URL_ENV + "/" + MYSQL_USER_ENV + "/" + MYSQL_PASSWORD_ENV + " are not set");

        MySQLConfig config = MySQLConfig.builder(
                requireEnv(MYSQL_URL_ENV),
                requireEnv(MYSQL_USER_ENV),
                requireEnv(MYSQL_PASSWORD_ENV))
            .maxPoolSize(5)
            .build();

        HikariDataSource dataSource = config.createDataSource();

        assertNotNull(dataSource);
        assertEquals(5, dataSource.getMaximumPoolSize());
        dataSource.close();
    }

    @Test
    @DisplayName("builder methods are chainable")
    void testChaining() {
        MySQLConfig.Builder builder = MySQLConfig.builder("jdbc:mysql://localhost:3306/test", "user", "pass");

        MySQLConfig.Builder result = builder.maxPoolSize(15);
        assertSame(builder, result);

        result = builder.connectionTimeout(45000L);
        assertSame(builder, result);

        result = builder.autoCreateSchema(true);
        assertSame(builder, result);
    }

    @Test
    @DisplayName("builder can build multiple configs")
    void testMultipleBuilds() {
        MySQLConfig.Builder builder = MySQLConfig.builder("jdbc:mysql://localhost:3306/test", "user", "pass");

        MySQLConfig config1 = builder.maxPoolSize(10).build();
        MySQLConfig config2 = builder.maxPoolSize(20).build();

        assertEquals(10, config1.getMaxPoolSize());
        assertEquals(20, config2.getMaxPoolSize());
    }

    private static boolean hasMySqlIntegrationEnv() {
        return !isBlank(System.getenv(MYSQL_URL_ENV))
            && !isBlank(System.getenv(MYSQL_USER_ENV))
            && !isBlank(System.getenv(MYSQL_PASSWORD_ENV));
    }

    private static String requireEnv(String key) {
        return System.getenv(key);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
