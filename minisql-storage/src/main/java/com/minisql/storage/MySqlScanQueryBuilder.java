package com.minisql.storage;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds MySQL SQL strings used by scan/get/delete operations.
 */
public class MySqlScanQueryBuilder {

    public String insertSql(String tableName) {
        return "INSERT INTO " + tableName + " (row_key, family, qualifier, timestamp, value, is_deleted) "
            + "VALUES (?, ?, ?, ?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE value = VALUES(value), is_deleted = VALUES(is_deleted)";
    }

    public String getSql(String tableName) {
        return "SELECT row_key, family, qualifier, timestamp, value, is_deleted FROM " + tableName
            + " WHERE row_key = ? ORDER BY timestamp DESC, family, qualifier";
    }

    public String rangeScanSql(String tableName) {
        return "SELECT row_key, family, qualifier, timestamp, value, is_deleted FROM " + tableName
            + " WHERE row_key >= ? AND row_key < ? ORDER BY row_key, timestamp DESC, family, qualifier";
    }

    public String deleteSql(String tableName) {
        return "INSERT INTO " + tableName + " (row_key, family, qualifier, timestamp, value, is_deleted) VALUES (?, ?, ?, ?, NULL, 1)";
    }

    public String dropTableSql(String tableName) {
        return "DROP TABLE IF EXISTS " + tableName;
    }

    public String compactSql(String tableName) {
        return "DELETE FROM " + tableName + " "
            + "WHERE (row_key, family, qualifier, timestamp) NOT IN ("
            + "SELECT row_key, family, qualifier, timestamp FROM ("
            + "SELECT row_key, family, qualifier, timestamp, "
            + "ROW_NUMBER() OVER (PARTITION BY row_key, family, qualifier ORDER BY timestamp DESC) AS rn "
            + "FROM " + tableName
            + ") t WHERE rn <= 3)";
    }

    public String buildPredicateScanSql(String tableName, StorageScanFilter filter) {
        List<StorageColumnPredicate> predicates = filter.getColumnPredicates();
        List<String> qualifiers = new ArrayList<>(collectUniqueQualifiers(predicates));
        String qualifierPlaceholders = qualifiers.isEmpty() ? "" : String.join(", ", java.util.Collections.nCopies(qualifiers.size(), "?"));
        String projectionPlaceholders = filter.getProjectedQualifiers().isEmpty()
            ? ""
            : String.join(", ", java.util.Collections.nCopies(filter.getProjectedQualifiers().size(), "?"));

        StringBuilder sql = new StringBuilder();
        sql.append("WITH row_deletes AS (")
            .append(" SELECT row_key, MAX(timestamp) AS row_delete_ts")
            .append(" FROM ").append(tableName)
            .append(" WHERE row_key >= ? AND row_key < ?")
            .append(" AND family = '' AND qualifier = '' AND is_deleted = 1")
            .append(" GROUP BY row_key")
            .append("), predicate_latest AS (")
            .append(" SELECT row_key, qualifier, timestamp, value, is_deleted,")
            .append(" ROW_NUMBER() OVER (PARTITION BY row_key, qualifier ORDER BY timestamp DESC) AS rn")
            .append(" FROM ").append(tableName)
            .append(" WHERE row_key >= ? AND row_key < ?")
            .append(" AND family = ''");
        if (!qualifiers.isEmpty()) {
            sql.append(" AND qualifier IN (").append(qualifierPlaceholders).append(")");
        }
        sql.append("), predicate_visible AS (")
            .append(" SELECT pl.row_key, pl.qualifier, pl.timestamp, pl.value")
            .append(" FROM predicate_latest pl")
            .append(" LEFT JOIN row_deletes rd ON rd.row_key = pl.row_key")
            .append(" WHERE pl.rn = 1")
            .append(" AND pl.is_deleted = 0")
            .append(" AND (rd.row_delete_ts IS NULL OR pl.timestamp > rd.row_delete_ts)")
            .append("), matching_rows AS (");

        if (predicates.isEmpty()) {
            sql.append(" SELECT DISTINCT row_key FROM ").append(tableName).append(" WHERE row_key >= ? AND row_key < ?");
        } else {
            sql.append(" SELECT p0.row_key FROM predicate_visible p0");
            for (int i = 1; i < predicates.size(); i++) {
                sql.append(" JOIN predicate_visible p").append(i).append(" ON p").append(i).append(".row_key = p0.row_key");
            }
            sql.append(" WHERE ");
            for (int i = 0; i < predicates.size(); i++) {
                if (i > 0) {
                    sql.append(" AND ");
                }
                sql.append("(p").append(i).append(".qualifier = ? AND p").append(i).append(".value ")
                    .append(normalizeOperator(predicates.get(i).getOperator()))
                    .append(" ?)");
            }
            sql.append(" GROUP BY p0.row_key");
        }

        sql.append("), latest_cells AS (")
            .append(" SELECT row_key, family, qualifier, timestamp, value, is_deleted,")
            .append(" ROW_NUMBER() OVER (PARTITION BY row_key, family, qualifier ORDER BY timestamp DESC) AS rn")
            .append(" FROM ").append(tableName)
            .append(" WHERE row_key >= ? AND row_key < ?")
            .append(" AND row_key IN (SELECT row_key FROM matching_rows)");
        if (filter.hasProjectedQualifiers()) {
            sql.append(" AND qualifier IN (").append(projectionPlaceholders).append(")");
        }
        sql.append("), visible_cells AS (")
            .append(" SELECT lc.row_key, lc.family, lc.qualifier, lc.timestamp, lc.value, lc.is_deleted")
            .append(" FROM latest_cells lc")
            .append(" LEFT JOIN row_deletes rd ON rd.row_key = lc.row_key")
            .append(" WHERE lc.rn = 1")
            .append(" AND lc.is_deleted = 0")
            .append(" AND (rd.row_delete_ts IS NULL OR lc.timestamp > rd.row_delete_ts)")
            .append(")")
            .append(" SELECT row_key, family, qualifier, timestamp, value, is_deleted")
            .append(" FROM visible_cells")
            .append(" ORDER BY row_key, timestamp DESC, family, qualifier");
        return sql.toString();
    }

    private Set<String> collectUniqueQualifiers(List<StorageColumnPredicate> predicates) {
        Set<String> qualifiers = new LinkedHashSet<>();
        for (StorageColumnPredicate predicate : predicates) {
            qualifiers.add(predicate.getQualifier());
        }
        return qualifiers;
    }

    private String normalizeOperator(String operator) {
        if ("=".equals(operator) || "==".equals(operator)) {
            return "=";
        }
        if (">".equals(operator) || ">=".equals(operator) || "<".equals(operator) || "<=".equals(operator)) {
            return operator;
        }
        throw new IllegalArgumentException("Unsupported storage predicate operator: " + operator);
    }
}
