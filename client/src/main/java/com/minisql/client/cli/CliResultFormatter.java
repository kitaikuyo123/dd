package com.minisql.client.cli;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 结果集格式化工具
 * 将 ResultSet 格式化为 ASCII 表格输出
 */
public class CliResultFormatter {

    /**
     * 打印 ResultSet 为表格
     */
    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 读取所有数据
        List<String[]> rows = new ArrayList<>();
        int[] columnWidths = new int[columnCount];

        // 初始化列宽为列名长度
        String[] columnNames = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnNames[i] = metaData.getColumnLabel(i + 1);
            columnWidths[i] = columnNames[i].length();
        }

        // 读取数据并计算每列的最大宽度
        while (rs.next()) {
            String[] row = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                Object value = rs.getObject(i + 1);
                if (value == null) {
                    row[i] = "NULL";
                } else if (value instanceof byte[]) {
                    row[i] = bytesToHex((byte[]) value);
                } else {
                    row[i] = value.toString();
                }
                columnWidths[i] = Math.max(columnWidths[i], row[i].length());
            }
            rows.add(row);
        }

        // 限制最大列宽
        int maxWidth = 50;
        for (int i = 0; i < columnCount; i++) {
            columnWidths[i] = Math.min(columnWidths[i], maxWidth);
        }

        // 打印表格
        printSeparator(columnWidths);
        printRow(columnNames, columnWidths);
        printSeparator(columnWidths);

        for (String[] row : rows) {
            printRow(row, columnWidths);
        }

        if (!rows.isEmpty()) {
            printSeparator(columnWidths);
        }

        // 显示行数
        System.out.println("(" + rows.size() + " row" + (rows.size() != 1 ? "s" : "") + ")");
    }

    /**
     * 打印分隔线
     */
    private static void printSeparator(int[] columnWidths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : columnWidths) {
            sb.append("-".repeat(width + 2));
            sb.append("+");
        }
        System.out.println(sb);
    }

    /**
     * 打印一行数据
     */
    private static void printRow(String[] values, int[] columnWidths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < values.length; i++) {
            String value = truncate(values[i], columnWidths[i]);
            sb.append(" ");
            sb.append(padRight(value, columnWidths[i]));
            sb.append(" |");
        }
        System.out.println(sb);
    }

    /**
     * 截断过长的字符串
     */
    private static String truncate(String value, int maxWidth) {
        if (value.length() <= maxWidth) {
            return value;
        }
        return value.substring(0, maxWidth - 3) + "...";
    }

    /**
     * 右填充字符串
     */
    private static String padRight(String value, int width) {
        if (value.length() >= width) {
            return value;
        }
        StringBuilder sb = new StringBuilder(value);
        while (sb.length() < width) {
            sb.append(' ');
        }
        return sb.toString();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (byte b : bytes) {
            sb.append(String.format("%02x ", b & 0xFF));
        }
        return sb.toString().trim();
    }
}
