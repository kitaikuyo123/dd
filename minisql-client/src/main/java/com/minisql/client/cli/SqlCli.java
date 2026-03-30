package com.minisql.client.cli;

import com.minisql.client.MiniSQLConnection;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * MiniSQL 交互式 SQL 命令行客户端
 *
 * 使用方式:
 * mvn exec:java -pl minisql-client -Dexec.mainClass="com.minisql.client.cli.SqlCli"
 *
 * 特殊命令:
 *   help, ?     - 显示帮助信息
 *   exit, quit  - 退出程序
 *   source      - 从文件执行 SQL
 *   describe    - 显示表结构
 *   show tables - 列出所有表
 */
public class SqlCli {

    private static final String PROMPT = "minisql> ";
    private static final String CONTINUE_PROMPT = "    -> ";

    private Connection conn;
    private boolean running = true;
    private BufferedReader reader;

    // 多行语句缓冲区
    private StringBuilder statementBuffer = new StringBuilder();
    private boolean inMultiLineMode = false;

    public static void main(String[] args) {
        String zkHost = "localhost";
        int zkPort = 2181;

        // 手动注册 JDBC 驱动（兼容某些类加载器环境）
        try {
            DriverManager.registerDriver(new com.minisql.client.MiniSQLDriver());
        } catch (SQLException e) {
            System.err.println("无法注册 MiniSQL 驱动：" + e.getMessage());
            System.exit(1);
        }

        // 解析命令行参数
        for (int i = 0; i < args.length; i++) {
            if ("--host".equals(args[i]) && i + 1 < args.length) {
                zkHost = args[++i];
            } else if ("--port".equals(args[i]) && i + 1 < args.length) {
                zkPort = Integer.parseInt(args[++i]);
            } else if ("--help".equals(args[i]) || "-h".equals(args[i])) {
                printUsage();
                return;
            }
        }

        String url = "jdbc:minisql://" + zkHost + ":" + zkPort;
        SqlCli cli = new SqlCli();
        cli.start(url);
    }

    private static void printUsage() {
        System.out.println("MiniSQL CLI - 交互式 SQL 命令行客户端");
        System.out.println();
        System.out.println("用法: mvn exec:java -pl minisql-client -Dexec.mainClass=\"com.minisql.client.cli.SqlCli\" [选项]");
        System.out.println();
        System.out.println("选项:");
        System.out.println("  --host <host>   ZooKeeper 主机 (默认：localhost)");
        System.out.println("  --port <port>   ZooKeeper 端口 (默认：2181)");
        System.out.println("  --help, -h      显示帮助信息");
        System.out.println();
        System.out.println("特殊命令:");
        System.out.println("  help, ?         显示帮助信息");
        System.out.println("  exit, quit      退出程序");
        System.out.println("  source <file>   从文件执行 SQL");
        System.out.println("  describe <tbl>  显示表结构");
        System.out.println("  show tables     列出所有表");
        System.out.println();
        System.out.println("SQL 语句以分号 (;) 结尾");
    }

    public void start(String url) {
        System.out.println("MiniSQL CLI - 交互式 SQL 命令行客户端");
        System.out.println("正在连接到 MiniSQL 集群：" + url);

        try {
            // 建立连接
            conn = DriverManager.getConnection(url);
            System.out.println("连接成功！\n");
            System.out.println("输入 'help' 获取帮助，'exit' 退出程序。\n");

            // 初始化读取器
            reader = new BufferedReader(new InputStreamReader(System.in));

            // 主循环
            while (running) {
                try {
                    String line = readLine();
                    if (line == null) {
                        // EOF (Ctrl+D)
                        System.out.println();
                        break;
                    }

                    processLine(line.trim());

                } catch (IOException e) {
                    System.err.println("读取输入时出错：" + e.getMessage());
                    break;
                }
            }

            System.out.println("Goodbye!");

        } catch (SQLException e) {
            System.err.println("数据库连接失败：" + e.getMessage());
            System.err.println("请确保 ZooKeeper 和 MiniSQL 集群正在运行。");
            System.exit(1);
        } finally {
            closeConnection();
        }
    }

    private String readLine() throws IOException {
        String prompt = inMultiLineMode ? CONTINUE_PROMPT : PROMPT;
        System.out.print(prompt);
        System.out.flush();
        return reader.readLine();
    }

    private void processLine(String line) throws IOException {
        // 空行处理
        if (line.isEmpty()) {
            return;
        }

        // 如果在多行模式下
        if (inMultiLineMode) {
            statementBuffer.append(" ").append(line);
        } else {
            statementBuffer.setLength(0);
            statementBuffer.append(line);
        }

        // 检查特殊命令（不需要分号）
        if (!inMultiLineMode) {
            if (executeSpecialCommand(line)) {
                statementBuffer.setLength(0);
                return;
            }
        }

        // 检查是否以分号结尾
        if (statementBuffer.toString().trim().endsWith(";")) {
            // 去掉分号并执行
            String sql = statementBuffer.toString().trim();
            sql = sql.substring(0, sql.length() - 1).trim();

            if (!sql.isEmpty()) {
                executeSql(sql);
            }

            statementBuffer.setLength(0);
            inMultiLineMode = false;
        } else {
            inMultiLineMode = true;
        }
    }

    /**
     * 执行特殊命令
     */
    private boolean executeSpecialCommand(String line) {
        String lower = line.toLowerCase().trim();

        // help / ?
        if ("help".equals(lower) || "?".equals(lower)) {
            printHelp();
            return true;
        }

        // exit / quit
        if ("exit".equals(lower) || "quit".equals(lower)) {
            running = false;
            return true;
        }

        // source <file>
        if (lower.startsWith("source ")) {
            String filePath = line.substring(7).trim();
            executeSourceFile(filePath);
            return true;
        }

        // describe <table>
        if (lower.startsWith("describe ") || lower.startsWith("desc ")) {
            String tableName = line.substring(lower.startsWith("describe ") ? 9 : 5).trim();
            executeDescribe(tableName);
            return true;
        }

        // show tables
        if ("show tables".equals(lower)) {
            executeShowTables();
            return true;
        }

        return false;
    }

    /**
     * 执行 SQL 语句
     */
    private void executeSql(String sql) {
        if (sql.isEmpty()) {
            return;
        }

        String upper = sql.trim().toUpperCase();

        try (Statement stmt = conn.createStatement()) {
            if (upper.startsWith("SELECT") || upper.startsWith("SHOW") || upper.startsWith("DESCRIBE")) {
                // 查询语句
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    CliResultFormatter.printResultSet(rs);
                }
            } else {
                // 更新语句
                int rows = stmt.executeUpdate(sql);
                System.out.println("OK (" + rows + " row" + (rows != 1 ? "s" : "") + " affected)");
            }
        } catch (SQLException e) {
            System.err.println("SQL 错误：" + e.getMessage());
        }
    }

    /**
     * 显示帮助信息
     */
    private void printHelp() {
        System.out.println("MiniSQL CLI 帮助:");
        System.out.println();
        System.out.println("SQL 语句:");
        System.out.println("  执行任意 SQL 语句，以分号 (;) 结尾");
        System.out.println("  示例：SELECT * FROM users;");
        System.out.println();
        System.out.println("特殊命令:");
        System.out.println("  help, ?         - 显示帮助信息");
        System.out.println("  exit, quit      - 退出程序");
        System.out.println("  source <file>   - 从文件执行 SQL");
        System.out.println("  describe <tbl>  - 显示表结构");
        System.out.println("  show tables     - 列出所有表");
        System.out.println();
        System.out.println("提示:");
        System.out.println("  多行输入：如果语句没有分号，可以继续输入下一行");
        System.out.println("  注释：支持 -- 单行注释");
    }

    /**
     * 显示表列表
     */
    private void executeShowTables() {
        System.out.println("Tables in database:");
        try {
            // 通过 MiniSQLConnection 的 listTables() 方法获取表列表
            if (conn instanceof com.minisql.client.MiniSQLConnection) {
                com.minisql.client.MiniSQLConnection miniSqlConn = (com.minisql.client.MiniSQLConnection) conn;
                List<String> tableNames = miniSqlConn.listTables();

                if (tableNames.isEmpty()) {
                    System.out.println("(empty)");
                } else {
                    // 打印表名列表
                    System.out.println("+------------+");
                    System.out.println("| table_name |");
                    System.out.println("+------------+");
                    for (String tableName : tableNames) {
                        System.out.println("| " + tableName);
                    }
                    System.out.println("+------------+");
                }
            } else {
                //  fallback: 尝试查询系统表（如果使用的是其他连接实现）
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SELECT table_name FROM system_tables");
                    CliResultFormatter.printResultSet(rs);
                }
            }
        } catch (SQLException e) {
            System.out.println("(无法获取表列表：" + e.getMessage() + ")");
        }
    }

    /**
     * 显示表结构
     */
    private void executeDescribe(String tableName) {
        if (tableName.isEmpty()) {
            System.err.println("用法：describe <table_name>");
            return;
        }

        try {
            // 尝试查询系统表获取列信息
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT column_name, data_type, is_nullable " +
                            "FROM system_columns WHERE table_name = '" + tableName + "'";
                ResultSet rs = stmt.executeQuery(sql);
                CliResultFormatter.printResultSet(rs);
            }
        } catch (SQLException e) {
            System.err.println("无法获取表结构：" + e.getMessage());
        }
    }

    /**
     * 从文件执行 SQL
     */
    private void executeSourceFile(String filePath) {
        if (filePath.isEmpty()) {
            System.err.println("用法：source <file_path>");
            return;
        }

        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("文件不存在：" + filePath);
            return;
        }

        System.out.println("执行 SQL 文件：" + filePath);

        try (BufferedReader fileReader = new BufferedReader(new FileReader(file));
             Statement stmt = conn.createStatement()) {

            String line;
            StringBuilder sqlBuffer = new StringBuilder();

            while ((line = fileReader.readLine()) != null) {
                line = line.trim();

                // 跳过空行和注释
                if (line.isEmpty() || line.startsWith("--")) {
                    continue;
                }

                sqlBuffer.append(line).append(" ");

                // 检查是否以分号结尾
                if (line.endsWith(";")) {
                    String sql = sqlBuffer.toString().trim();
                    sql = sql.substring(0, sql.length() - 1).trim();

                    if (!sql.isEmpty()) {
                        if (sql.toUpperCase().startsWith("SELECT")) {
                            try (ResultSet rs = stmt.executeQuery(sql)) {
                                CliResultFormatter.printResultSet(rs);
                            }
                        } else {
                            int rows = stmt.executeUpdate(sql);
                            System.out.println("OK (" + rows + " row(s) affected)");
                        }
                    }

                    sqlBuffer.setLength(0);
                }
            }

            System.out.println("文件执行完成。");

        } catch (SQLException e) {
            System.err.println("SQL 错误：" + e.getMessage());
        } catch (IOException e) {
            System.err.println("读取文件失败：" + e.getMessage());
        }
    }

    private void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // 忽略
            }
        }
    }
}
