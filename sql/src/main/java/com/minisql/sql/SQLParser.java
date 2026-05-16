package com.minisql.sql;

import com.minisql.sql.Lexer.Token;
import com.minisql.sql.Lexer.TokenType;
import com.minisql.sql.ast.Assignment;
import com.minisql.sql.ast.ColumnDef;
import com.minisql.sql.ast.ColumnType;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.CreateTableStatement;
import com.minisql.sql.ast.DeleteStatement;
import com.minisql.sql.ast.DropTableStatement;
import com.minisql.sql.ast.InsertStatement;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.ast.ShowTablesStatement;
import com.minisql.sql.ast.SimpleCondition;
import com.minisql.sql.ast.Statement;
import com.minisql.sql.ast.UpdateStatement;

import com.minisql.sql.JoinType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 手写递归下降 SQL 解析器
 *
 * 将 SQL 文本经词法分析后产生的 Token 流解析为 AST。
 * 支持 SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, SHOW TABLES 七种语句。
 *
 * 条件表达式采用经典的三层递归结构：
 *   OR 表达式 -> AND 表达式 -> 基本比较条件
 * 支持括号分组、聚合函数（COUNT/SUM/AVG/MAX/MIN）、JOIN、GROUP BY、HAVING、ORDER BY、LIMIT。
 */
public class SQLParser {

    /** 支持的聚合函数集合 */
    private static final Set<String> AGGREGATE_FUNCTIONS = new HashSet<>(
        Arrays.asList("COUNT", "SUM", "AVG", "MAX", "MIN"));

    /** 词法分析产生的 Token 流 */
    private final List<Token> tokens;

    /** 当前读取位置 */
    private int position;

    /**
     * 构造解析器，内部完成词法分析
     *
     * @param sql 待解析的 SQL 语句文本
     */
    public SQLParser(String sql) {
        this.tokens = new Lexer(sql).tokenize();
    }

    /**
     * 解析入口，根据首 Token 分发到对应语句的解析方法
     *
     * @return 解析产生的 AST 根节点
     */
    public Statement parse() {
        switch (current().type) {
            case SELECT:
                return parseSelect();
            case INSERT:
                return parseInsert();
            case UPDATE:
                return parseUpdate();
            case DELETE:
                return parseDelete();
            case CREATE:
                return parseCreate();
            case DROP:
                return parseDrop();
            case SHOW:
                return parseShowTables();
            default:
                throw new RuntimeException("Unexpected token: " + current().type);
        }
    }

    /** 解析 SHOW TABLES 语句 */
    private ShowTablesStatement parseShowTables() {
        consume(TokenType.SHOW);
        consume(TokenType.TABLES);
        return new ShowTablesStatement();
    }

    /**
     * 解析 SELECT 语句
     *
     * 文法: SELECT [DISTINCT] selectList FROM tableRef [JOIN ...] [WHERE ...]
     *       [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT ... [OFFSET ...]]
     */
    private SelectStatement parseSelect() {
        consume(TokenType.SELECT);

        SelectStatement stmt = new SelectStatement();
        parseSelectList(stmt);
        consume(TokenType.FROM);
        parseTableRef(stmt);

        // JOIN: [LEFT | INNER] JOIN tableRef ON condition
        parseJoin(stmt);

        if (match(TokenType.WHERE)) {
            stmt.setWhere(parseCondition());
        }
        parseGroupBy(stmt);
        parseHaving(stmt);
        parseOrderBy(stmt);
        parseLimit(stmt);
        return stmt;
    }

    /**
     * 解析 SELECT 列表，包括普通列和聚合函数
     *
     * 支持格式:  *  |  col1, col2, COUNT(*), SUM(col) AS alias, ...
     */
    private void parseSelectList(SelectStatement stmt) {
        if (match(TokenType.ASTERISK)) {
            stmt.setSelectAll(true);
            return;
        }

        List<String> columns = new ArrayList<>();
        do {
            // Check for aggregate function: IDENTIFIER "(" ...
            if (current().type == TokenType.IDENTIFIER
                && AGGREGATE_FUNCTIONS.contains(current().value.toUpperCase())
                && peek(1).type == TokenType.LPAREN) {
                SelectStatement.AggregateExpr agg = parseAggregateFunction();
                stmt.addAggregate(agg);
                columns.add(agg.getOutputName());
                // Check for alias after aggregate
                if (match(TokenType.AS)) {
                    agg.setAlias(consume(TokenType.IDENTIFIER).value);
                    // Replace last column with alias
                    columns.set(columns.size() - 1, agg.getAlias());
                }
                stmt.addColumnAlias(agg.getAlias());
            } else {
                String column = parseIdentifierPath();
                columns.add(column);
                if (match(TokenType.AS)) {
                    String alias = consume(TokenType.IDENTIFIER).value;
                    stmt.addColumnAlias(alias);
                } else {
                    stmt.addColumnAlias(null);
                }
            }
        } while (match(TokenType.COMMA));
        stmt.setColumns(columns);
    }

    /**
     * 解析聚合函数调用，如 COUNT(*), SUM(price)
     *
     * @return 聚合表达式对象
     */
    private SelectStatement.AggregateExpr parseAggregateFunction() {
        String function = consume(TokenType.IDENTIFIER).value.toUpperCase();
        consume(TokenType.LPAREN);
        String column;
        if (match(TokenType.ASTERISK)) {
            column = "*";
        } else {
            column = parseIdentifierPath();
        }
        consume(TokenType.RPAREN);
        return new SelectStatement.AggregateExpr(function, column);
    }

    /** 解析表引用，包括表名和可选的别名（AS 关键字或裸标识符） */
    private void parseTableRef(SelectStatement stmt) {
        stmt.setTable(parseIdentifierPath());
        // Table alias: [AS identifier] or bare identifier (if not a keyword that starts next clause)
        if (match(TokenType.AS)) {
            stmt.setTableAlias(consume(TokenType.IDENTIFIER).value);
        } else if (current().type == TokenType.IDENTIFIER) {
            // Bare alias: FROM students s — but only if not a clause keyword
            stmt.setTableAlias(consume(TokenType.IDENTIFIER).value);
        }
    }

    /**
     * 解析 JOIN 子句
     *
     * 支持格式:  [LEFT | INNER] JOIN tableRef [alias] ON condition
     * 省略连接类型时默认为 INNER JOIN
     */
    private void parseJoin(SelectStatement stmt) {
        JoinType joinType = null;

        if (match(TokenType.LEFT)) {
            joinType = JoinType.LEFT;
            consume(TokenType.JOIN);
        } else if (match(TokenType.INNER)) {
            joinType = JoinType.INNER;
            consume(TokenType.JOIN);
        } else if (match(TokenType.JOIN)) {
            joinType = JoinType.INNER;
        }

        if (joinType == null) {
            return;
        }

        stmt.setJoinType(joinType);
        stmt.setJoinTable(parseIdentifierPath());
        // Join table alias
        if (match(TokenType.AS)) {
            stmt.setJoinTableAlias(consume(TokenType.IDENTIFIER).value);
        } else if (current().type == TokenType.IDENTIFIER && current().type != TokenType.ON) {
            stmt.setJoinTableAlias(consume(TokenType.IDENTIFIER).value);
        }
        consume(TokenType.ON);
        stmt.setJoinCondition(parseCondition());
    }

    /** 解析 GROUP BY 子句 */
    private void parseGroupBy(SelectStatement stmt) {
        if (!match(TokenType.GROUP)) {
            return;
        }
        consume(TokenType.BY);
        stmt.setGroupByColumns(parseIdentifierList());
    }

    /** 解析 HAVING 子句，对聚合后的结果进行过滤 */
    private void parseHaving(SelectStatement stmt) {
        if (!match(TokenType.HAVING)) {
            return;
        }
        stmt.setHaving(parseCondition());
    }

    /** 向前预读 offset 个位置的 Token，不移动读取位置 */
    private Token peek(int offset) {
        int idx = position + offset;
        return idx < tokens.size() ? tokens.get(idx) : tokens.get(tokens.size() - 1);
    }

    /** 解析 ORDER BY 子句，支持多列排序和 ASC/DESC 方向 */
    private void parseOrderBy(SelectStatement stmt) {
        if (!match(TokenType.ORDER)) {
            return;
        }

        consume(TokenType.BY);
        List<SelectStatement.OrderByElement> orderBy = new ArrayList<>();
        do {
            String column = parseIdentifierPath();
            boolean ascending = true;
            if (match(TokenType.ASC)) {
                ascending = true;
            } else if (match(TokenType.DESC)) {
                ascending = false;
            }
            orderBy.add(new SelectStatement.OrderByElement(column, ascending));
        } while (match(TokenType.COMMA));
        stmt.setOrderBy(orderBy);
    }

    /** 解析 LIMIT 和可选的 OFFSET 子句 */
    private void parseLimit(SelectStatement stmt) {
        if (!match(TokenType.LIMIT)) {
            return;
        }
        stmt.setLimit(Integer.parseInt(consume(TokenType.INTEGER).value));
        if (match(TokenType.OFFSET)) {
            stmt.setOffset(Integer.parseInt(consume(TokenType.INTEGER).value));
        }
    }

    /** 解析 INSERT INTO 语句，格式: INSERT INTO table (col1, col2) VALUES (v1, v2) */
    private InsertStatement parseInsert() {
        consume(TokenType.INSERT);
        consume(TokenType.INTO);

        InsertStatement stmt = new InsertStatement();
        stmt.setTable(parseIdentifierPath());

        consume(TokenType.LPAREN);
        stmt.setColumns(parseIdentifierList());
        consume(TokenType.RPAREN);

        consume(TokenType.VALUES);
        consume(TokenType.LPAREN);
        List<String> values = new ArrayList<>();
        do {
            values.add(consumeLiteral().value);
        } while (match(TokenType.COMMA));
        consume(TokenType.RPAREN);
        stmt.setValues(values);
        return stmt;
    }

    /** 解析 UPDATE 语句，格式: UPDATE table SET col1=v1, col2=v2 [WHERE ...] */
    private UpdateStatement parseUpdate() {
        consume(TokenType.UPDATE);
        UpdateStatement stmt = new UpdateStatement();
        stmt.setTable(parseIdentifierPath());

        consume(TokenType.SET);
        List<Assignment> assignments = new ArrayList<>();
        do {
            String column = parseIdentifierPath();
            consume(TokenType.EQ);
            assignments.add(new Assignment(column, consumeLiteral().value));
        } while (match(TokenType.COMMA));
        stmt.setAssignments(assignments);

        if (match(TokenType.WHERE)) {
            stmt.setWhere(parseCondition());
        }
        return stmt;
    }

    /** 解析 DELETE FROM 语句，格式: DELETE FROM table [WHERE ...] */
    private DeleteStatement parseDelete() {
        consume(TokenType.DELETE);
        consume(TokenType.FROM);

        DeleteStatement stmt = new DeleteStatement();
        stmt.setTable(parseIdentifierPath());
        if (match(TokenType.WHERE)) {
            stmt.setWhere(parseCondition());
        }
        return stmt;
    }

    /**
     * 解析 CREATE TABLE 语句
     *
     * 支持格式:
     *   CREATE TABLE t (col1 INT, col2 VARCHAR(255), PRIMARY KEY(col1))
     *   CREATE TABLE t (col1 INT NOT NULL, col2 TEXT, PRIMARY KEY((col1, col2), col3))
     *
     * 主键声明支持三种形式:
     *   1. 列定义后紧跟 PRIMARY KEY（单列主键）
     *   2. PRIMARY KEY(col) 语法（单列主键）
     *   3. PRIMARY KEY((pk1, pk2), ck1) 语法（复合分区键 + 聚簇键）
     */
    private CreateTableStatement parseCreate() {
        consume(TokenType.CREATE);
        consume(TokenType.TABLE);
        CreateTableStatement stmt = new CreateTableStatement();
        stmt.setTable(parseIdentifierPath());

        consume(TokenType.LPAREN);
        List<ColumnDef> columns = new ArrayList<>();
        List<String> partitionKeys = new ArrayList<>();
        List<String> clusteringKeys = new ArrayList<>();
        String primaryKey = null;

        while (current().type != TokenType.RPAREN) {
            if (current().type == TokenType.PRIMARY) {
                parsePrimaryKeyClause(partitionKeys, clusteringKeys);
            } else {
                ColumnDef columnDef = parseColumnDefinition();
                columns.add(columnDef);
                if (!columnDef.isNullable()) {
                    primaryKey = columnDef.getName();
                    if (partitionKeys.isEmpty()) {
                        partitionKeys.add(primaryKey);
                    }
                }
            }
            match(TokenType.COMMA);
        }
        consume(TokenType.RPAREN);

        if (primaryKey == null && partitionKeys.size() == 1 && clusteringKeys.isEmpty()) {
            primaryKey = partitionKeys.get(0);
        }

        stmt.setColumns(columns);
        stmt.setPrimaryKey(primaryKey);
        stmt.setPartitionKeys(partitionKeys.isEmpty() ? null : partitionKeys);
        stmt.setClusteringKeys(clusteringKeys.isEmpty() ? null : clusteringKeys);
        return stmt;
    }

    /** 解析单个列定义，包括列名、类型、长度和是否可空 */
    private ColumnDef parseColumnDefinition() {
        String columnName = parseIdentifierPath();
        Token typeToken = consume(TokenType.INT, TokenType.BIGINT, TokenType.VARCHAR, TokenType.DOUBLE,
            TokenType.STRING_TYPE, TokenType.TEXT);
        ColumnType type = mapColumnType(typeToken.type);
        int length = 0;

        if (match(TokenType.LPAREN)) {
            length = Integer.parseInt(consume(TokenType.INTEGER).value);
            consume(TokenType.RPAREN);
        }

        boolean nullable = true;
        if (match(TokenType.PRIMARY)) {
            consume(TokenType.KEY);
            nullable = false;
        }
        return new ColumnDef(columnName, type, length, nullable);
    }

    /**
     * 解析 PRIMARY KEY 子句
     *
     * 格式一: PRIMARY KEY(col)                     -> partitionKeys = [col]
     * 格式二: PRIMARY KEY((pk1, pk2), ck1, ck2)    -> partitionKeys = [pk1, pk2], clusteringKeys = [ck1, ck2]
     */
    private void parsePrimaryKeyClause(List<String> partitionKeys, List<String> clusteringKeys) {
        consume(TokenType.PRIMARY);
        consume(TokenType.KEY);
        consume(TokenType.LPAREN);
        if (match(TokenType.LPAREN)) {
            partitionKeys.addAll(parseIdentifierList());
            consume(TokenType.RPAREN);
            if (match(TokenType.COMMA)) {
                clusteringKeys.addAll(parseIdentifierList());
            }
        } else {
            partitionKeys.add(parseIdentifierPath());
        }
        consume(TokenType.RPAREN);
    }

    /** 解析 DROP TABLE 语句 */
    private DropTableStatement parseDrop() {
        consume(TokenType.DROP);
        consume(TokenType.TABLE);
        DropTableStatement stmt = new DropTableStatement();
        stmt.setTable(parseIdentifierPath());
        return stmt;
    }

    /** 条件表达式入口，解析 OR 层级 */
    private Condition parseCondition() {
        return parseOrCondition();
    }

    /** 解析 OR 层级: AND_expr (OR AND_expr)* */
    private Condition parseOrCondition() {
        Condition left = parseAndCondition();
        while (match(TokenType.OR)) {
            left = new CompoundCondition(left, parseAndCondition(), "OR");
        }
        return left;
    }

    /** 解析 AND 层级: primary_cond (AND primary_cond)* */
    private Condition parseAndCondition() {
        Condition left = parsePrimaryCondition();
        while (match(TokenType.AND)) {
            left = new CompoundCondition(left, parsePrimaryCondition(), "AND");
        }
        return left;
    }

    /** 解析基本条件单元，支持括号分组和简单比较 */
    private Condition parsePrimaryCondition() {
        if (match(TokenType.LPAREN)) {
            Condition condition = parseCondition();
            consume(TokenType.RPAREN);
            return condition;
        }
        return parseSimpleCondition();
    }

    /**
     * 解析简单比较条件
     *
     * 格式: column operator value  或  column operator column
     * 当右侧为标识符时标记为列引用（用于 JOIN ON 条件）
     */
    private Condition parseSimpleCondition() {
        String column = parseIdentifierPath();
        String operator = consume(TokenType.EQ, TokenType.LT, TokenType.GT, TokenType.LE, TokenType.GE, TokenType.NE).value;

        if (current().type == TokenType.IDENTIFIER) {
            String valueReference = parseIdentifierPath();
            return new SimpleCondition(column, operator, valueReference, true);
        }
        return new SimpleCondition(column, operator, consumeLiteral().value, false);
    }

    /** 解析逗号分隔的标识符列表 */
    private List<String> parseIdentifierList() {
        List<String> values = new ArrayList<>();
        do {
            values.add(parseIdentifierPath());
        } while (match(TokenType.COMMA));
        return values;
    }

    /** 解析点号分隔的标识符路径，如 table.column */
    private String parseIdentifierPath() {
        String value = consume(TokenType.IDENTIFIER).value;
        while (match(TokenType.DOT)) {
            value = value + "." + consume(TokenType.IDENTIFIER).value;
        }
        return value;
    }

    /** 将词法 Token 类型映射为列类型枚举 */
    private ColumnType mapColumnType(TokenType tokenType) {
        switch (tokenType) {
            case STRING_TYPE:
                return ColumnType.STRING;
            case TEXT:
                return ColumnType.TEXT;
            default:
                return ColumnType.valueOf(tokenType.name());
        }
    }

    /**
     * 尝试匹配期望的 Token 类型
     * 匹配成功则前进位置并返回 true，否则不移动并返回 false
     */
    private boolean match(TokenType expected) {
        if (current().type != expected) {
            return false;
        }
        position++;
        return true;
    }

    /** 获取当前 Token，不移动位置 */
    private Token current() {
        return tokens.get(position);
    }

    /**
     * 消费期望类型的 Token
     * 类型不匹配时抛出运行时异常
     */
    private Token consume(TokenType expected) {
        Token token = current();
        if (token.type != expected) {
            throw new RuntimeException("Expected " + expected + " but got " + token.type);
        }
        position++;
        return token;
    }

    /** 消费多种候选类型之一的 Token */
    private Token consume(TokenType... expected) {
        Token token = current();
        for (TokenType type : expected) {
            if (token.type == type) {
                position++;
                return token;
            }
        }
        throw new RuntimeException("Unexpected token: " + token.type);
    }

    /** 消费字面量 Token（字符串、整数或浮点数） */
    private Token consumeLiteral() {
        Token token = current();
        if (token.type == TokenType.STRING || token.type == TokenType.INTEGER || token.type == TokenType.FLOAT) {
            position++;
            return token;
        }
        throw new RuntimeException("Expected literal but got " + token.type);
    }
}
