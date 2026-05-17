package com.minisql.sql;

import com.minisql.sql.Lexer.Token;
import com.minisql.sql.Lexer.TokenType;
import com.minisql.sql.ast.Assignment;
import com.minisql.sql.ast.BetweenCondition;
import com.minisql.sql.ast.ColumnDef;
import com.minisql.sql.ast.ColumnType;
import com.minisql.sql.ast.CompoundCondition;
import com.minisql.sql.ast.Condition;
import com.minisql.sql.ast.CreateTableStatement;
import com.minisql.sql.ast.DeleteStatement;
import com.minisql.sql.ast.DropTableStatement;
import com.minisql.sql.ast.ExistsCondition;
import com.minisql.sql.ast.InCondition;
import com.minisql.sql.ast.InSubqueryCondition;
import com.minisql.sql.ast.InsertStatement;
import com.minisql.sql.ast.NotCondition;
import com.minisql.sql.ast.IsNullCondition;
import com.minisql.sql.ast.SelectStatement;
import com.minisql.sql.ast.ShowTablesStatement;
import com.minisql.sql.ast.SimpleCondition;
import com.minisql.sql.ast.Statement;
import com.minisql.sql.ast.SubqueryExpression;
import com.minisql.sql.ast.UpdateStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 手写递归下降 SQL 解析器。
 *
 * <p>支持的语句：SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, SHOW TABLES。
 *
 * <p>条件表达式采用三层递归结构：
 * <pre>
 *   condition     → orCondition
 *   orCondition   → andCondition (OR andCondition)*
 *   andCondition  → primaryCondition (AND primaryCondition)*
 *   primaryCondition → '(' condition ')'
 *                   | NOT primaryCondition
 *                   | EXISTS '(' SELECT ... ')'
 *                   | simpleCondition
 *   simpleCondition  → column BETWEEN literal AND literal
 *                   | column [NOT] BETWEEN literal AND literal
 *                   | column [NOT] IN '(' valueList | SELECT ... ')'
 *                   | column IS [NOT] NULL
 *                   | column LIKE pattern
 *                   | column operator (literal | column)
 * </pre>
 */
public class SQLParser {

    private static final Set<String> AGGREGATE_FUNCTIONS = new HashSet<>(
        Arrays.asList("COUNT", "SUM", "AVG", "MAX", "MIN"));

    private final List<Token> tokens;
    private int position;

    public SQLParser(String sql) {
        this.tokens = new Lexer(sql).tokenize();
    }

    public Statement parse() {
        switch (current().type) {
            case SELECT: return parseSelect();
            case INSERT: return parseInsert();
            case UPDATE: return parseUpdate();
            case DELETE: return parseDelete();
            case CREATE: return parseCreate();
            case DROP:   return parseDrop();
            case SHOW:   return parseShowTables();
            default: throw new RuntimeException("Unexpected token: " + current().type);
        }
    }

    // ==================== SELECT ====================

    private ShowTablesStatement parseShowTables() {
        consume(TokenType.SHOW);
        consume(TokenType.TABLES);
        return new ShowTablesStatement();
    }

    /**
     * SELECT [DISTINCT] selectList FROM tableRef
     *   [LEFT|INNER|RIGHT|FULL [OUTER]] JOIN tableRef ON condition
     *   [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT ... [OFFSET ...]]
     */
    private SelectStatement parseSelect() {
        consume(TokenType.SELECT);
        SelectStatement stmt = new SelectStatement();

        if (match(TokenType.DISTINCT)) {
            stmt.setDistinct(true);
        }

        parseSelectList(stmt);
        consume(TokenType.FROM);
        parseTableRef(stmt);
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

    private void parseSelectList(SelectStatement stmt) {
        if (match(TokenType.ASTERISK)) {
            stmt.setSelectAll(true);
            return;
        }
        List<String> columns = new ArrayList<>();
        do {
            if (current().type == TokenType.IDENTIFIER
                && AGGREGATE_FUNCTIONS.contains(current().value.toUpperCase())
                && peek(1).type == TokenType.LPAREN) {
                SelectStatement.AggregateExpr agg = parseAggregateFunction();
                stmt.addAggregate(agg);
                columns.add(agg.getOutputName());
                if (match(TokenType.AS)) {
                    agg.setAlias(consume(TokenType.IDENTIFIER).value);
                    columns.set(columns.size() - 1, agg.getAlias());
                }
                stmt.addColumnAlias(agg.getAlias());
            } else {
                String column = parseIdentifierPath();
                columns.add(column);
                if (match(TokenType.AS)) {
                    stmt.addColumnAlias(consume(TokenType.IDENTIFIER).value);
                } else {
                    stmt.addColumnAlias(null);
                }
            }
        } while (match(TokenType.COMMA));
        stmt.setColumns(columns);
    }

    private SelectStatement.AggregateExpr parseAggregateFunction() {
        String function = consume(TokenType.IDENTIFIER).value.toUpperCase();
        consume(TokenType.LPAREN);
        String column = match(TokenType.ASTERISK) ? "*" : parseIdentifierPath();
        consume(TokenType.RPAREN);
        return new SelectStatement.AggregateExpr(function, column);
    }

    private void parseTableRef(SelectStatement stmt) {
        stmt.setTable(parseIdentifierPath());
        if (match(TokenType.AS)) {
            stmt.setTableAlias(consume(TokenType.IDENTIFIER).value);
        } else if (current().type == TokenType.IDENTIFIER && !isClauseKeyword(current().type)) {
            stmt.setTableAlias(consume(TokenType.IDENTIFIER).value);
        }
    }

    /**
     * 解析 JOIN 子句。
     *
     * <p>支持：{@code [LEFT|INNER|RIGHT|FULL [OUTER]] JOIN tableRef ON condition}
     */
    private void parseJoin(SelectStatement stmt) {
        JoinType joinType = null;

        if (match(TokenType.LEFT)) {
            match(TokenType.OUTER);
            joinType = JoinType.LEFT;
            consume(TokenType.JOIN);
        } else if (match(TokenType.RIGHT)) {
            match(TokenType.OUTER);
            joinType = JoinType.RIGHT;
            consume(TokenType.JOIN);
        } else if (match(TokenType.FULL)) {
            match(TokenType.OUTER);
            joinType = JoinType.FULL;
            consume(TokenType.JOIN);
        } else if (match(TokenType.INNER)) {
            joinType = JoinType.INNER;
            consume(TokenType.JOIN);
        } else if (match(TokenType.JOIN)) {
            joinType = JoinType.INNER;
        }

        if (joinType == null) return;

        stmt.setJoinType(joinType);
        stmt.setJoinTable(parseIdentifierPath());
        if (match(TokenType.AS)) {
            stmt.setJoinTableAlias(consume(TokenType.IDENTIFIER).value);
        } else if (current().type == TokenType.IDENTIFIER && current().type != TokenType.ON) {
            stmt.setJoinTableAlias(consume(TokenType.IDENTIFIER).value);
        }
        consume(TokenType.ON);
        stmt.setJoinCondition(parseCondition());
    }

    /** 判断当前 token 是否是子句起始关键字（用于区分裸别名和下一子句） */
    private boolean isClauseKeyword(TokenType type) {
        return type == TokenType.WHERE || type == TokenType.ON
            || type == TokenType.JOIN || type == TokenType.LEFT
            || type == TokenType.RIGHT || type == TokenType.INNER
            || type == TokenType.FULL || type == TokenType.GROUP
            || type == TokenType.HAVING || type == TokenType.ORDER
            || type == TokenType.LIMIT || type == TokenType.OUTER;
    }

    private void parseGroupBy(SelectStatement stmt) {
        if (!match(TokenType.GROUP)) return;
        consume(TokenType.BY);
        stmt.setGroupByColumns(parseIdentifierList());
    }

    private void parseHaving(SelectStatement stmt) {
        if (!match(TokenType.HAVING)) return;
        stmt.setHaving(parseCondition());
    }

    private void parseOrderBy(SelectStatement stmt) {
        if (!match(TokenType.ORDER)) return;
        consume(TokenType.BY);
        List<SelectStatement.OrderByElement> orderBy = new ArrayList<>();
        do {
            String column = parseIdentifierPath();
            boolean ascending = true;
            if (match(TokenType.ASC)) ascending = true;
            else if (match(TokenType.DESC)) ascending = false;
            orderBy.add(new SelectStatement.OrderByElement(column, ascending));
        } while (match(TokenType.COMMA));
        stmt.setOrderBy(orderBy);
    }

    private void parseLimit(SelectStatement stmt) {
        if (!match(TokenType.LIMIT)) return;
        stmt.setLimit(Integer.parseInt(consume(TokenType.INTEGER).value));
        if (match(TokenType.OFFSET)) {
            stmt.setOffset(Integer.parseInt(consume(TokenType.INTEGER).value));
        }
    }

    // ==================== 条件解析（三层递归） ====================

    private Condition parseCondition() { return parseOrCondition(); }

    private Condition parseOrCondition() {
        Condition left = parseAndCondition();
        while (match(TokenType.OR)) {
            left = new CompoundCondition(left, parseAndCondition(), "OR");
        }
        return left;
    }

    private Condition parseAndCondition() {
        Condition left = parsePrimaryCondition();
        while (match(TokenType.AND)) {
            left = new CompoundCondition(left, parsePrimaryCondition(), "AND");
        }
        return left;
    }

    /**
     * 解析基本条件单元。
     *
     * <ul>
     *   <li>括号分组：{@code (condition)}</li>
     *   <li>NOT 前缀：{@code NOT condition} 或 {@code NOT EXISTS (SELECT ...)}</li>
     *   <li>EXISTS 子查询：{@code EXISTS (SELECT ...)}</li>
     *   <li>简单条件：BETWEEN / IN / IS NULL / LIKE / 比较运算符</li>
     * </ul>
     */
    private Condition parsePrimaryCondition() {
        // 括号分组
        if (match(TokenType.LPAREN)) {
            Condition condition = parseCondition();
            consume(TokenType.RPAREN);
            return condition;
        }
        // EXISTS / NOT EXISTS 子查询
        if (current().type == TokenType.EXISTS) {
            return parseExistsCondition(false);
        }
        // NOT 前缀
        if (current().type == TokenType.NOT) {
            if (peek(1).type == TokenType.EXISTS) {
                consume(TokenType.NOT);
                return parseExistsCondition(true);
            }
            consume(TokenType.NOT);
            return new NotCondition(parsePrimaryCondition());
        }
        return parseSimpleCondition();
    }

    /** 解析 EXISTS (SELECT ...) 或 NOT EXISTS (SELECT ...) */
    private Condition parseExistsCondition(boolean negated) {
        consume(TokenType.EXISTS);
        consume(TokenType.LPAREN);
        SelectStatement subSelect = parseSelect();
        consume(TokenType.RPAREN);
        return new ExistsCondition(new SubqueryExpression(subSelect), negated);
    }

    /**
     * 解析简单条件。
     *
     * <pre>
     *   column BETWEEN low AND high
     *   column NOT BETWEEN low AND high
     *   column IN (v1, v2, ...)
     *   column IN (SELECT ...)
     *   column NOT IN (v1, v2, ...)
     *   column NOT IN (SELECT ...)
     *   column IS NULL
     *   column IS NOT NULL
     *   column LIKE pattern
     *   column operator (literal | column)
     * </pre>
     */
    private Condition parseSimpleCondition() {
        String column = parseIdentifierPath();

        // BETWEEN
        if (match(TokenType.BETWEEN)) {
            String low = consumeLiteral().value;
            consume(TokenType.AND);
            String high = consumeLiteral().value;
            return new BetweenCondition(column, low, high, false);
        }

        // NOT BETWEEN
        if (current().type == TokenType.NOT && peek(1).type == TokenType.BETWEEN) {
            consume(TokenType.NOT);
            consume(TokenType.BETWEEN);
            String low = consumeLiteral().value;
            consume(TokenType.AND);
            String high = consumeLiteral().value;
            return new BetweenCondition(column, low, high, true);
        }

        // IN / NOT IN
        if (match(TokenType.IN)) {
            return parseInRhs(column, false);
        }
        if (current().type == TokenType.NOT && peek(1).type == TokenType.IN) {
            consume(TokenType.NOT);
            consume(TokenType.IN);
            return parseInRhs(column, true);
        }

        // IS [NOT] NULL
        if (current().type == TokenType.IS) {
            consume(TokenType.IS);
            boolean negated = match(TokenType.NOT);
            consume(TokenType.NULL);
            return new IsNullCondition(column, negated);
        }

        // LIKE
        if (match(TokenType.LIKE)) {
            String pattern = consumeLiteral().value;
            return new SimpleCondition(column, "LIKE", pattern, false);
        }

        // 标准比较：=, !=, <>, >, >=, <
        String operator = consume(TokenType.EQ, TokenType.LT, TokenType.GT,
            TokenType.LE, TokenType.GE, TokenType.NE).value;
        if (current().type == TokenType.IDENTIFIER && !isLiteralLikeKeyword(current().type)) {
            return new SimpleCondition(column, operator, parseIdentifierPath(), true);
        }
        return new SimpleCondition(column, operator, consumeLiteral().value, false);
    }

    /** 解析 IN 的右操作数：值列表或子查询 */
    private Condition parseInRhs(String column, boolean negated) {
        consume(TokenType.LPAREN);
        // 子查询：IN (SELECT ...)
        if (current().type == TokenType.SELECT) {
            SelectStatement subSelect = parseSelect();
            consume(TokenType.RPAREN);
            return new InSubqueryCondition(column, new SubqueryExpression(subSelect), negated);
        }
        // 值列表：IN (v1, v2, ...)
        List<String> values = new ArrayList<>();
        do {
            values.add(consumeLiteral().value);
        } while (match(TokenType.COMMA));
        consume(TokenType.RPAREN);
        return new InCondition(column, values, negated);
    }

    /** 判断 token 是否看起来像字面量而非列名（防止误把关键字当标识符） */
    private boolean isLiteralLikeKeyword(TokenType type) {
        return type == TokenType.AND || type == TokenType.OR || type == TokenType.NOT
            || type == TokenType.NULL || type == TokenType.IS || type == TokenType.IN
            || type == TokenType.BETWEEN || type == TokenType.LIKE || type == TokenType.EXISTS
            || type == TokenType.SELECT || type == TokenType.WHERE || type == TokenType.ON
            || type == TokenType.JOIN || type == TokenType.LEFT || type == TokenType.RIGHT
            || type == TokenType.INNER || type == TokenType.FULL || type == TokenType.OUTER
            || type == TokenType.GROUP || type == TokenType.HAVING || type == TokenType.ORDER
            || type == TokenType.LIMIT || type == TokenType.OFFSET || type == TokenType.ASC
            || type == TokenType.DESC || type == TokenType.FROM || type == TokenType.SET;
    }

    // ==================== 其他语句 ====================

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
        do { values.add(consumeLiteral().value); } while (match(TokenType.COMMA));
        consume(TokenType.RPAREN);
        stmt.setValues(values);
        return stmt;
    }

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
        if (match(TokenType.WHERE)) stmt.setWhere(parseCondition());
        return stmt;
    }

    private DeleteStatement parseDelete() {
        consume(TokenType.DELETE);
        consume(TokenType.FROM);
        DeleteStatement stmt = new DeleteStatement();
        stmt.setTable(parseIdentifierPath());
        if (match(TokenType.WHERE)) stmt.setWhere(parseCondition());
        return stmt;
    }

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
                    if (partitionKeys.isEmpty()) partitionKeys.add(primaryKey);
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

    private ColumnDef parseColumnDefinition() {
        String columnName = parseIdentifierPath();
        Token typeToken = consume(TokenType.INT, TokenType.BIGINT, TokenType.VARCHAR,
            TokenType.DOUBLE, TokenType.STRING_TYPE, TokenType.TEXT);
        ColumnType type = mapColumnType(typeToken.type);
        int length = 0;
        if (match(TokenType.LPAREN)) {
            length = Integer.parseInt(consume(TokenType.INTEGER).value);
            consume(TokenType.RPAREN);
        }
        boolean nullable = true;
        if (match(TokenType.PRIMARY)) { consume(TokenType.KEY); nullable = false; }
        return new ColumnDef(columnName, type, length, nullable);
    }

    private void parsePrimaryKeyClause(List<String> partitionKeys, List<String> clusteringKeys) {
        consume(TokenType.PRIMARY);
        consume(TokenType.KEY);
        consume(TokenType.LPAREN);
        if (match(TokenType.LPAREN)) {
            partitionKeys.addAll(parseIdentifierList());
            consume(TokenType.RPAREN);
            if (match(TokenType.COMMA)) clusteringKeys.addAll(parseIdentifierList());
        } else {
            partitionKeys.add(parseIdentifierPath());
        }
        consume(TokenType.RPAREN);
    }

    private DropTableStatement parseDrop() {
        consume(TokenType.DROP);
        consume(TokenType.TABLE);
        DropTableStatement stmt = new DropTableStatement();
        stmt.setTable(parseIdentifierPath());
        return stmt;
    }

    // ==================== 工具方法 ====================

    private List<String> parseIdentifierList() {
        List<String> values = new ArrayList<>();
        do { values.add(parseIdentifierPath()); } while (match(TokenType.COMMA));
        return values;
    }

    private String parseIdentifierPath() {
        String value = consume(TokenType.IDENTIFIER).value;
        while (match(TokenType.DOT)) {
            value = value + "." + consume(TokenType.IDENTIFIER).value;
        }
        return value;
    }

    private ColumnType mapColumnType(TokenType tokenType) {
        switch (tokenType) {
            case STRING_TYPE: return ColumnType.STRING;
            case TEXT: return ColumnType.TEXT;
            default: return ColumnType.valueOf(tokenType.name());
        }
    }

    private Token peek(int offset) {
        int idx = position + offset;
        return idx < tokens.size() ? tokens.get(idx) : tokens.get(tokens.size() - 1);
    }

    private boolean match(TokenType expected) {
        if (current().type != expected) return false;
        position++;
        return true;
    }

    private Token current() {
        return tokens.get(position);
    }

    private Token consume(TokenType expected) {
        Token token = current();
        if (token.type != expected) {
            throw new RuntimeException("Expected " + expected + " but got " + token.type);
        }
        position++;
        return token;
    }

    private Token consume(TokenType... expected) {
        Token token = current();
        for (TokenType type : expected) {
            if (token.type == type) { position++; return token; }
        }
        throw new RuntimeException("Unexpected token: " + token.type);
    }

    private Token consumeLiteral() {
        Token token = current();
        if (token.type == TokenType.STRING || token.type == TokenType.INTEGER || token.type == TokenType.FLOAT) {
            position++;
            return token;
        }
        throw new RuntimeException("Expected literal but got " + token.type);
    }
}
