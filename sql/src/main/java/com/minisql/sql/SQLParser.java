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

import java.util.ArrayList;
import java.util.List;

/**
 * Hand-written SQL parser for the subset used by MiniSQL.
 */
public class SQLParser {

    private final List<Token> tokens;
    private int position;

    public SQLParser(String sql) {
        this.tokens = new Lexer(sql).tokenize();
    }

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

    private ShowTablesStatement parseShowTables() {
        consume(TokenType.SHOW);
        consume(TokenType.TABLES);
        return new ShowTablesStatement();
    }

    private SelectStatement parseSelect() {
        consume(TokenType.SELECT);

        SelectStatement stmt = new SelectStatement();
        parseSelectList(stmt);
        consume(TokenType.FROM);
        stmt.setTable(parseIdentifierPath());

        if (match(TokenType.JOIN)) {
            stmt.setJoinTable(parseIdentifierPath());
            consume(TokenType.ON);
            stmt.setJoinCondition(parseCondition());
        }
        if (match(TokenType.WHERE)) {
            stmt.setWhere(parseCondition());
        }
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
            columns.add(parseIdentifierPath());
        } while (match(TokenType.COMMA));
        stmt.setColumns(columns);
    }

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

    private void parseLimit(SelectStatement stmt) {
        if (!match(TokenType.LIMIT)) {
            return;
        }
        stmt.setLimit(Integer.parseInt(consume(TokenType.INTEGER).value));
        if (match(TokenType.OFFSET)) {
            stmt.setOffset(Integer.parseInt(consume(TokenType.INTEGER).value));
        }
    }

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

    private DropTableStatement parseDrop() {
        consume(TokenType.DROP);
        consume(TokenType.TABLE);
        DropTableStatement stmt = new DropTableStatement();
        stmt.setTable(parseIdentifierPath());
        return stmt;
    }

    private Condition parseCondition() {
        return parseOrCondition();
    }

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

    private Condition parsePrimaryCondition() {
        if (match(TokenType.LPAREN)) {
            Condition condition = parseCondition();
            consume(TokenType.RPAREN);
            return condition;
        }
        return parseSimpleCondition();
    }

    private Condition parseSimpleCondition() {
        String column = parseIdentifierPath();
        String operator = consume(TokenType.EQ, TokenType.LT, TokenType.GT, TokenType.LE, TokenType.GE, TokenType.NE).value;

        if (current().type == TokenType.IDENTIFIER) {
            String valueReference = parseIdentifierPath();
            return new SimpleCondition(column, operator, valueReference, true);
        }
        return new SimpleCondition(column, operator, consumeLiteral().value, false);
    }

    private List<String> parseIdentifierList() {
        List<String> values = new ArrayList<>();
        do {
            values.add(parseIdentifierPath());
        } while (match(TokenType.COMMA));
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
            case STRING_TYPE:
                return ColumnType.STRING;
            case TEXT:
                return ColumnType.TEXT;
            default:
                return ColumnType.valueOf(tokenType.name());
        }
    }

    private boolean match(TokenType expected) {
        if (current().type != expected) {
            return false;
        }
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
            if (token.type == type) {
                position++;
                return token;
            }
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
