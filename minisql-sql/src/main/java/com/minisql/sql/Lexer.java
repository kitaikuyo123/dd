package com.minisql.sql;

import com.minisql.sql.ast.*;

import java.util.ArrayList;
import java.util.List;

/**
 * SQL 词法分析器
 * 负责模块: 开发者C
 */
public class Lexer {

    private final String sql;
    private final String upperSql;
    private int position;
    private int length;

    public Lexer(String sql) {
        this.sql = sql;
        this.upperSql = sql.toUpperCase();
        this.length = sql.length();
        this.position = 0;
    }

    /**
     * 获取下一个Token
     */
    public Token nextToken() {
        skipWhitespace();

        if (position >= length) {
            return new Token(TokenType.EOF, "");
        }

        char ch = sql.charAt(position);

        // 标识符或关键字
        if (Character.isLetter(ch) || ch == '_') {
            return readIdentifierOrKeyword();
        }

        // 数字
        if (Character.isDigit(ch)) {
            return readNumber();
        }

        // 字符串
        if (ch == '\'') {
            return readString();
        }

        // 操作符和点号
        switch (ch) {
            case '.':
                position++;
                return new Token(TokenType.DOT, ".");
            case ',':
                position++;
                return new Token(TokenType.COMMA, ",");
            case '(':
                position++;
                return new Token(TokenType.LPAREN, "(");
            case ')':
                position++;
                return new Token(TokenType.RPAREN, ")");
            case ';':
                position++;
                return new Token(TokenType.SEMICOLON, ";");
            case '*':
                position++;
                return new Token(TokenType.ASTERISK, "*");
            case '=':
                position++;
                return new Token(TokenType.EQ, "=");
            case '<':
                position++;
                if (position < length && sql.charAt(position) == '=') {
                    position++;
                    return new Token(TokenType.LE, "<=");
                } else if (position < length && sql.charAt(position) == '>') {
                    position++;
                    return new Token(TokenType.NE, "<>");
                }
                return new Token(TokenType.LT, "<");
            case '>':
                position++;
                if (position < length && sql.charAt(position) == '=') {
                    position++;
                    return new Token(TokenType.GE, ">=");
                }
                return new Token(TokenType.GT, ">");
            case '!':
                position++;
                if (position < length && sql.charAt(position) == '=') {
                    position++;
                    return new Token(TokenType.NE, "!=");
                }
                throw new RuntimeException("Unexpected character: !");
            default:
                throw new RuntimeException("Unexpected character: " + ch);
        }
    }

    private Token readIdentifierOrKeyword() {
        int start = position;
        while (position < length && (Character.isLetterOrDigit(upperSql.charAt(position)) || upperSql.charAt(position) == '_')) {
            position++;
        }
        // 使用原始 sql 字符串提取标识符的值，保持大小写
        String originalValue = sql.substring(start, position);
        // 使用大写版本判断是否为关键字
        String upperValue = upperSql.substring(start, position);
        TokenType type = TokenType.KEYWORDS.getOrDefault(upperValue, TokenType.IDENTIFIER);
        return new Token(type, originalValue);
    }

    private Token readNumber() {
        int start = position;
        while (position < length && Character.isDigit(sql.charAt(position))) {
            position++;
        }
        if (position < length && sql.charAt(position) == '.') {
            position++;
            while (position < length && Character.isDigit(sql.charAt(position))) {
                position++;
            }
            return new Token(TokenType.FLOAT, sql.substring(start, position));
        }
        return new Token(TokenType.INTEGER, sql.substring(start, position));
    }

    private Token readString() {
        position++;  // 跳过开头的单引号
        int start = position;
        while (position < length && sql.charAt(position) != '\'') {
            position++;
        }
        if (position >= length) {
            throw new RuntimeException("Unterminated string");
        }
        String value = sql.substring(start, position);
        position++;  // 跳过结尾的单引号
        return new Token(TokenType.STRING, value);
    }

    private void skipWhitespace() {
        while (position < length && Character.isWhitespace(sql.charAt(position))) {
            position++;
        }
    }

    /**
     * 获取所有Token
     */
    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        Token token;
        do {
            token = nextToken();
            tokens.add(token);
        } while (token.type != TokenType.EOF);
        return tokens;
    }

    public enum TokenType {
        // 关键字
        SELECT, FROM, WHERE, INSERT, INTO, VALUES,
        UPDATE, SET, DELETE, CREATE, TABLE, DROP,
        PRIMARY, KEY, INT, BIGINT, VARCHAR, DOUBLE, STRING_TYPE, TEXT,
        AND, OR, NOT, NULL, JOIN, ON,
        ORDER, BY, LIMIT, OFFSET, ASC, DESC,
        SHOW, TABLES,

        // 标识符和字面量
        IDENTIFIER, STRING, INTEGER, FLOAT,

        // 操作符
        EQ, LT, GT, LE, GE, NE,

        // 标点符号
        COMMA, LPAREN, RPAREN, SEMICOLON, ASTERISK, DOT,

        EOF;

        static final java.util.Map<String, TokenType> KEYWORDS = new java.util.HashMap<>();
        static {
            KEYWORDS.put("SELECT", SELECT);
            KEYWORDS.put("FROM", FROM);
            KEYWORDS.put("WHERE", WHERE);
            KEYWORDS.put("INSERT", INSERT);
            KEYWORDS.put("INTO", INTO);
            KEYWORDS.put("VALUES", VALUES);
            KEYWORDS.put("UPDATE", UPDATE);
            KEYWORDS.put("SET", SET);
            KEYWORDS.put("DELETE", DELETE);
            KEYWORDS.put("CREATE", CREATE);
            KEYWORDS.put("TABLE", TABLE);
            KEYWORDS.put("DROP", DROP);
            KEYWORDS.put("PRIMARY", PRIMARY);
            KEYWORDS.put("KEY", KEY);
            KEYWORDS.put("INT", INT);
            KEYWORDS.put("BIGINT", BIGINT);
            KEYWORDS.put("VARCHAR", VARCHAR);
            KEYWORDS.put("DOUBLE", DOUBLE);
            KEYWORDS.put("STRING", STRING_TYPE);
            KEYWORDS.put("TEXT", TEXT);
            KEYWORDS.put("AND", AND);
            KEYWORDS.put("OR", OR);
            KEYWORDS.put("NOT", NOT);
            KEYWORDS.put("NULL", NULL);
            KEYWORDS.put("JOIN", JOIN);
            KEYWORDS.put("ON", ON);
            KEYWORDS.put("ORDER", ORDER);
            KEYWORDS.put("BY", BY);
            KEYWORDS.put("LIMIT", LIMIT);
            KEYWORDS.put("OFFSET", OFFSET);
            KEYWORDS.put("ASC", ASC);
            KEYWORDS.put("DESC", DESC);
            KEYWORDS.put("SHOW", SHOW);
            KEYWORDS.put("TABLES", TABLES);
        }
    }

    public static class Token {
        public final TokenType type;
        public final String value;

        public Token(TokenType type, String value) {
            this.type = type;
            this.value = value;
        }

        @Override
        public String toString() {
            return type + "(" + value + ")";
        }
    }
}
