package com.minisql.sql;

import com.minisql.sql.Lexer.Token;
import com.minisql.sql.Lexer.TokenType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lexer 词法分析器单元测试
 */
@DisplayName("Lexer 词法分析器单元测试")
class LexerTest {

    @Test
    @DisplayName("测试 SELECT 语句分词")
    void testSelectTokenize() {
        String sql = "SELECT * FROM users WHERE id = 1";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        assertEquals(TokenType.SELECT, tokens.get(0).type);
        assertEquals(TokenType.ASTERISK, tokens.get(1).type);
        assertEquals(TokenType.FROM, tokens.get(2).type);
        assertEquals(TokenType.IDENTIFIER, tokens.get(3).type);
        assertEquals("users", tokens.get(3).value);
        assertEquals(TokenType.WHERE, tokens.get(4).type);
        assertEquals(TokenType.IDENTIFIER, tokens.get(5).type);
        assertEquals("id", tokens.get(5).value);
        assertEquals(TokenType.EQ, tokens.get(6).type);
        assertEquals(TokenType.INTEGER, tokens.get(7).type);
        assertEquals("1", tokens.get(7).value);
        assertEquals(TokenType.EOF, tokens.get(tokens.size() - 1).type);
    }

    @Test
    @DisplayName("测试 INSERT 语句分词")
    void testInsertTokenize() {
        String sql = "INSERT INTO users (id, name) VALUES (1, 'zhangsan')";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        assertEquals(TokenType.INSERT, tokens.get(0).type);
        assertEquals(TokenType.INTO, tokens.get(1).type);
        assertEquals(TokenType.IDENTIFIER, tokens.get(2).type);
        assertEquals("users", tokens.get(2).value);
        assertEquals(TokenType.LPAREN, tokens.get(3).type);
        assertEquals(TokenType.IDENTIFIER, tokens.get(4).type);
        assertEquals("id", tokens.get(4).value);
        assertEquals(TokenType.COMMA, tokens.get(5).type);
        assertEquals(TokenType.IDENTIFIER, tokens.get(6).type);
        assertEquals("name", tokens.get(6).value);
        assertEquals(TokenType.RPAREN, tokens.get(7).type);
        assertEquals(TokenType.VALUES, tokens.get(8).type);
        assertEquals(TokenType.LPAREN, tokens.get(9).type);
        assertEquals(TokenType.INTEGER, tokens.get(10).type);
        assertEquals("1", tokens.get(10).value);
        assertEquals(TokenType.COMMA, tokens.get(11).type);
        assertEquals(TokenType.STRING, tokens.get(12).type);
        assertEquals("zhangsan", tokens.get(12).value);
    }

    @Test
    @DisplayName("测试字符串字面量解析")
    void testStringLiteral() {
        String sql = "SELECT * FROM users WHERE name = 'John Doe'";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        Token stringToken = tokens.stream()
                .filter(t -> t.type == TokenType.STRING)
                .findFirst()
                .orElse(null);

        assertNotNull(stringToken);
        assertEquals("John Doe", stringToken.value);
    }

    @Test
    @DisplayName("测试数字字面量解析")
    void testNumberLiteral() {
        String sql = "SELECT * FROM users WHERE age = 25 AND score = 95.5";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        List<Token> numberTokens = tokens.stream()
                .filter(t -> t.type == TokenType.INTEGER || t.type == TokenType.FLOAT)
                .collect(java.util.stream.Collectors.toList());

        assertEquals(2, numberTokens.size());
        assertEquals("25", numberTokens.get(0).value);
        assertEquals("95.5", numberTokens.get(1).value);
        assertEquals(TokenType.INTEGER, numberTokens.get(0).type);
        assertEquals(TokenType.FLOAT, numberTokens.get(1).type);
    }

    @Test
    @DisplayName("测试操作符解析")
    void testOperators() {
        String sql = "SELECT * FROM users WHERE id = 1 AND age > 18 AND score <= 100";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.EQ && t.value.equals("=")));
        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.GT && t.value.equals(">")));
        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.LE && t.value.equals("<=")));
    }

    @Test
    @DisplayName("测试 DISTINCT 操作符解析")
    void testDistinctOperators() {
        String[] operators = {"<>", "!=", ">=", "<="};
        TokenType[] expectedTypes = {TokenType.NE, TokenType.NE, TokenType.GE, TokenType.LE};

        for (int i = 0; i < operators.length; i++) {
            String sql = "SELECT * FROM t WHERE a " + operators[i] + " b";
            Lexer lexer = new Lexer(sql);
            List<Token> tokens = lexer.tokenize();

            final int index = i;
            Token opToken = tokens.stream()
                    .filter(t -> t.type == expectedTypes[index])
                    .findFirst()
                    .orElse(null);

            assertNotNull(opToken, "Operator " + operators[i] + " should be recognized");
        }
    }

    @Test
    @DisplayName("测试关键字识别")
    void testKeywords() {
        String[] keywords = {"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
                            "CREATE", "TABLE", "DROP", "JOIN", "ORDER", "BY", "LIMIT",
                            "ASC", "DESC", "AND", "OR", "NOT", "NULL"};

        for (String keyword : keywords) {
            Lexer lexer = new Lexer(keyword);
            Token token = lexer.nextToken();

            assertNotNull(token);
            assertTrue(TokenType.KEYWORDS.containsKey(keyword),
                    keyword + " should be a keyword");
        }
    }

    @Test
    @DisplayName("测试标识符大小写保留")
    void testIdentifierCasePreservation() {
        String sql = "SELECT * FROM MyTable WHERE userName = 'John'";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        Token tableToken = tokens.stream()
                .filter(t -> t.type == TokenType.IDENTIFIER && t.value.equals("MyTable"))
                .findFirst()
                .orElse(null);

        assertNotNull(tableToken);
        assertEquals("MyTable", tableToken.value, "Table name case should be preserved");

        Token userToken = tokens.stream()
                .filter(t -> t.type == TokenType.IDENTIFIER && t.value.equals("userName"))
                .findFirst()
                .orElse(null);

        assertNotNull(userToken);
        assertEquals("userName", userToken.value, "Column name case should be preserved");
    }

    @Test
    @DisplayName("测试空白字符处理")
    void testWhitespaceHandling() {
        String sql1 = "SELECT * FROM users";
        String sql2 = "SELECT    *    FROM   users";
        String sql3 = "SELECT\t*\r\nFROM\t\tusers";

        Lexer lexer1 = new Lexer(sql1);
        Lexer lexer2 = new Lexer(sql2);
        Lexer lexer3 = new Lexer(sql3);

        List<Token> tokens1 = lexer1.tokenize();
        List<Token> tokens2 = lexer2.tokenize();
        List<Token> tokens3 = lexer3.tokenize();

        // 验证所有分词结果相同（忽略空白字符）
        assertEquals(tokens1.size(), tokens2.size());
        assertEquals(tokens1.size(), tokens3.size());

        for (int i = 0; i < tokens1.size(); i++) {
            assertEquals(tokens1.get(i).type, tokens2.get(i).type,
                    "Token " + i + " type should match");
            assertEquals(tokens1.get(i).value, tokens2.get(i).value,
                    "Token " + i + " value should match");
        }
    }

    @Test
    @DisplayName("测试空 SQL 语句")
    void testEmptySql() {
        String sql = "";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        assertEquals(1, tokens.size());
        assertEquals(TokenType.EOF, tokens.get(0).type);
    }

    @Test
    @DisplayName("测试 Token toString 方法")
    void testTokenToString() {
        Token token = new Token(TokenType.SELECT, "SELECT");
        String result = token.toString();

        assertTrue(result.contains("SELECT"));
    }

    @Test
    @DisplayName("测试复杂 SQL 语句分词")
    void testComplexSql() {
        String sql = "SELECT u.id, u.name, o.total " +
                    "FROM users u " +
                    "JOIN orders o ON u.id = o.user_id " +
                    "WHERE u.status = 'active' AND o.total > 100 " +
                    "ORDER BY o.total DESC " +
                    "LIMIT 10 OFFSET 5";

        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        // 验证基本结构
        assertEquals(TokenType.SELECT, tokens.get(0).type);
        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.JOIN));
        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.ON));
        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.ORDER));
        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.DESC));
        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.LIMIT));
        assertTrue(tokens.stream().anyMatch(t -> t.type == TokenType.OFFSET));
    }

    @Test
    @DisplayName("测试注释外的特殊字符处理")
    void testSpecialCharacters() {
        // 测试下划线在标识符中
        String sql = "SELECT user_name FROM user_table";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        assertEquals("user_name", tokens.get(1).value);
        assertEquals("user_table", tokens.get(3).value);
    }

    @Test
    @DisplayName("测试下一个 Token 方法")
    void testNextToken() {
        String sql = "SELECT id FROM users";
        Lexer lexer = new Lexer(sql);

        Token token1 = lexer.nextToken();
        assertEquals(TokenType.SELECT, token1.type);

        Token token2 = lexer.nextToken();
        assertEquals(TokenType.IDENTIFIER, token2.type);
        assertEquals("id", token2.value);

        Token token3 = lexer.nextToken();
        assertEquals(TokenType.FROM, token3.type);

        // 跳过所有 token 后应该返回 EOF
        while (lexer.nextToken().type != TokenType.EOF) {
            // skip
        }
        Token eof = lexer.nextToken();
        assertEquals(TokenType.EOF, eof.type);
    }

    @Test
    @DisplayName("测试浮点数解析")
    void testFloatParsing() {
        String sql = "SELECT * FROM products WHERE price = 19.99";
        Lexer lexer = new Lexer(sql);
        List<Token> tokens = lexer.tokenize();

        Token floatToken = tokens.stream()
                .filter(t -> t.type == TokenType.FLOAT)
                .findFirst()
                .orElse(null);

        assertNotNull(floatToken);
        assertEquals("19.99", floatToken.value);
    }
}
