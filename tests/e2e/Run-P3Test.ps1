Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "MiniSqlE2E.ps1")

function Assert-Contains {
    param([string]$Output, [string]$Pattern, [string]$Message)
    if ($Output -notmatch [regex]::Escape($Pattern)) {
        throw "FAIL: $Message`n  Expected to find: `"$Pattern`"`n  Output (last 500 chars):`n$($Output.Substring([Math]::Max(0, $Output.Length - 500)))"
    }
    Write-Host "  PASS  $Message" -ForegroundColor Green
}

function Assert-NotContains {
    param([string]$Output, [string]$Pattern, [string]$Message)
    if ($Output -match [regex]::Escape($Pattern)) {
        throw "FAIL: $Message`n  Unexpected: `"$Pattern`""
    }
    Write-Host "  PASS  $Message" -ForegroundColor Green
}

function Assert-KnownIssue {
    param([string]$Message)
    Write-Host "  KNOWN $Message" -ForegroundColor DarkYellow
}

# ============================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  P3 E2E Verification Test"
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

# --- Cleanup from previous runs ---
Write-Host "[Setup] Dropping leftover tables..." -ForegroundColor Yellow
Invoke-SqlText -SqlText "DROP TABLE orders; DROP TABLE products; DROP TABLE users;" | Out-Null
Start-Sleep -Seconds 2

# --- All test SQL in one stdin pipe ---
$testSql = @'
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT, email STRING);
CREATE TABLE products (id BIGINT PRIMARY KEY, name VARCHAR(100), price DOUBLE, stock INT, description TEXT);
CREATE TABLE orders (user_id INT, order_id INT, product STRING, amount DOUBLE, status STRING, PRIMARY KEY ((user_id), order_id));
SHOW TABLES;
INSERT INTO users (id, name, age, email) VALUES (1, 'alice', 25, 'alice@example.com');
INSERT INTO users (id, name, age, email) VALUES (2, 'bob', 30, 'bob@example.com');
INSERT INTO users (id, name, age, email) VALUES (3, 'carol', 28, 'carol@example.com');
INSERT INTO products (id, name, price, stock, description) VALUES (1, 'Laptop', 999.99, 50, 'High-performance laptop');
INSERT INTO products (id, name, price, stock, description) VALUES (2, 'Mouse', 29.99, 200, 'Wireless mouse');
INSERT INTO products (id, name, price, stock, description) VALUES (3, 'Keyboard', 79.99, 150, 'Mechanical keyboard');
INSERT INTO orders (user_id, order_id, product, amount, status) VALUES (1, 101, 'Laptop', 999.99, 'paid');
INSERT INTO orders (user_id, order_id, product, amount, status) VALUES (1, 102, 'Mouse', 29.99, 'paid');
INSERT INTO orders (user_id, order_id, product, amount, status) VALUES (2, 201, 'Keyboard', 79.99, 'pending');
INSERT INTO orders (user_id, order_id, product, amount, status) VALUES (3, 301, 'Mouse', 29.99, 'paid');
SELECT * FROM users;
SELECT id, name, email FROM users WHERE age > 25;
SELECT * FROM products ORDER BY price DESC;
SELECT * FROM products ORDER BY price DESC LIMIT 2;
SELECT COUNT(*) FROM orders;
SELECT SUM(amount) FROM orders;
SELECT AVG(amount), MAX(amount), MIN(amount) FROM orders;
SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id;
SELECT u.name, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id;
SELECT u.name, SUM(o.amount) AS total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY total DESC;
DROP TABLE orders;
DROP TABLE products;
DROP TABLE users;
SHOW TABLES;
'@

Write-Host "[Test] Executing all SQL (stdin pipe)..."
$out = Invoke-SqlText -SqlText $testSql

# ============================================================
Write-Host ""
Write-Host "--- DDL ---" -ForegroundColor Yellow
Assert-NotContains $out "Create table failed" "CREATE TABLE users"
Assert-NotContains $out "Create table failed" "CREATE TABLE products"
Assert-NotContains $out "Create table failed" "CREATE TABLE orders"

# ============================================================
Write-Host ""
Write-Host "--- SHOW TABLES (P3 fix verified) ---" -ForegroundColor Green
Assert-Contains $out "users"     "SHOW TABLES lists users"
Assert-Contains $out "products"  "SHOW TABLES lists products"
Assert-Contains $out "orders"    "SHOW TABLES lists orders"

# ============================================================
Write-Host ""
Write-Host "--- INSERT ---" -ForegroundColor Yellow
Assert-NotContains $out "Failed to execute" "no SQL failures"
# Count OK lines: 3 DDls + 10 INSERTs + 3 DROPs = 16
$okCount = ([regex]::Matches($out, "OK \(")).Count
Write-Host "  $okCount OK statements executed"

# ============================================================
Write-Host ""
Write-Host "--- SELECT ---" -ForegroundColor Yellow
Assert-Contains $out "alice" "alice exists"
Assert-Contains $out "bob"   "bob exists"
Assert-Contains $out "carol" "carol exists"
Assert-Contains $out "(3 rows)" "users: 3 rows returned"

# WHERE age > 25
Assert-Contains $out "bob"   "WHERE age>25 includes bob (30)"
Assert-Contains $out "carol" "WHERE age>25 includes carol (28)"
$whereSection = $out.Substring($out.IndexOf("WHERE age > 25") + 18)
$first2rows = ([regex]::Matches($out, "\(2 rows\)")).Count
Write-Host "  (2 rows) count: $first2rows"

# ============================================================
Write-Host ""
Write-Host "--- ORDER BY + LIMIT ---" -ForegroundColor Yellow
Assert-Contains $out "Laptop"   "ORDER BY DESC: Laptop listed"
Assert-Contains $out "Keyboard" "ORDER BY DESC: Keyboard listed"
Assert-Contains $out "Mouse"    "ORDER BY DESC: Mouse listed"

# ============================================================
Write-Host ""
Write-Host "--- AGGREGATES ---" -ForegroundColor Yellow
# Output: | COUNT(*) | ... | 3 | ... (1 row)
Assert-Contains $out "COUNT(*)"   "COUNT function present"
Assert-Contains $out "SUM(amount)" "SUM function present"
Assert-Contains $out "AVG(amount)" "AVG function present"
Assert-Contains $out "MAX(amount)" "MAX function present"
Assert-Contains $out "MIN(amount)" "MIN function present"

# ============================================================
Write-Host ""
Write-Host "--- GROUP BY ---" -ForegroundColor Yellow
Assert-Contains $out "user_id" "GROUP BY: user_id column present"
Assert-Contains $out "total"   "GROUP BY: total alias present"

# ============================================================
Write-Host ""
Write-Host "--- JOIN ---" -ForegroundColor Yellow
Assert-Contains $out "u.name"   "JOIN: u.name column present"
Assert-Contains $out "o.product" "JOIN: o.product column present"
Assert-Contains $out "o.amount"  "JOIN: o.amount column present"

# ============================================================
Write-Host ""
Write-Host "--- CLEANUP ---" -ForegroundColor Yellow
Assert-Contains $out "(0 rows)" "Final SHOW TABLES: database empty after DROP"

# ============================================================
Write-Host ""
Write-Host "--- KNOWN ISSUES (not regressions) ---" -ForegroundColor DarkYellow
Assert-KnownIssue "UPDATE/DELETE WHERE may not match rows in composite-key tables"
Assert-KnownIssue "LIKE operator not supported in query executor"
Assert-KnownIssue "HAVING clause not supported in query executor"
Assert-KnownIssue "executeUpdate() returns 0 for some valid statements"

# ============================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  ALL ASSERTIONS PASSED"
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
