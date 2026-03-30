# MiniSQL Distributed System

MiniSQL is a distributed MiniSQL prototype built on top of `Master + RegionServer + ZooKeeper + MySQL`.

Current verified end-to-end paths:

- `CREATE TABLE` initializes a primary and ready secondary replicas
- `INSERT / SELECT` works on the primary path
- primary failure triggers automatic failover to a secondary
- restarted RegionServers rejoin and recover business replicas
- `DROP TABLE` cleans metadata, replica groups, and monitor/lifecycle state
- single-table queries support predicate pushdown and projection pushdown
- `JOIN` queries support per-side projection pruning

## Test Entry Points

```powershell
mvn -q test
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-All.ps1
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-Smoke.ps1
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-JoinProjection.ps1
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-FailoverRejoin.ps1
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-DropCleanup.ps1
```

See [TESTING.md](/d:/aLabs/dd/TESTING.md) for the full testing guide.

## Modules

- `minisql-master`
  Control plane: metadata, region assignment, failover, recovery, replica lifecycle.
- `minisql-regionserver`
  Data plane: region open/close, reads, writes, replication receive path, pushdown execution.
- `minisql-storage`
  MySQL-backed KV engine with MVCC and SQL-level scan optimization.
- `minisql-replication`
  Replica groups, WAL, quorum acks, promote/fencing logic.
- `minisql-client`
  CLI/JDBC client, routing, distributed query execution, result merge.
- `minisql-sql`
  SQL parser, AST, and execution-plan helpers.
- `minisql-common`
  Shared models, protobuf contracts, serialization helpers.
- `minisql-zookeeper`
  ZooKeeper utilities.

## Verified Capabilities

- `CREATE TABLE / DROP TABLE / SHOW TABLES`
- `INSERT / UPDATE / DELETE`
- single-table `SELECT`
- `WHERE / ORDER BY / LIMIT / OFFSET`
- `GROUP BY + COUNT / SUM / AVG / MAX / MIN`
- `HAVING`
- `INNER JOIN / LEFT JOIN`
- primary failover
- RegionServer rejoin recovery

## Query Optimization

Implemented optimization features:

- region-level parallel scan
- replica-aware read routing
- single-table predicate pushdown
- single-table projection pushdown
- MySQL SQL-level pushdown for primary-key ranges and simple column predicates
- per-side projection pruning for `JOIN`

Current limits:

- `RIGHT JOIN / FULL OUTER JOIN`
- SQL-level pushdown for complex `OR`
- complex expressions or function pushdown
- full cost-based optimizer

## Quick Start

### 1. Start ZooKeeper

```powershell
cd path/to/zookeeper
zkServer.cmd
```

### 2. Prepare MySQL

Make sure the MySQL settings in `regionserver-1/2/3.properties` are valid


### 3. Start the cluster

```powershell
cmd /c scripts\start-all.bat
```

Default ports:

- Master: `16000`
- RegionServer1: `16020`
- RegionServer2: `16021`
- RegionServer3: `16022`

### 5. Access Web

localhost:16010/monitor

### 6. Run a minimal SQL sample

```sql
SHOW TABLES;
CREATE TABLE products (id INT PRIMARY KEY, name STRING, price INT);
INSERT INTO products (id, name, price) VALUES (1, 'A', 10);
INSERT INTO products (id, name, price) VALUES (2, 'B', 20);
SELECT * FROM products ORDER BY id;
```
