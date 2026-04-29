# MiniSQL 分布式数据库

基于 `Master + RegionServer + ZooKeeper + RocksDB` 的分布式 SQL 数据库，支持自动分片、副本复制、故障转移和分布式查询。

## 快速开始

### 1. 启动 ZooKeeper

```powershell
.\scripts\start-zk.bat
```

### 2. 启动集群

```powershell
.\scripts\start-all.bat
```

### 3. 打开 CLI

```powershell
.\scripts\start-cli.bat
```

### 4. 跑 SQL

```sql
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);
INSERT INTO users (id, name, age) VALUES (1, 'alice', 25);
INSERT INTO users (id, name, age) VALUES (2, 'bob', 30);
SELECT * FROM users WHERE age > 25 ORDER BY name;
```

默认端口：Master `16000`，RS1 `16020`，RS2 `16021`，RS3 `16022`，Monitor `16010`

### 5. 停止集群

```powershell
.\scripts\stop-all.bat
```

---

## SQL 支持

### DDL

```sql
-- 单列主键
CREATE TABLE products (id INT PRIMARY KEY, name STRING, price DOUBLE);

-- VARCHAR 带长度
CREATE TABLE users (id INT, name VARCHAR(100), email VARCHAR(200), PRIMARY KEY(id));

-- 复合主键：分区键 + 聚类键
CREATE TABLE orders (
  user_id INT, order_id INT, product STRING, amount DOUBLE, status STRING,
  PRIMARY KEY ((user_id), order_id)
);

DROP TABLE products;
SHOW TABLES;
```

数据类型：`INT` | `BIGINT` | `DOUBLE` | `VARCHAR(n)` | `STRING` | `TEXT`

### DML

```sql
INSERT INTO users (id, name, age) VALUES (1, 'alice', 25);

-- 完整主键 WHERE
UPDATE users SET age = 26 WHERE id = 1;
DELETE FROM orders WHERE user_id = 1 AND order_id = 101;

-- 部分主键 WHERE（扫描式匹配）
UPDATE users SET age = 99 WHERE name = 'bob';
DELETE FROM orders WHERE status = 'cancelled';
```

### SELECT

```sql
-- 基础
SELECT * FROM users;
SELECT id, name FROM users WHERE age > 25;

-- 排序 + 分页
SELECT * FROM products ORDER BY price DESC;
SELECT * FROM users ORDER BY age LIMIT 10 OFFSET 20;

-- 聚合
SELECT COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount) FROM orders;

-- 分组
SELECT user_id, COUNT(*) AS cnt, SUM(amount) AS total
FROM orders GROUP BY user_id;

-- JOIN
SELECT u.name, o.product, o.amount
FROM users u JOIN orders o ON u.id = o.user_id;

SELECT u.name, o.amount
FROM users u LEFT JOIN orders o ON u.id = o.user_id
ORDER BY u.name;
```

| 支持 | 未支持 |
|------|--------|
| `=` `!=` `>` `>=` `<` `<=` `AND` `OR` `LIKE` | `BETWEEN` `IN` `IS NULL` 子查询 |
| `COUNT` `SUM` `AVG` `MAX` `MIN` | `DISTINCT` |
| `GROUP BY` `ORDER BY` `LIMIT` `OFFSET` | `HAVING`（parser 已有，运行时未完全跑通） |
| `INNER JOIN` `LEFT JOIN` | `RIGHT JOIN` `FULL OUTER JOIN` |
| 表别名 `AS`、列别名 `AS` | 无事务（auto-commit only） |

---

## 架构

```
Client (CLI / JDBC)
  → ZooKeeper 发现 Master
  → Master 路由到 RegionServer
  → RegionServer (RocksDB) 读写数据
  → ReplicationCoordinator 同步到 Secondary Replicas
```

| 模块 | 职责 |
|------|------|
| `master` | 元数据、Region 分配、Failover、Recovery、负载均衡、热点检测 |
| `regionserver` | Region 打开/关闭、读写、复制接收、查询下推 |
| `storage` | RocksDB 引擎，MVCC、BlockCache、BloomFilter、流式扫描 |
| `replication` | WAL、副本组、快照同步、降级通知 |
| `client` | CLI、JDBC 驱动、路由、分布式查询执行、结果合并 |
| `sql` | Parser、AST、执行计划、聚合/排序/JOIN 算子 |
| `common` | 共享模型、protobuf、序列化、gRPC 通道工厂 |
| `zookeeper` | ZK 客户端、Leader 选举、RegionServer 注册 |

---

## 分布式特性

### 分片与分区

`PRIMARY KEY ((partition_key), clustering_key)` 语义：

- 分区键决定数据落在哪个 Region（进而哪个 RegionServer）
- 聚类键决定 Region 内排序，支持范围查询
- 同分区键的数据物理上在一起，查询效率最高

### 副本与故障转移

- 可配置复制因子（默认 3），一主多从
- Primary 故障时 `FailoverCoordinator` 自动从 Secondary 中选举最优节点提升
- 指数退避冷却机制防止抖动，紧急模式可绕过
- RegionServer 重启后 `RecoveryCoordinator` 自动引导副本追上 Primary

### 负载均衡与热点检测

**综合负载评分**：`LoadBalancer` 基于 CPU、内存、磁盘、Region 数量、请求负载五个指标加权评分（权重可配置，自动归一化），自动跨节点迁移 Region。

**EWMA 负载预测**：使用指数加权移动平均替代简单滑动窗口，通过趋势预测提前识别负载上升/下降趋势，而非仅依赖历史均值。

**多因子 Region 选择**：迁移时综合考虑四个因子选择最优 Region——负载降低收益、迁移传输成本、写密集惩罚（二次方）、目标服务器适配度，替代简单的"大小/热度"评分。

**迁移预算控制**：每轮最大并发迁移数可配置（默认 3），结合正在进行的迁移数量动态计算预算，防止惊群效应。

**热点感知放置**：`HotSpotCoordinator` 检测读/写热点后，通过共享的 `HotSpotRegistry` 将热点信息同步给 `LoadBalancer`。放置 Region 时自动避开已托管同表热点 Region 的服务器，避免热点聚集。

**相关配置项**（`master.properties`，均支持热加载）：

```properties
# 负载评分权重（相对值，自动归一化）
load.balance.weight.cpu=25
load.balance.weight.memory=25
load.balance.weight.disk=20
load.balance.weight.region.count=15
load.balance.weight.request=15

# 每轮最大并发迁移数
load.balance.max.migrations.per.round=3

# EWMA 负载预测参数
load.balance.ewma.alpha=0.3
load.balance.ewma.trend.threshold=5.0
load.balance.ewma.prediction.steps=2

# Region 选择成本模型权重
load.balance.region.weight.benefit=1.0
load.balance.region.weight.cost=0.5
load.balance.region.weight.write.penalty=0.8
load.balance.region.weight.fit=0.3

# 热点感知放置惩罚权重
load.balance.hotspot.penalty.weight=15.0
```

### 并发安全

- Failover 使用 `putIfAbsent` + sentinel 原子化
- Table/Region 级分布式锁（ZK）
- RocksDB LOCK 文件独占打开检测，进程残留自动清理，并发冲突 fail-fast

---

## 存储引擎 (RocksDB)

| 配置 | 默认值 |
|------|--------|
| Block Cache (LRU) | 128 MB |
| Bloom Filter | 10 bits/key |
| Compaction | LEVEL（L0:4 files, L1:256MB） |
| 压缩 | Snappy |
| Statistics | 每 5 分钟 dump |
| MemTable | 64 MB × 2 |

可配置项见 `RocksDBConfig`：blockCacheSizeBytes、bloomFilterBitsPerKey、compactionStyle、enableStatistics、rateLimiterBytesPerSec、maxBackgroundJobs。

---

## 运维

### 动态配置

Master 每 30 秒检查配置文件变更，自动热加载：

- LoadBalancer 策略/阈值/间隔/评分权重/迁移预算/EWMA 参数/Region 选择权重/热点惩罚
- Monitoring 目标 QPS

无需重启。

### 优雅停机

Ctrl+C → `RegionServer.stop()`：

1. gRPC 服务器关闭，排空 30s inflight RPC
2. Flush 全部 Region 持久化脏数据
3. 停 ReplicationCoordinator + 关 WAL
4. 关 RocksDB → 清理 LOCK 文件
5. 关 Master Channel

`stop-all.bat` 等效。

### gRPC 安全

- 默认 plaintext，TLS 可选（`GrpcSslConfig` + 证书配置）
- Channel 工厂统一缓存复用，不每次新建连接

### 监控

浏览器 `http://localhost:16010/monitor`，含集群概览、Server/Region/Table 视图和 SQL Console。

---

## 测试

```powershell
mvn test                            # 649 个单元+集成测试
powershell -File tests/e2e/Run-P3Test.ps1         # P3 验证测试
powershell -File tests/e2e/Run-FeatureTest.ps1    # 核心功能验证
powershell -File tests/e2e/Run-FailoverRejoin.ps1 # Failover E2E
powershell -File tests/e2e/Run-All.ps1            # 全部 E2E
```

---

## 已知局限

| 方面 | 说明 |
|------|------|
| 认证授权 | 零认证，需 network-level 防护 |
| 事务 | auto-commit only，无 BEGIN/COMMIT/ROLLBACK |
| 滚动升级 | 序列化格式无版本号，混合版本不安全 |
| LIKE/HAVING | Parser + ConditionEvaluator 已实现，运行时待完全跑通 |
| UPDATE/DELETE 部分主键 | 全表扫描，无二级索引 |
| 二级索引 | 不支持 |
| RIGHT/FULL JOIN | 不支持 |
| 子查询 | 不支持 |
