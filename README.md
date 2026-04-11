# MiniSQL 分布式系统

MiniSQL 是一个基于 `Master + RegionServer + ZooKeeper + MySQL` 的分布式数据库原型系统。当前仓库已经完成了控制面、数据面、副本管理、故障恢复、客户端路由和监控页面的一体化实现。

目前已经验证通过的主链路包括：

- `CREATE TABLE` 会初始化 primary 和可用 secondary 副本
- `INSERT / SELECT` 可以在 primary 路径正常执行
- primary 故障后可以自动切换到 secondary
- RegionServer 重启后可以重新加入集群并恢复业务副本
- `DROP TABLE` 会清理元数据、副本组以及监控/生命周期状态
- 单表查询支持谓词下推和投影下推
- `JOIN` 查询支持按左右两侧做投影裁剪

## 最近更新

### 2026/04/11

**SQL 解析统一（AST 驱动的分布式查询）**

将两套 SQL 解析器统一为一套 AST 驱动的执行路径，删除了脆弱的字符串解析。

- **Lexer / AST 扩展**（`sql` 模块）：新增 `GROUP`、`HAVING`、`LEFT`、`INNER`、`AS` 关键字和对应的 `TokenType`；`SelectStatement` 新增 `tableAlias`、`joinType`、`joinTableAlias`、`aggregates`、`groupByColumns`、`having`、`columnAliases` 字段及 `AggregateExpr` 内部类
- **SQLParser 扩展**：重写 `parseSelectList` 支持聚合函数和列别名；`parseFrom` 支持表别名；`parseJoin` 支持 `[LEFT|INNER] JOIN`；新增 `parseGroupBy`、`parseHaving`；完整的 `SELECT ... FROM ... JOIN ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT` 流程
- **MiniSQLConnection 路由统一**：删除 `isComplexDistributedQuery()`、`extractRowKeyFromCondition()`、`isAggregationQuery()`，所有 SELECT 统一走 `SQLParser` → AST → `ParallelQueryExecutor.executeQuery(SelectStatement, String)`
- **ParallelQueryExecutor 消费 AST**：新增 `executeQuery(SelectStatement, String)` 方法及辅助方法 `conditionToSql`、`extractJoinConditionColumns`、`buildAggregateExpressions`、`aggregateRowsFromAst`、`projectRowsFromAst` 等；删除全部字符串解析代码（`parseQuerySpec`、`parseFromPart`、`parseJoinConditions`、`parseSelectItems`、`parseHavingCondition` 等 20+ 方法）和 `QuerySpec`、`JoinQuerySpec`、`TableSource`、`SelectItem` 内部类

**负载均衡修复与优化**

- `LoadBalancer.LoadCalculator`：CPU 指标从未参与计算，现已加入加权公式；权重调整为 CPU=25、Memory=25、Disk=20、Region=15、Request=15（总和 100）
- `isOverloaded` 阈值从 80 调整为 70，与实际负载计算对齐
- 删除未使用的 `needsImmediateRebalance()`、`getServerLoadRanking()` 和 `ServerLoadRank` 内部类

**热点分数规范化**

- `HotSpotCoordinator.calculateDisplayScore`：从无上限的 `requestCount / threshold` 改为 0-100 分制：0-50 对应非热点梯度（QPS/阈值占比），50-100 对应热点梯度（超出阈值部分，3 倍封顶），额外 0-10 分复制延迟惩罚
- `MonitoringService.hotspotScore`：删除 Fallback 公式（原方法在 `hotSpotCoordinator == null` 时用原始请求数计算无意义分数），改为直接返回 0.0

**死代码清理**

- **空指针保护删除**（final 字段不可能为 null）：`MasterServiceImpl` 中 11 处 `if (xxx != null)` 包装；`RegionMigrationCoordinator.transition`、`RegionServer.stop`、`FailoverCoordinator`、`HotSpotCoordinator.addReadReplica` 中各 1 处
- **未使用的公共方法删除**：`FailoverCoordinator` 的 `getFailoverHistory`、`getFailoverState`、`clearFailoverHistory`、`canFailover`、`getOngoingFailovers`、`getStatus`、`getDetailedStatus`；`RegionSplitCoordinator.triggerManualSplit`；`RegionMergeCoordinator.triggerManualMerge` 和 `getMergingRegions`；`HotSpotCoordinator.configureHistoryWindow` 和 `drainPendingActions`（改为 private）；`LoadBalancer.getServerLoadRanking`
- **废弃类删除**：`MigrationCoordinator`（已被 `RegionMigrationCoordinator` 替代）

**分裂→合并冷却连通**

- `RegionSplitCoordinator` 新增 `RegionMergeCoordinator mergeCoordinator` 字段和 setter
- `executeSplit()` 成功后调用 `mergeCoordinator.recordRegionSplit()` 记录两个新 Region，防止刚分裂的 Region 被立即合并
- 在 `MasterServiceImpl` 构造时注入连接

**硬编码值配置文件化**

所有原硬编码的阈值参数现在可通过 `.properties` 文件配置：

| 配置项 | 默认值 | 影响模块 |
|---|---|---|
| `replication.factor` | `3` | MasterMain, RegionServer |
| `region.split.threshold.mb` | `10240`（10GB） | RegionSplitCoordinator |
| `region.merge.threshold.mb` | `100`（100MB） | RegionMergeCoordinator |
| `region.merge.max.size.gb` | `8`（8GB） | RegionMergeCoordinator |
| `region.merge.min.size.mb` | `10`（10MB） | RegionMergeCoordinator |
| `region.merge.cooldown.ms` | `3600000`（1h） | RegionMergeCoordinator |

`MasterServiceImpl` 构造函数新增 `Properties config` 参数，从配置文件读取上述值并注入各 Coordinator。已更新 `master.properties`、`master2.properties`、`regionserver-{1,2,3}.properties`。

### 2026/04/10

- 修复热点检测（`HotSpotCoordinator`）5 个互相关联的问题：
  - 阈值判断仅用最后 2 个快照，单次尖峰即误触发；改为对所有增量取平均值
  - Delta 未按实际心跳间隔归一化；新增 `computePerIntervalDeltas` 方法转为 per-second rate
  - 读写在 if-else 链中读优先于写，写热点可能被掩盖；改为按严重度比较，写优先
  - 读写各自未过阈值但总压力高时无法检测；新增混合热点检测（combined > 70% 合成阈值）
  - 移除 `READ_GROWING` / `WRITE_GROWING` 区分，简化为 `READ` / `WRITE` 两种类型，删除 `isSustainedGrowth` 及 `growthThreshold` 配置
- 修复 `LoadCalculatorTest.testIsOverloaded`：`calculateRequestScore` 首次调用返回 0（它算 QPS 增长率），补充基线建立步骤
- 修复 `ReplicaMonitorTest.testReplicaOfflineThenRecovered`：`performHealthCheck` 已改为空方法（存活检测交给 ZooKeeper），改为直接测试 `updateHeartbeat` 的恢复路径

### 2026/04/01

- 完成 ZooKeeper 协调面重构，统一主路径为：
  - `/minisql/masters`
  - `/minisql/regionservers`
  - `/minisql/tables/{table}/regions/{region}/primary`
  - `/minisql/tables/{table}/regions/{region}/replicas`
  - `/minisql/locks/...`
- 接入多 Master 选主：
  - Master 通过 `/minisql/masters/election` 参与竞争
  - leader 地址发布到 `/minisql/masters/leader`
  - standby Master 不再处理控制面写请求
- RegionServer 改为通过 ZooKeeper 发现并跟随当前 leader Master
- 客户端改为从新的 leader 路径发现 Master，并补上 watcher 驱动的路由刷新
- 故障收敛统一到 ZooKeeper 事件链：
  - RS 上下线以 `/minisql/regionservers` 临时节点为准
  - heartbeat 仅保留运行状态上报语义，不再参与成员收敛
- primary/replicas 元数据统一收敛到表路径下，不再依赖旧的 `/minisql/regions/...`
- split / merge / migration / failover 路径补充分布式锁和元数据收敛
- 监控页 Region 统计口径改为支持实际 Region 实例视图，并清理了多余日志输出
- gRPC 传输栈升级为 `io.grpc:grpc-netty-shaded:1.76.1`
- E2E 脚本与双 Master 验证配置同步更新
- 新增第二份 Master 配置文件 [master2.properties](/d:/aLabs/dd/minisql-master/src/main/resources/master2.properties)，用于双 Master 验证

## 测试入口

```powershell
mvn -q test
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-All.ps1
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-Smoke.ps1
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-JoinProjection.ps1
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-FailoverRejoin.ps1
powershell -ExecutionPolicy Bypass -File tests/e2e/Run-DropCleanup.ps1
```

完整测试说明见 [TESTING.md](/d:/aLabs/dd/TESTING.md)。

## 模块说明

- `minisql-master`
  控制面，负责元数据、Region 分配、故障转移、恢复和副本生命周期管理。
- `minisql-regionserver`
  数据面，负责 Region 打开/关闭、读写请求、复制接收链路和下推执行。
- `minisql-storage`
  基于 MySQL 的 KV 存储引擎，包含 MVCC 和 SQL 级扫描优化。
- `minisql-replication`
  副本组、WAL、仲裁确认、主副本提升和 fencing 逻辑。
- `minisql-client`
  CLI/JDBC 客户端、路由、分布式查询执行和结果合并。
- `minisql-sql`
  SQL 解析、AST 和执行计划辅助逻辑。
- `minisql-common`
  共享模型、protobuf 协议和序列化工具。
- `minisql-zookeeper`
  ZooKeeper 协调与路径管理工具。

## 已验证能力

- `CREATE TABLE / DROP TABLE / SHOW TABLES`
- `INSERT / UPDATE / DELETE`
- 单表 `SELECT`
- `WHERE / ORDER BY / LIMIT / OFFSET`
- `GROUP BY + COUNT / SUM / AVG / MAX / MIN`
- `HAVING`
- `INNER JOIN / LEFT JOIN`
- primary 自动故障转移
- RegionServer 重连恢复
- 多 Master 选主与 leader 切换

## 查询优化

当前已实现的优化能力：

- Region 级并行扫描
- 面向副本的读路由
- 单表谓词下推
- 单表投影下推
- 针对主键范围和简单列条件的 MySQL SQL 级下推
- `JOIN` 两侧的投影裁剪

当前限制：

- `RIGHT JOIN / FULL OUTER JOIN`
- 复杂 `OR` 条件的 SQL 级下推
- 复杂表达式或函数下推
- 完整的基于代价优化器

## 快速开始

### 1. 启动 ZooKeeper

```powershell
cd path\to\zookeeper
zkServer.cmd
```

### 2. 准备 MySQL

请确认 `regionserver-1/2/3.properties` 中的 MySQL 连接参数有效。

### 3. 启动集群

```powershell
cmd /c scripts\start-all.bat
```

默认端口：

- Master：`16000`
- RegionServer1：`16020`
- RegionServer2：`16021`
- RegionServer3：`16022`
- Monitor：`16010`

### 4. 访问监控页

在浏览器中打开：

```text
http://localhost:16010/monitor
```

### 5. 运行最小 SQL 示例

```sql
SHOW TABLES;
CREATE TABLE products (id INT PRIMARY KEY, name STRING, price INT);
INSERT INTO products (id, name, price) VALUES (1, 'A', 10);
INSERT INTO products (id, name, price) VALUES (2, 'B', 20);
SELECT * FROM products ORDER BY id;
```

## 双 Master 验证提示

如果需要验证多 Master 选主，可以同时启动：

- [master.properties](/d:/aLabs/dd/minisql-master/src/main/resources/master.properties)
- [master2.properties](/d:/aLabs/dd/minisql-master/src/main/resources/master2.properties)

要求：

- 两个 Master 使用不同的 `master.port`
- 两个 Master 使用相同的 `zookeeper.connect`
- 两个 Master 使用不同的 `minisql.monitor.port`

启动后可通过 ZooKeeper 检查：

```text
ls /minisql/masters/election
get /minisql/masters/leader
```

当 leader 下线后，`/minisql/masters/leader` 应自动切换到 standby Master。
