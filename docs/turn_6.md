# MiniSQL 第 6 轮迭代设计文档

## 1. 当前系统存在的问题

### 1.1 日志记录不一致

| 问题 | 涉及文件 |
|------|----------|
| 部分文件仍使用 `System.out.println` 输出日志 | 16 个文件 |
| 部分文件仍使用 `System.err.println` 输出错误 | 16 个文件 |
| `ReplicationCoordinator` 使用 `LOGGER` 大写命名，其他使用 `logger` 小写 | `ReplicationCoordinator.java` |

### 1.2 日志记录问题详情

**minisql-replication 模块**：
- `GrpcReplicationTransportClient.java` - 3 处 System.err.println
- `PrimaryChangeNotifier.java` - 3 处 System.err.println
- `ReplicationCoordinator.java` - 4 处 System.err.println
- `ReplicationWAL.java` - 多处 System.out/err.println

**minisql-master 模块**：
- `MasterServiceImpl.java` - 多处 System.out.println
- `RegionMergeCoordinator.java` - 多处 System.out/err.println
- `RegionSplitCoordinator.java` - 多处 System.out.println
- `MetadataManager.java` - 多处 System.out/err.println
- `ClusterManager.java` - 多处 System.out/err.println
- `ReplicaMonitor.java` - 多处 System.out/err.println
- `ReplicaLifecycleManager.java` - 多处 System.out/err.println
- `RegionMigrationCoordinator.java` - 多处 System.out/err.println
- `MigrationCoordinator.java` - 多处 System.out/err.println

**minisql-sql 模块**：
- `QueryExecutor.java` - 多处 System.out.println

**minisql-client 模块**：
- `SqlCli.java` - 多处 System.out.println（CLI 工具，可保留）
- `CliResultFormatter.java` - 多处 System.out.println（CLI 工具，可保留）

### 1.3 LOGGER 命名不一致

- `ReplicationCoordinator.java` 使用 `LOGGER`（大写）
- 其他所有文件使用 `logger`（小写）

---

## 2. 本轮删减策略

### 2.1 统一日志记录（优先级最高）

**目标**：将所有 `System.out.println` 和 `System.err.println` 替换为 SLF4J 日志记录。

**规则**：
1. `System.out.println` → `logger.info()`
2. `System.err.println` → `logger.warn()` 或 `logger.error()`
3. CLI 工具（`SqlCli.java`, `CliResultFormatter.java`）可保留 `System.out.println`，因为它们是用户交互界面

### 2.2 统一 Logger 命名

将 `ReplicationCoordinator.java` 中的 `LOGGER` 改为 `logger`，与其他文件保持一致。

---

## 3. 保留的最小闭环定义

本轮迭代后，系统应保留以下核心能力：

1. **数据存储**：MySQL 后端的 KV 存储引擎
2. **SQL 解析与执行**：手写解析器 + 查询执行器
3. **分布式协调**：ZooKeeper 选举与注册
4. **Region 管理**：Region 分配、分裂、合并
5. **复制与故障转移**：主从复制 + 自动故障转移
6. **热点检测和负载均衡**：简化后的定时检测 + 直接调用
7. **客户端访问**：JDBC 驱动 + 路由
8. **统一日志记录**：全部使用 SLF4J

---

## 4. 将被修改的文件清单

| 文件路径 | 修改内容 |
|----------|----------|
| `minisql-replication/.../GrpcReplicationTransportClient.java` | 替换 System.err.println 为 logger.warn/error |
| `minisql-replication/.../PrimaryChangeNotifier.java` | 替换 System.err.println 为 logger.warn/error |
| `minisql-replication/.../ReplicationCoordinator.java` | 替换 System.err.println 为 logger.warn/error，LOGGER 改为 logger |
| `minisql-replication/.../ReplicationWAL.java` | 替换 System.out/err.println 为 logger |
| `minisql-master/.../rpc/MasterServiceImpl.java` | 替换 System.out.println 为 logger.info |
| `minisql-master/.../rebalance/RegionMergeCoordinator.java` | 替换 System.out/err.println 为 logger |
| `minisql-master/.../rebalance/RegionSplitCoordinator.java` | 替换 System.out.println 为 logger.info |
| `minisql-master/.../rebalance/RegionMigrationCoordinator.java` | 替换 System.out/err.println 为 logger |
| `minisql-master/.../rebalance/MigrationCoordinator.java` | 替换 System.out/err.println 为 logger |
| `minisql-master/.../state/MetadataManager.java` | 替换 System.out/err.println 为 logger |
| `minisql-master/.../state/ClusterManager.java` | 替换 System.out/err.println 为 logger |
| `minisql-master/.../state/ReplicaMonitor.java` | 替换 System.out/err.println 为 logger |
| `minisql-master/.../state/ReplicaLifecycleManager.java` | 替换 System.out/err.println 为 logger |
| `minisql-sql/.../execution/QueryExecutor.java` | 替换 System.out.println 为 logger.info |

---

## 5. 不修改的文件

| 文件路径 | 原因 |
|----------|------|
| `minisql-client/.../cli/SqlCli.java` | CLI 工具，需要输出到控制台 |
| `minisql-client/.../cli/CliResultFormatter.java` | CLI 工具，需要输出到控制台 |

---

## 6. 执行顺序

1. 为没有 logger 的文件添加 SLF4J Logger
2. 替换所有 System.out.println 为 logger.info
3. 替换所有 System.err.println 为 logger.warn 或 logger.error
4. 统一 ReplicationCoordinator 中的 LOGGER 命名为 logger
5. 验证编译和测试
6. 生成 diff 文件

---

## 7. 为什么这些是"第一性原理下可以删掉的"

### 7.1 日志记录的一致性

使用 SLF4J 的好处：
1. **可配置性**：日志级别可通过配置文件调整
2. **结构化**：支持参数化日志，避免字符串拼接
3. **统一输出**：所有日志输出到同一目标（文件、控制台等）
4. **性能**：SLF4J 的参数化日志在禁用时不会计算字符串

### 7.2 命名一致性

统一使用小写 `logger`：
- 符合 Java 社区惯例
- 与项目中其他文件保持一致
- 减少代码审查时的困惑

---

## 8. 降低错误面

本轮迭代如何降低错误面：

1. **统一日志记录**：所有日志通过 SLF4J 输出，便于问题排查
2. **消除控制台输出**：生产环境中 System.out/err 不会被日志框架捕获
3. **参数化日志**：避免字符串拼接带来的性能问题
4. **命名一致性**：减少代码风格差异带来的认知负担
