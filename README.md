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

## 最近一次架构改造摘要

本次改造重点是把 ZooKeeper 从“辅助元数据存储”升级成“统一协调面”，核心变化如下：

- 将 ZooKeeper 主路径统一为：
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
- E2E 脚本与双 Master 验证配置同步更新，新增第二份 Master 配置文件 [master2.properties](/d:/aLabs/dd/minisql-master/src/main/resources/master2.properties)

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
