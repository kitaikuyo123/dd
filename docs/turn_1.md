# MiniSQL 第 1 轮迭代设计文档

## 1. 当前系统存在的问题

### 1.1 死代码 / 无效代码

| 文件 | 问题 |
|------|------|
| `minisql-master/.../recover/DataExporter.java` | SQL 引用 `kv_store` 表带 `region_id` 列，但实际表是 `kv_store_{regionId}` 且无此列，运行时必失败 |
| `minisql-master/.../recover/DataImporter.java` | 同上，SQL 与实际 schema 不匹配 |
| `minisql-master/.../recover/DataVerifier.java` | 同上，SQL 与实际 schema 不匹配 |
| `minisql-regionserver/.../RegionServerServiceImpl.java` | `executeUpdate` 方法永远抛出 `UnsupportedOperationException`，是死代码 |
| `minisql-master/.../LoadBalancer.java` | `LoadCalculator` 中 CPU weight 设为 0.0，相关代码无效 |

### 1.2 重复代码

| 问题 | 涉及文件 |
|------|----------|
| 两个 `Row` 类 | `minisql-common/.../model/Row.java` 和 `minisql-sql/.../execution/Row.java` 功能重复，API 略有不同 |

### 1.3 过度抽象

| 问题 | 涉及文件 |
|------|----------|
| 事件系统过于复杂 | `minisql-master/.../detect/` 下有 `ClusterEvent`, `ClusterEventHandler`, `ClusterEventSink`, `ClusterEventCoordinator` 等多个抽象层，对于当前用途过度设计 |
| Builder 模式滥用 | `KeyValue.java` 仅 6 个字段，使用 Builder 模式是不必要的复杂度 |

### 1.4 配置分散

| 问题 | 涉及位置 |
|------|----------|
| 硬编码常量 | `Constants.java`, `LoadBalancer.java`, `Router.java` 中大量硬编码值 |
| 配置文件过多 | `client.properties`, `master.properties`, `master2.properties`, `regionserver-1/2/3.properties` |

### 1.5 模块依赖问题

| 问题 | 涉及模块 |
|------|----------|
| 循环依赖 | `minisql-master` 依赖 `minisql-client`，这对于 Master 节点是不必要的 |

---

## 2. 本轮删减策略

### 2.1 删除死代码（优先级最高）

1. **删除整个 `recover` 包**：`DataExporter.java`, `DataImporter.java`, `DataVerifier.java`
   - 理由：SQL 与实际 schema 不匹配，代码无法运行

2. **删除 `RegionServerServiceImpl.executeUpdate` 方法**
   - 理由：永远抛异常，是死代码

3. **删除 `LoadCalculator` 中 CPU 相关代码**
   - 理由：weight = 0.0，计算结果从不被使用

### 2.2 Row 类分析（保留两者）

经分析，两个 `Row` 类设计目的不同：

| 类 | 存储结构 | 设计目的 |
|----|----------|----------|
| `common.model.Row` | Map<String, Object> | 通用数据模型，按列名访问 |
| `sql.execution.Row` | Object[] 数组 | 查询执行结果，按索引访问，性能优化 |

**决策**：保留两者，但添加注释说明用途区别，避免混淆。

### 2.3 简化过度抽象

1. **事件系统**（本轮暂不执行，留待后续）
   - 删除 `ClusterEvent`, `ClusterEventHandler`, `ClusterEventSink` 接口
   - 将 `ClusterEventCoordinator` 简化为直接回调模式
   - 保留具体事件类（`HotSpotActionEvent` 等）作为简单数据类

2. **`KeyValue` 的 Builder 模式**（本轮不执行）
   - 经分析，Builder 模式在生产代码 `KeyValueConverter.java` 中被使用
   - 保留 Builder 模式，避免破坏现有功能

### 2.4 配置收敛

1. **将硬编码常量移到配置文件**（本轮暂不执行，留待后续）

---

## 3. 保留的最小闭环定义

本轮迭代后，系统应保留以下核心能力：

1. **数据存储**：MySQL 后端的 KV 存储引擎
2. **SQL 解析与执行**：手写解析器 + 查询执行器
3. **分布式协调**：ZooKeeper 选举与注册
4. **Region 管理**：Region 分配、分裂、合并
5. **复制与故障转移**：主从复制 + 自动故障转移
6. **客户端访问**：JDBC 驱动 + 路由

---

## 4. 将被删除/合并的文件清单

### 4.1 删除文件

| 文件路径 | 删除原因 |
|----------|----------|
| `minisql-master/src/main/java/com/minisql/master/recover/DataExporter.java` | SQL 与 schema 不匹配，无法运行 |
| `minisql-master/src/main/java/com/minisql/master/recover/DataImporter.java` | SQL 与 schema 不匹配，无法运行 |
| `minisql-master/src/main/java/com/minisql/master/recover/DataVerifier.java` | SQL 与 schema 不匹配，无法运行 |

### 4.2 修改文件

| 文件路径 | 修改内容 |
|----------|----------|
| `minisql-regionserver/.../RegionServerServiceImpl.java` | 删除 `executeUpdate` 死代码方法 |
| `minisql-master/.../LoadBalancer.java` | 删除 CPU weight 相关无效代码 |

---

## 5. 修改后的目标结构

```
minisql-master/
  └── recover/               (整个目录删除)
  └── rebalance/
      └── LoadBalancer.java  (移除无效 CPU 代码)

minisql-regionserver/
  └── RegionServerServiceImpl.java  (移除 executeUpdate)
```

---

## 6. 执行顺序

1. 删除 `minisql-master/.../recover/` 整个包
2. 修改 `RegionServerServiceImpl.java`：删除 `executeUpdate` 方法
3. 修改 `LoadBalancer.java`：删除 CPU 相关代码
4. 生成 diff 文件
