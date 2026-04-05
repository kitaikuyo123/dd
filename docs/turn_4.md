# MiniSQL 第 4 轮迭代设计文档

## 1. 当前系统存在的问题

### 1.1 死代码 / 未使用代码

| 文件 | 问题 |
|------|------|
| `Constants.java` | `ZK_MASTER_PATH` 是兼容性别名，可能已不再使用 |
| `ClusterDetector.java` | `setEventSink()` 默认方法从未被重写 |
| `ReplicaInfo.java` | `getLastPromotionTime()` 和 `setLastPromotionTime()` 可能未被使用 |

### 1.2 未使用的导入

| 文件 | 问题 |
|------|------|
| `ReplicationCoordinator.java` | `import java.util.logging.Level;` 只使用一次 |
| `ParallelQueryExecutor.java` | `import java.util.logging.Logger;` 应该用 SLF4J |

### 1.3 魔法数字

| 文件 | 问题 |
|------|------|
| `RowKeySerializer.java` | 多处重复使用 `0x80000000`, `0x8000000000000000L`, `0xFFFFFFFF`, `0xFFFFFFFFFFFFFFFFL` |
| `RegionSplitService.java` | `0x00`, `(byte) 0xFF` 等边界标记 |

### 1.4 System.out.println 使用

| 文件 | 问题 |
|------|------|
| `MasterConnectionManager.java` | 使用 `System.out.println` 而非 proper logging |
| `ReplicationCoordinator.java` | 使用 `System.out.println` 而非 proper logging |

### 1.5 不必要的字段初始化

| 文件 | 问题 |
|------|------|
| `ResultSetMerger.java` | 字段显式初始化为 null |

---

## 2. 本轮删减策略

### 2.1 删除死代码（优先级最高）

1. **删除 `ClusterDetector.setEventSink()` 默认方法**
   - 理由：从未被重写，是无用的默认实现

2. **删除 `Constants.ZK_MASTER_PATH` 兼容性别名**
   - 理由：搜索后确认无使用，是过时的兼容代码

### 2.2 清理未使用的导入

1. **删除 `ReplicationCoordinator.java` 中未使用的导入**
2. **删除 `ParallelQueryExecutor.java` 中未使用的导入**

### 2.3 提取魔法数字为常量（保留主流程）

1. **在 `RowKeySerializer.java` 中提取位掩码常量**
2. **在 `RegionSplitService.java` 中提取边界标记常量**

### 2.4 替换 System.out.println 为日志（保留主流程）

1. **替换 `MasterConnectionManager.java` 中的 `System.out.println`**
2. **替换 `ReplicationCoordinator.java` 中的 `System.out.println`**

### 2.5 移除不必要的字段初始化

1. **删除 `ResultSetMerger.java` 中的显式 null 初始化**

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

## 4. 将被修改的文件清单

| 文件路径 | 修改内容 |
|----------|----------|
| `minisql-master/.../detect/ClusterDetector.java` | 删除未使用的 `setEventSink()` 默认方法 |
| `minisql-common/.../Constants.java` | 删除 `ZK_MASTER_PATH` 兼容性别名 |
| `minisql-replication/.../ReplicationCoordinator.java` | 删除未使用的导入，替换 System.out.println |
| `minisql-client/.../ParallelQueryExecutor.java` | 删除未使用的导入 |
| `minisql-client/.../MasterConnectionManager.java` | 替换 System.out.println 为日志 |
| `minisql-common/.../utils/RowKeySerializer.java` | 提取魔法数字为常量 |
| `minisql-regionserver/.../RegionSplitService.java` | 提取魔法数字为常量 |
| `minisql-client/.../ResultSetMerger.java` | 删除不必要的 null 初始化 |

---

## 5. 修改后的目标结构

```
minisql-common/
  └── utils/
      └── RowKeySerializer.java   (添加常量)

minisql-master/
  └── detect/
      └── ClusterDetector.java   (删除未使用方法)

minisql-client/
  └── MasterConnectionManager.java (使用 SLF4J 日志)
  └── ResultSetMerger.java       (简化初始化)

minisql-replication/
  └── ReplicationCoordinator.java (清理导入，使用 SLF4J)
```

---

## 6. 执行顺序

1. 搜索确认 `Constants.ZK_MASTER_PATH` 是否被使用
2. 删除 `ClusterDetector.setEventSink()` 默认方法
3. 删除 `Constants.ZK_MASTER_PATH` 常量
4. 清理未使用的导入
5. 替换 `System.out.println` 为 SLF4J 日志
6. 提取 `RowKeySerializer.java` 魔法数字为常量
7. 提取 `RegionSplitService.java` 魔法数字为常量
8. 删除 `ResultSetMerger.java` 不必要的 null 初始化
9. 生成 diff 文件