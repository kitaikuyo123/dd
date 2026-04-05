# MiniSQL 第 2 轮迭代设计文档

## 1. 当前系统存在的问题

### 1.1 死代码 / 未使用代码

| 文件 | 问题 |
|------|------|
| `minisql-regionserver/.../RegionMergeService.java` | `estimateRegionSize()` 方法标记为 `@Deprecated` 且从未被调用 |
| `minisql-client/.../Router.java` | `roundRobinSelect()` 方法定义但从未被调用 |
| `minisql-client/.../MiniSQLConnection.java` | `getZkClient()` 方法存在但从未被调用 |

### 1.2 重复代码

| 问题 | 涉及文件 |
|------|----------|
| `bytesToHex()` 方法重复 6 次 | `RegionSplitService.java`, `RegionServerServiceImpl.java`, `RegionMergeService.java`, `KeyValueConverter.java`, `RowAssembler.java`, `RegionSplitCoordinator.java` |
| `JoinType` 定义重复 | `QueryPlan.java` 和 `ParallelQueryExecutor.java` 各自定义了 `JoinType` |

### 1.3 冗余抽象

| 问题 | 涉及文件 |
|------|----------|
| `StorageScanRequest` 继承 `StorageScanFilter` 但未添加任何新字段或方法 | `minisql-storage/.../StorageScanRequest.java` |

### 1.4 硬编码配置

| 问题 | 涉及位置 |
|------|----------|
| 硬编码默认地址 | `Router.java` 中多处 `localhost:16020`, `localhost:16000` |
| 硬编码 ZK 地址 | `SqlCli.java` 中 `zkHost = "localhost"` |

---

## 2. 本轮删减策略

### 2.1 删除死代码（优先级最高）

1. **删除 `RegionMergeService.estimateRegionSize()` 方法**
   - 理由：标记为 `@Deprecated` 且从未被调用

2. **删除 `Router.roundRobinSelect()` 方法**
   - 理由：定义但从未被调用，注释掉的代码也引用了它

3. **删除 `MiniSQLConnection.getZkClient()` 方法**
   - 理由：从未被调用，暴露内部实现细节

### 2.2 合并重复代码

1. **创建 `BytesUtil` 工具类**
   - 在 `minisql-common` 中创建 `BytesUtil.java`
   - 将 `bytesToHex()` 方法统一到此类中
   - 更新所有 6 个文件使用新的工具类

2. **合并 `JoinType` 定义**
   - 保留 `QueryPlan.JoinType` 作为权威定义
   - 删除 `ParallelQueryExecutor` 中的重复定义
   - 更新引用

### 2.3 简化冗余抽象

1. **合并 `StorageScanRequest` 和 `StorageScanFilter`**
   - 删除 `StorageScanRequest.java`
   - 将其功能合并到 `StorageScanFilter.java`
   - 更新所有引用

### 2.4 配置收敛（本轮暂不执行）

- 硬编码默认值留待后续迭代处理

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
| `minisql-storage/.../StorageScanRequest.java` | 与 `StorageScanFilter` 重复，合并到后者 |

### 4.2 新增文件

| 文件路径 | 内容 |
|----------|------|
| `minisql-common/.../util/BytesUtil.java` | 统一的字节工具类 |

### 4.3 修改文件

| 文件路径 | 修改内容 |
|----------|----------|
| `minisql-regionserver/.../RegionMergeService.java` | 删除 `estimateRegionSize()` 方法 |
| `minisql-client/.../Router.java` | 删除 `roundRobinSelect()` 方法 |
| `minisql-client/.../MiniSQLConnection.java` | 删除 `getZkClient()` 方法 |
| `minisql-regionserver/.../RegionSplitService.java` | 使用 `BytesUtil.bytesToHex()` |
| `minisql-regionserver/.../RegionServerServiceImpl.java` | 使用 `BytesUtil.bytesToHex()` |
| `minisql-regionserver/.../RegionMergeService.java` | 使用 `BytesUtil.bytesToHex()` |
| `minisql-common/.../KeyValueConverter.java` | 使用 `BytesUtil.bytesToHex()` |
| `minisql-common/.../RowAssembler.java` | 使用 `BytesUtil.bytesToHex()` |
| `minisql-master/.../RegionSplitCoordinator.java` | 使用 `BytesUtil.bytesToHex()` |
| `minisql-client/.../ParallelQueryExecutor.java` | 使用 `QueryPlan.JoinType` |
| `minisql-storage/.../StorageScanFilter.java` | 合并 `StorageScanRequest` 的功能 |

---

## 5. 修改后的目标结构

```
minisql-common/
  └── util/
      └── BytesUtil.java      (新增：统一字节工具类)

minisql-storage/
  └── StorageScanFilter.java  (合并 StorageScanRequest)
  └── StorageScanRequest.java (删除)

minisql-regionserver/
  └── RegionMergeService.java (删除废弃方法)
  └── RegionSplitService.java (使用 BytesUtil)
  └── RegionServerServiceImpl.java (使用 BytesUtil)

minisql-client/
  └── Router.java (删除未使用方法)
  └── MiniSQLConnection.java (删除未使用方法)
  └── ParallelQueryExecutor.java (使用 QueryPlan.JoinType)
```

---

## 6. 执行顺序

1. 创建 `BytesUtil.java` 工具类
2. 更新 6 个文件使用 `BytesUtil.bytesToHex()`
3. 删除 `RegionMergeService.estimateRegionSize()` 方法
4. 删除 `Router.roundRobinSelect()` 方法
5. 删除 `MiniSQLConnection.getZkClient()` 方法
6. 合并 `StorageScanRequest` 到 `StorageScanFilter`
7. 合并 `JoinType` 定义
8. 生成 diff 文件
