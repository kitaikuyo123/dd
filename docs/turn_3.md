# MiniSQL 第 3 轮迭代设计文档

## 1. 当前系统存在的问题

### 1.1 重复代码

| 问题 | 涉及文件 |
|------|----------|
| 两个 `BytesUtil` 类 | `minisql-common/.../util/BytesUtil.java` 和 `minisql-common/.../utils/BytesUtil.java` |
| 重复的 `compareBytes` 方法 | `KeyValue.java`, `Region.java`, `Router.java`, `MiniSQLConnection.java`, `RegionMergeCoordinator.java` |
| 重复的 `isKeyInRange` 方法 | `Router.java`, `MiniSQLConnection.java` |

### 1.2 死代码

| 文件 | 问题 |
|------|------|
| `KeyValueConverter.java` | `createRowKeyFromPartitionKeys` 方法从未被调用，只抛异常 |
| `MiniSQLConnection.java` | `masterConnectionManager` 字段声明但从未使用 |
| `Router.java` | 注释掉的代码 `// return roundRobinSelect(regions);` |
| `MiniSQLConnection.java` | 注释掉的字段 `// private final String url;` |

### 1.3 包命名不一致

| 问题 | 涉及位置 |
|------|----------|
| `util` vs `utils` | `com.minisql.common.util` 和 `com.minisql.common.utils` 两个包 |

---

## 2. 本轮删减策略

### 2.1 合并两个 BytesUtil 类（优先级最高）

1. **保留** `com.minisql.common.utils.BytesUtil`（功能更完整）
2. **删除** `com.minisql.common.util.BytesUtil`
3. **添加** `bytesToHex()` 方法到保留的 BytesUtil
4. **更新** 所有引用

### 2.2 统一 compareBytes 方法

1. **确认** `BytesUtil.compareTo()` 已存在
2. **更新** 5 个文件使用 `BytesUtil.compareTo()`
3. **删除** 各文件中的私有 `compareBytes` 方法

### 2.3 统一 isKeyInRange 方法

1. **添加** `BytesUtil.isKeyInRange()` 方法
2. **更新** 2 个文件使用新方法
3. **删除** 各文件中的私有 `isKeyInRange` 方法

### 2.4 删除死代码

1. **删除** `KeyValueConverter.createRowKeyFromPartitionKeys()` 方法
2. **删除** `MiniSQLConnection.masterConnectionManager` 字段
3. **删除** 注释掉的代码

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
| `minisql-common/.../util/BytesUtil.java` | 与 `utils/BytesUtil.java` 重复，合并到后者 |

### 4.2 修改文件

| 文件路径 | 修改内容 |
|----------|----------|
| `minisql-common/.../utils/BytesUtil.java` | 添加 `bytesToHex()` 和 `isKeyInRange()` 方法 |
| `minisql-common/.../KeyValue.java` | 使用 `BytesUtil.compareTo()`，删除私有方法 |
| `minisql-common/.../Region.java` | 使用 `BytesUtil.compareTo()`，删除私有方法 |
| `minisql-client/.../Router.java` | 使用 `BytesUtil`，删除私有方法和注释代码 |
| `minisql-client/.../MiniSQLConnection.java` | 使用 `BytesUtil`，删除私有方法、字段和注释代码 |
| `minisql-master/.../RegionMergeCoordinator.java` | 使用 `BytesUtil.compareTo()`，删除私有方法 |
| `minisql-common/.../KeyValueConverter.java` | 删除死代码方法 |
| `minisql-regionserver/.../RegionSplitService.java` | 更新 import |
| `minisql-regionserver/.../RegionServerServiceImpl.java` | 更新 import |
| `minisql-master/.../RegionSplitCoordinator.java` | 更新 import |
| `minisql-common/.../KeyValueConverter.java` | 更新 import |
| `minisql-common/.../RowAssembler.java` | 更新 import |

---

## 5. 修改后的目标结构

```
minisql-common/
  └── utils/
      └── BytesUtil.java       (合并后的统一工具类)
  └── util/                    (删除整个目录)
  └── model/
      ├── KeyValue.java        (简化：使用 BytesUtil)
      └── Region.java          (简化：使用 BytesUtil)

minisql-client/
  └── Router.java              (简化：使用 BytesUtil)
  └── MiniSQLConnection.java   (简化：删除死代码)

minisql-master/
  └── RegionMergeCoordinator.java (简化：使用 BytesUtil)
```

---

## 6. 执行顺序

1. 读取 `utils/BytesUtil.java`，添加 `bytesToHex()` 和 `isKeyInRange()` 方法
2. 更新所有使用 `util.BytesUtil` 的文件改为使用 `utils.BytesUtil`
3. 删除 `util/BytesUtil.java` 文件
4. 更新 `KeyValue.java` 使用 `BytesUtil.compareTo()`
5. 更新 `Region.java` 使用 `BytesUtil.compareTo()`
6. 更新 `Router.java` 使用 `BytesUtil`
7. 更新 `MiniSQLConnection.java` 使用 `BytesUtil`
8. 更新 `RegionMergeCoordinator.java` 使用 `BytesUtil.compareTo()`
9. 删除 `KeyValueConverter.createRowKeyFromPartitionKeys()` 方法
10. 删除 `MiniSQLConnection.masterConnectionManager` 字段
11. 删除注释掉的代码
12. 生成 diff 文件
