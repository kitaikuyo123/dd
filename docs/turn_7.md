# MiniSQL 第 7 轮迭代设计文档

## 1. 当前系统存在的问题

### 1.1 重复的工具方法

| 问题 | 涉及文件 |
|------|----------|
| `DataVerifier.java` 中有私有的 `bytesToHex` 方法，与 `BytesUtil.bytesToHex` 重复 | `DataVerifier.java` |
| `RowKeySerializerTest.java` 中有私有的 `compareBytes` 方法，与 `BytesUtil.compareTo` 重复 | `RowKeySerializerTest.java` |

### 1.2 未使用的接口方法

| 问题 | 涉及文件 |
|------|----------|
| `RegionServerCommandClient` 接口有多个方法，但部分方法可能未被使用 | `RegionServerCommandClient.java` |
| `ReplicationTransportClient` 接口有多个方法，需要验证使用情况 | `ReplicationTransportClient.java` |

### 1.3 测试文件中的重复代码

| 问题 | 涉及文件 |
|------|----------|
| 测试文件中有重复的 mock 实现 | 多个测试文件 |

---

## 2. 本轮删减策略

### 2.1 统一工具方法调用（优先级最高）

**目标**：消除重复的工具方法，统一使用 `BytesUtil` 中的公共方法。

**具体操作**：
1. 将 `DataVerifier.java` 中的 `bytesToHex` 替换为 `BytesUtil.bytesToHex`
2. 将 `RowKeySerializerTest.java` 中的 `compareBytes` 替换为 `BytesUtil.compareTo`

### 2.2 简化测试代码

**目标**：减少测试文件中的重复 mock 实现。

**具体操作**：
1. 检查测试文件中是否有重复的 mock 代码
2. 如果有，考虑提取公共的 mock 工具类

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
9. **统一工具方法**：消除重复的工具方法

---

## 4. 将被修改的文件清单

| 文件路径 | 修改内容 |
|----------|----------|
| `minisql-master/.../recover/DataVerifier.java` | 删除私有的 `bytesToHex` 方法，使用 `BytesUtil.bytesToHex` |
| `minisql-common/.../test/RowKeySerializerTest.java` | 删除私有的 `compareBytes` 方法，使用 `BytesUtil.compareTo` |

---

## 5. 执行顺序

1. 修改 `DataVerifier.java`，删除 `bytesToHex` 方法并替换调用
2. 修改 `RowKeySerializerTest.java`，删除 `compareBytes` 方法并替换调用
3. 验证编译和测试
4. 生成 diff 文件

---

## 6. 为什么这些是"第一性原理下可以删掉的"

### 6.1 重复的工具方法

`BytesUtil` 类已经提供了 `bytesToHex` 和 `compareTo` 方法，其他类中的私有实现是重复代码，应该统一使用公共方法。

### 6.2 代码维护性

统一使用公共工具方法可以：
- 减少代码重复
- 确保行为一致性
- 便于未来修改和维护

---

## 7. 降低错误面

本轮迭代如何降低错误面：

1. **消除重复代码**：减少维护成本和潜在的不一致性
2. **统一工具方法**：确保字节数组操作的行为一致
3. **简化测试代码**：减少测试中的重复实现
