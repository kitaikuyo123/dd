# MiniSQL 第 5 轮迭代设计文档

## 1. 当前系统存在的问题

### 1.1 过度事件化的检测器架构

| 问题 | 涉及文件 |
|------|----------|
| `ClusterEventCoordinator` 事件总线增加了不必要的复杂度 | `ClusterEventCoordinator.java` |
| `HotSpotActionEvent` 仅被 `HotSpotDetector` 发出，仅被 `MasterServiceImpl` 处理 | `HotSpotActionEvent.java` |
| `RegionSplitSuggestedEvent` 仅被 `RegionSplitDetector` 发出，仅被 `MasterServiceImpl` 处理 | `RegionSplitSuggestedEvent.java` |
| `ClusterEventHandler` 接口和 `ClusterEventHandler<?>` 泛型接口冗余 | `ClusterEventHandler.java`, `ClusterEventSink.java` |
| `HotSpotDetector` 只是 `HotSpotCoordinator` 的简单调度封装 | `HotSpotDetector.java` |
| `RegionSplitDetector` 只是 `RegionSplitCoordinator` 的简单调度封装 | `RegionSplitDetector.java` |

### 1.2 未被引用的事件类

| 类 | 使用情况 |
|---|----------|
| `HotSpotActionEvent` | 仅在 `HotSpotDetector` 和 `MasterServiceImpl` 中使用，两点间直接调用即可 |
| `RegionSplitSuggestedEvent` | 仅在 `RegionSplitDetector` 和 `MasterServiceImpl` 中使用，两点间直接调用即可 |

### 1.3 冗余的检测器模式

| 类 | 实际作用 |
|---|----------|
| `HotSpotDetector` | 定时调用 `HotSpotCoordinator.planPendingActions()` 并发布事件 |
| `RegionSplitDetector` | 定时调用 `RegionSplitCoordinator.shouldSplit()` 并发布事件 |

这些检测器实际上只是简单的定时调度器，不需要 `ClusterDetector` 接口的抽象。

### 1.4 关于 ReplicaInfo.lastPromotionTime

第4轮迭代文档中标记 `ReplicaInfo.getLastPromotionTime()` 和 `setLastPromotionTime()` 可能未被使用。
经过检查，这两个方法在 `ReplicaMonitor.java:323, 328` 中被实际使用，用于记录副本提升为主副本的时间。
**结论**：这两个方法应保留，不应删除。

---

## 2. 本轮删减策略

### 2.1 简化热点检测和 Region 检测的调用链（优先级最高）

**当前调用链**：
```
HotSpotDetector (定时) -> planPendingActions() -> 发布 HotSpotActionEvent
-> ClusterEventCoordinator -> MasterServiceImpl.handleHotSpotAction() -> 执行
```

**简化后调用链**：
```
MasterServiceImpl 定时器 -> hotSpotCoordinator.planPendingActions() -> 直接执行动作
```

**具体操作**：
1. 删除 `ClusterEventCoordinator` 事件总线
2. 删除 `HotSpotActionEvent`、`RegionSplitSuggestedEvent`、`ClusterEventHandler`、`ClusterEventSink`
3. 删除 `HotSpotDetector`、`RegionSplitDetector` 适配器
4. 在 `MasterServiceImpl` 中直接创建定时器调用 `HotSpotCoordinator` 和 `RegionSplitCoordinator`

### 2.2 更新 `ClusterDetector` 接口

删除后，`ClusterDetector` 只保留 `ServerFailureDetector` 这一个实现者。
由于接口只剩一个实现，可以考虑：
1. 将 `ServerFailureDetector` 的逻辑内化到 `MasterServiceImpl`
2. 或者保留接口但简化其定义（移除事件相关方法）

本次迭代选择方案 1：将 `ServerFailureDetector` 逻辑内化。

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

---

## 4. 将被删除的文件清单

| 文件路径 | 删除原因 |
|----------|----------|
| `minisql-master/.../detect/ClusterEventCoordinator.java` | 不必要的事件总线 |
| `minisql-master/.../detect/ClusterEvent.java` | 不必要的事件基类 |
| `minisql-master/.../detect/ClusterEventHandler.java` | 不必要的事件处理器接口 |
| `minisql-master/.../detect/ClusterEventSink.java` | 不必要的事件发布接口 |
| `minisql-master/.../detect/HotSpotActionEvent.java` | 直接调用即可 |
| `minisql-master/.../detect/RegionSplitSuggestedEvent.java` | 直接调用即可 |
| `minisql-master/.../detect/HotSpotDetector.java` | 简单调度器，直接内化 |
| `minisql-master/.../detect/RegionSplitDetector.java` | 简单调度器，直接内化 |
| `minisql-master/.../detect/ServerFailedEvent.java` | 直接调用即可 |
| `minisql-master/.../detect/ServerFailureDetector.java` | 逻辑内化到 MasterServiceImpl |
| `minisql-master/.../detect/ClusterDetector.java` | 只剩一个实现，移除接口 |

---

## 5. 将被修改的文件清单

| 文件路径 | 修改内容 |
|----------|----------|
| `minisql-master/.../rpc/MasterMain.java` | 移除 `ClusterEventCoordinator` 相关初始化代码 |
| `minisql-master/.../rpc/MasterServiceImpl.java` | 内化检测器逻辑，添加定时器直接调用 Coordinator |
| `minisql-common/.../model/ReplicaInfo.java` | **保留** `lastPromotionTime` 方法（被 ReplicaMonitor 使用） |

---

## 6. 修改后的目标结构

```
minisql-master/
  └── rpc/
      ├── MasterMain.java          (简化：移除事件总线初始化)
      └── MasterServiceImpl.java   (内化检测器逻辑，直接定时调用)
```

删除整个 `minisql-master/.../detect/` 目录（事件系统被移除）。

---

## 7. 执行顺序

1. 读取并确认 `ReplicaInfo.lastPromotionTime` 被实际使用，保留该字段和方法
2. 修改 `MasterServiceImpl`：
   - 添加热点检测定时器
   - 添加 Region 分裂检测定时器
   - 内化 ServerFailureDetector 逻辑
   - 移除对事件系统的依赖
3. 修改 `MasterMain`：
   - 移除 `ClusterEventCoordinator` 相关初始化
   - 简化组件启动流程
4. 删除整个 `detect/` 目录及其中的所有文件
5. 验证编译和测试
6. 生成 diff 文件

---

## 8. 为什么这些是"第一性原理下可以删掉的"

### 8.1 事件总线增加了不必要的间接层

当前的事件系统（`ClusterEventCoordinator` + 事件类 + 处理器接口）引入了：
- 5+ 个额外的文件
- 事件发布/订阅的运行时开销
- 需要维护事件类型和处理器类型的匹配

而实际上：
- 所有事件都是同步处理的（非异步）
- 事件只有单一消费者
- 生产者和消费者之间不存在动态变化

直接调用能：
- 减少代码量约 500 行
- 消除运行时类型检查
- 让调用链更清晰

### 8.2 检测器适配器没有价值

`HotSpotDetector` 和 `RegionSplitDetector` 只是简单的定时调度器：
```java
scheduler.scheduleAtFixedRate(() -> {
    coordinator.planPendingActions().forEach(action -> eventSink.publish(new XEvent(action)));
}, interval, interval, ms);
```

这种模式可以通过在 `MasterServiceImpl` 中直接创建定时器实现，不需要额外的类和接口抽象。

### 8.3 ServerFailureDetector 可以内化

`ServerFailureDetector` 的逻辑相对简单，核心是通过心跳检测服务器失败。将其内化到 `MasterServiceImpl` 可以：
- 减少跨模块调用
- 让故障处理逻辑更集中
- 减少需要维护的状态

---

## 9. 降低错误面

本轮迭代如何降低错误面：

1. **减少状态分叉**：移除事件总线后，热点检测和分裂检测的状态管理更加集中
2. **减少隐式行为**：不再需要维护事件类型字符串（`getEventType()`）和事件发布逻辑
3. **减少配置路径**：不再需要配置事件处理器的注册
4. **消除运行时类型检查**：直接调用替代了事件分发的类型匹配
5. **减少代码复杂度**：删除了约 500 行不必要的事件系统代码