# Vibecoding (AI辅助开发) 体会与小结

## 项目背景

**项目**: MiniSQL 分布式SQL数据库
**开发方式**: 主要使用 Claude Code 辅助开发
**提交统计**: 80+个正式提交，60个fix/refactor相关提交

---

## 一、AI开发的核心体会

### 1.1 AI做得好的地方

#### 快速原型验证
AI能快速生成大量代码，从概念到可运行原型的时间大幅缩短：

```
feat: 负载均衡策略全面升级 — 可配置权重/EWMA预测/多因子选择/迁移预算/热点感知
```

一次性实现了5项复杂功能，纯手工开发可能需要数天。

#### 大规模重构能力
AI能跨模块进行大规模重构，保持代码一致性：

```
refactor: 跨模块冗余合并 + 死代码清理，净删 2100 行
```

删除了9个模块中的冗余代码，手工很难保证不遗漏。

#### 测试与文档生成
```
test: 并发/容错/集成测试 + 压力测试框架    → 6个ConcurrencyTest + 3个ErrorPathTest
docs: 全面提升注释质量                      → 覆盖8个模块39个源文件
```

---

### 1.2 AI的痛点

#### 痛点1: 过度设计倾向（最大浪费）

**典型案例**：负载均衡策略的演进

```
第1步: AI生成了复杂的EWMA预测 + 多因子权重模型
    feat: 负载均衡策略全面升级 — 可配置权重/EWMA预测/多因子选择/迁移预算/热点感知

    （提交间隔仅1天）

第3步: 删除所有复杂代码，回归简单策略
    refactor: 清理负载均衡死代码 — 移除EWMA/权重/多因子选择模型
    → 负载均衡已退化为Region计数策略(regionCount×10)
```

**根本原因**：AI倾向于"给出最全面的解决方案"，缺乏对实际运维复杂度的感知。参数调优的困难在代码生成时无法预见。

**时间浪费**：2天开发 + 后续清理。

---

#### 痛点2: 批量产生系统性bug（P0级）

**典型案例**：ZooKeeper元数据一致性问题

```
fix: ZK 元数据一致性修复 — 7 项 ZK 写入遗漏/冗余/安全问题
```

一次性修复了7个问题：
1. `pruneFailedReplicaReferences`: 移除 primary 故障 early return，始终写 ZK
2. `MetadataManager`: 新增 registerRegionForTableWithNullablePrimary
3. `registerRegionForTable`: 移除 setPrimary/addReplica 副作用
4. `FailoverCoordinator`: 删除冗余 updateZooKeeper 二次写入
5. `RecoveryCoordinator`: ensurePrimaryRegionOpen 补 registerRegionForTable
6. `HotSpotCoordinator`: addReadReplica 移除 bootstrap 前预写 ZK
7. `deleteTable`: 跳过已停机 RS 避免 DROP TABLE 失败

**根本原因**：AI在生成跨模块代码时，每个模块看起来正确，但整体协调有问题。缺乏全局状态管理的意识。

**发现方式**：集成测试偶发失败，通过日志分析发现。单元测试全部通过，无法发现此类问题。

---

#### 痛点3: 并发竞态问题（P0级）

**典型案例**：时序竞态导致偶发故障

```
fix: RS 注册偶发 Stream already completed + 主副本 open 加重试消解时序竞态
```

**问题**：RegionServer在完成gRPC bind之前就注册到Master，Master立即调用openRegion，但服务还未就绪。

**AI生成的代码**：
```java
// 直接调用，没有重试
regionServerRegistry.register(serverId);
```

**修复后**：
```java
// 添加5次退避重试
for (int i = 0; i < 5; i++) {
    try {
        openRegion(regionId);
        return;
    } catch (StreamAlreadyCompletedException e) {
        Thread.sleep(100 * (i + 1));
    }
}
```

**根本原因**：AI生成的代码通常假设理想时序，难以预判分布式环境下的各种时序组合。

---

#### 痛点4: 架构反复重构（高时间成本）

**典型案例**：Router架构的反复修改

```
第1次: Router有Master fallback + ZK双数据源
第2次: refactor: Router 统一走 ZK + 全局 GrpcChannelFactory 复用
      → 删除 Master RPC fallback，ZK 作为唯一路由数据源
第3次: refactor: 统一 Region 放置数据源到 Region 模型
      → 删除 ClusterManager 中 regionAssignments/regionReplicas 双份 map
```

**根本原因**：AI缺乏长期架构规划能力，每次需求变更倾向于重写而非修改。

---

#### 痛点5: 死代码大量积累

**统计数据**：单次"净删2100行"，加上多次"清理死代码"、"删除冗余xxx"。

**典型来源**：AI生成了复杂的多因子计算，实际使用时发现不需要，但代码保留在仓库中。

**根本原因**：AI倾向于"预留扩展点"，不遵守YAGNI原则。

---

#### 痛点6: 测试覆盖质量问题

**AI生成的测试**：
```java
Thread.sleep(50);  // 不可靠的竞态测试
```

**后续修复**：
```
test: 消除 Thread.sleep 竞态 + 补 LeaderElectionService 测试
- RegionMergeCoordinator: 8处 → 0处 (新增 triggerCheckNow() 同步触发)
- FailoverCoordinator: 3处 → 0处
- ReplicationCoordinator: 3处 → 0处
```

**根本原因**：AI倾向于用Thread.sleep模拟时序，缺乏对同步/异步测试设计的理解。

---

#### 痛点7: 上下文压缩后正确性骤降（隐蔽性最强的痛点）

**现象**：当对话上下文窗口接近极限时，系统会自动压缩（compact）历史消息。压缩后AI丢失了之前的架构决策、约束条件和边界case讨论，后续生成的代码开始与既定设计矛盾或重复犯错。

**典型案例**：

```
对话前半段: 讨论确定了 Region 放置统一走 Region 模型，删除 ClusterManager 双份 map
    ↓ 上下文压缩
对话后半段: AI 又在 ClusterManager 里新增了 regionAssignments 字段
    → 与之前的决策直接矛盾
```

**根本原因**：
- Compact 是有损压缩，细节和"为什么"最容易被丢弃
- AI不记得自己做过的决策，会重新犯已修复的同类bug
- 越长的开发会话，compact 次数越多，正确性衰减越严重

**实际影响**：
- 开发到后期，几乎每次都要先重新"唤醒"AI的上下文记忆
- 之前修过的同类bug可能再次出现
- 架构决策被悄悄推翻，直到集成测试才发现

**应对策略**：
- 在 `CLAUDE.md` 或项目文档中持久化关键架构决策和约束
- 每次大型变更后，把"做了什么、为什么这样做"写进文档，而非依赖对话记忆
- 遇到 compact 后，主动向 AI 重述当前状态和约束条件

## 二、人类在Vibecoding中需要做好哪些事情

AI能写代码，但它不替你思考。下面是50天开发中总结出的方法论。

### 2.1 架构决策必须由人来做

**原则**：AI负责填充细节，人负责画边界。

**具体做法**：
- 在让AI写代码之前，先自己想清楚模块拆分、数据流向、接口契约
- 把决策结果写在 `CLAUDE.md` 或项目文档里，让AI遵守
- 每次架构变更后，更新文档而非依赖对话上下文

**反例**（本项目真实教训）：
```
没有提前规划数据源归属
    → ClusterManager 和 MetadataManager 各维护一份 placement map
    → 双数据源导致 5 个 bug，花 1 天修复
```

### 2.2 控制AI的输出粒度

**原则**：一次只让AI做一件事，做完审查，再做下一件。

**具体做法**：
- **拆任务**：把"实现负载均衡"拆成"实现Region计数评分" → "实现迁移调度" → "实现热点感知"，逐步递进
- **设边界**：明确告诉AI"只改这个文件"、"不要动其他模块"
- **看 diff 再接受**：不要盲目accept全部输出，逐段审查

**反例**：
```
一次性让AI实现5项负载均衡特性
    → 产出400行复杂代码
    → 参数无法调优
    → 第二天全删回退
```

### 2.3 建立持久的上下文锚点

**原则**：不依赖对话记忆，把关键信息持久化到文件中。

**具体做法**：
- **CLAUDE.md**：写明项目结构、编码规范、已知的架构约束
- **设计文档**：记录"为什么这样做"，而不只是"做了什么"
- **提交信息**：写得详细，作为未来的上下文恢复源
- **遇到 compact 后**：主动向AI重述当前状态和约束，而非假设它还记得

**本项目的实际做法**：
```
git commit -m "refactor: failover 参数配置化 + ZK 路径接入普通模式
- failover 参数从 MasterMain 硬编码改为 master.properties 配置
- ZK 故障检测路径从 triggerEmergencyFailover 改为 triggerFailover
- 使冷却退避和重试限制真正生效，防止 ZK 假阳性导致雪崩"
```
这样的提交信息在 compact 后可以作为上下文恢复的依据。

### 2.4 测试策略：信任但要验证

**原则**：AI生成的测试能覆盖happy path，但人要补充异常路径和跨模块场景。

**具体做法**：
- **AI生成单元测试后**：检查是否有Thread.sleep、是否只测了正常路径
- **人工补充集成测试**：跨模块的协调逻辑必须端到端验证
- **故障注入**：手动模拟网络断开、节点宕机、ZK超时等场景
- **偶发失败必追**：测试偶发失败不是侥幸通过，是竞态的信号

**本项目真实教训**：
```
单元测试全部通过
    → 集成测试偶发失败（被忽略）
    → 线上出现 Stream already completed
    → 根因：时序竞态，单元测试根本覆盖不到
```

### 2.5 定期清理技术债务

**原则**：AI开发的技术债务积累速度远超手工开发，必须有意识地定期清理。

**具体做法**：
- **每周一次死代码扫描**：搜索未被引用的类和方法
- **每次功能完成后review**：是否引入了不需要的抽象或预留扩展点
- **重构小步进行**：每次只清理一个方向（如"统一工具方法"或"删除未使用的类"）

**本项目的清理节奏**：
```
feat: xxx功能        → 生成代码
    ↓ 发现问题
fix: 修复xxx问题     → 修bug
    ↓ 稳定后
refactor: 清理死代码 → 删除冗余
```

---

## 三、总结

> **"AI生成代码，人工保证质量"**

AI辅助开发的效率提升是真实的，但75%的提交是fix/refactor — **质量保证的人工投入不可省略**。

如果不进行人工审核和重构，系统会快速累积技术债务，最终需要更大的成本来偿还。
