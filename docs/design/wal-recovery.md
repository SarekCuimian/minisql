# WAL 与 Recovery 目标设计及实施计划

本文档描述 MiniSQL 从当前 WAL recovery 实现演进至 ARIES-style recovery 的目标设计，并将其拆分为可独立实现、验证和归档的 OpenSpec changes。本文档中的未完成内容是目标与计划，不代表当前代码已经具备相应能力；当前事实以 [Architecture 文档](../architecture/README.md) 为准，正在实施的行为以 `openspec/changes/` 为准。

各类 WAL record 的字段宽度、字节布局和边界校验统一定义在
[WAL Record 目标格式](wal-record-format.md)。

## 1. 目标与边界

目标：在保留 `STEAL + NO-FORCE` 策略的前提下，逐步补齐 page-level REDO、transaction log chain、restartable Undo 与 fuzzy checkpoint。

非目标：一次性实现完整 ARIES、partial rollback、savepoint、online format migration 或尚未需要的 Page 回收。

## 2. LSN 约定

| 名称 | 语义 | 持久化位置 |
|---|---|---|
| `startLsn` | WAL record 的起始位置，供 reader 定位和解析 | WAL record 的物理位置 |
| `endLsn` | WAL record 的结束位置，代表完整 record 覆盖边界 | WAL record header 与 WriteAheadLogger 边界 |
| page LSN | 最后修改 Page 的 `endLsn` | Page header |
| recovery LSN | 首次使 Page 变脏的 `startLsn` | 运行时 DPT，M6 写入 checkpoint record |
| `flushedLsn` | 已 durable 的 WAL 结束边界 | WAL header |
| `lastLsn` | 某 transaction 最后一条日志的 `startLsn` | ATT |
| `prevLsn` | 同 transaction 前一条日志的 `startLsn` | transaction WAL record |
| `undoNextLsn` | CLR 指向下一条待撤销日志的 `startLsn` | CLR |
| `redoLsn` | DPT 中最小 recovery LSN，Redo pass 从这里开始 | Analysis 期间计算 |
| `checkpointLsn` | 最近一次完整 checkpoint 的 `BEGIN_CHECKPOINT.startLsn` | WAL master/header |

ARIES 论文将 master record 中指向最近完整 `begin_chkpt` 的字段称为 `Chkpt_LSN`，将 Analysis 输出的 Redo 起点称为 `RedoLSN`。因此目标实现采用 `checkpointLsn` 与 `redoLsn`，不另造 `masterCheckpointLsn`、`redoStartLsn`。

M6 之前代码中的 `checkpointLsn` 曾表示 recovery-safe boundary；M6 已将 header
字段升级为 ARIES 语义，当前保存最近完整 `BEGIN_CHECKPOINT.startLsn`。

### 2.1 startLsn 与 endLsn 使用矩阵

| 使用位置 | 使用的 LSN | 原因 |
|---|---|---|
| `LogRecord.startLsn` | `startLsn` | record 的物理起点，可直接 `seek` |
| `prevLsn` | 上一条 transaction record 的 `startLsn` | 直接定位并解析上一条日志 |
| ATT `lastLsn` | 最新 transaction record 的 `startLsn` | Undo 从完整 record 开头开始 |
| CLR `undoNextLsn` | 下一条待撤销 record 的 `startLsn` | Undo 可以直接跳转 |
| DPT `recoveryLsn` | 首次使 Page 变脏 record 的 `startLsn` | Redo 需要从该 record 本身开始 |
| `checkpointLsn` | `BEGIN_CHECKPOINT.startLsn` | Analysis 从 checkpoint record 开头开始 |
| `redoLsn` | `min(DPT.recoveryLsn)` | Redo 从最早可能缺失的 record 开始 |
| `LogRecord.endLsn` | `endLsn` | 完整 record 的结束边界 |
| page LSN | 最后修改 Page record 的 `endLsn` | 标记该 Page 已包含完整 record 的 effect |
| `currentLsn` / `writtenLsn` / `flushedLsn` | `endLsn` boundary | 表示已分配、已写入、已 durable 到哪个 byte |
| `flush(targetLsn)` | 目标 record 的 `endLsn` | 返回时保证完整 record durable |
| COMMIT force target | `COMMIT.endLsn` | 返回成功前保证完整 COMMIT record durable |
| PageCleaner WAL target | Page 的 page LSN | Page 落盘前保证对应完整 WAL durable |

### 2.2 为什么 page LSN 使用 endLsn

ARIES 将 page LSN 定义为 Page 最后一次修改对应的日志顺序标记，并不要求 LSN 必须是文件起始或结束偏移。MiniSQL 的 `flushedLsn` 是 WAL 文件的 durable byte boundary，因此 page LSN 使用 `endLsn` 可以直接建立：

```text
flushedLsn >= page LSN
```

该条件能够证明描述页面修改的完整 WAL record 已 durable；若 page LSN 使用 `startLsn`，还需要额外取得该 record 的结束位置才能完成同样证明。PostgreSQL `pd_lsn` 与 InnoDB `FIL_PAGE_LSN` 也采用记录结束位置语义。

### 2.3 UID 与 physical WAL address

UID 是项目运行时的 physical record identifier，本质上由 PGNO 与 Page 内 offset 组合。它适合用于 `PageRecordManager.acquire(uid)`、MVCC Entry 和索引引用。

WAL codec 直接保存：

```text
[PGNO:4][recordOffset:2]
```

而不是保存 `[UID:8]`。Recovery 本来就需要 PGNO 获取 Page、再以 offset 修改 Page bytes；显式字段不会依赖 `UidUtil` 的 packing 规则，也能复用于没有 UID 的 Page-level WAL。解析后如业务代码需要 UID，再由 `UidUtil` 组合。

### 2.4 Physical image 策略

`INSERT` 保存完整 record bytes；`UPDATE` 保存完整 `beforeImage` 与 `afterImage`；CLR 保存完整 `compensationImage`。当前不采用 byte diff，以较高 WAL 写入量换取简单、明确且容易验证的 REDO / Undo。

## 3. 持久化 page LSN

所有 Page 使用公共 header：

```text
[page LSN:8][Page-specific header][Page body]
```

DataPage：

```text
[page LSN:8][FSO:2][Record area]
```

写入与刷页关系：

```text
append WAL → startLsn, endLsn
      ↓
修改 Page bytes，并写 page LSN = endLsn
      ↓
DPT: PGNO → recoveryLsn = startLsn
      ↓
PageCleaner 确保 flushedLsn >= page LSN
      ↓
写 Page snapshot 并 force
```

REDO：

```text
page LSN >= record.endLsn  → skip
page LSN <  record.endLsn  → replay and advance page LSN
```

## 4. Transaction log chain 与 CLR

Transaction WAL 使用公共头：

```text
[type][XID][prevLsn][body]
```

`prevLsn` 与 ATT 的 `lastLsn` 都使用 `startLsn`。Undo 可以从 `lastLsn` 开始，沿 `prevLsn` 直接定位该 transaction 的历史，不需要扫描并分组全部 WAL。

Undo 每完成一步就先追加 CLR：

```text
[CLR][XID][prevLsn]
[undoNextLsn][PGNO][recordOffset][compensationImageLength][compensationImage]
```

CLR 可以 REDO，但绝不再次 UNDO。若系统在 Undo 中再次崩溃，下一次 Recovery 根据 CLR 的 `undoNextLsn` 跳过已经完成的撤销。

## 5. Fuzzy checkpoint

Checkpoint 不停止 transaction，也不要求强制写出所有 dirty page。它持久化恢复所需的 ATT 与 DPT snapshot：

```text
BEGIN_CHECKPOINT
CHECKPOINT_DPT...
CHECKPOINT_ATT...
END_CHECKPOINT
```

只有 `END_CHECKPOINT` durable 后，才允许：

```text
checkpointLsn = beginCheckpointLsn
```

`beginCheckpointLsn` 是 checkpoint 执行期间保存新 `BEGIN_CHECKPOINT.startLsn` 的局部变量；`checkpointLsn` 是 WAL master/header 中已经确认完整的 checkpoint 起点。二者在 checkpoint 完成后数值相同，但 durable 时机不同。

## 6. 最终 Recovery 流程

```text
Analysis
  从 checkpointLsn 开始
  重建 DPT 与 ATT
  识别 winner / loser

Redo
  从 min(DPT.recoveryLsn) 开始
  repeat history
  根据 page LSN 跳过已落盘修改

Undo
  从 loser.lastLsn 开始
  沿 prevLsn 逆序撤销
  先写 CLR，再修改 Page
  遇到 CLR 时沿 undoNextLsn 继续
```

## 7. 当前基线

当前已经具备：

- WAL 顺序 append、record CRC、坏尾检测与裁剪。
- ring buffer、LogWriter、LogFlusher 与并发 flush target 合并。
- typed WAL payload 与稳定 type code。
- BEGIN / COMMIT / ABORT / END transaction log chain。
- PageCleaner、可快照 DirtyPageTable 与 WAL-before-page flush。
- Analysis、repeat-history Redo、CLR 与 restartable Undo。
- 分块持久化 DPT / ATT 的 fuzzy checkpoint。
- Page header 持久化 page LSN，REDO 使用 `LogRecord.endLsn` 幂等跳过。
- database Page format version 与旧格式 fail-fast 检查。

当前缺口：

- M7 Page allocation/free WAL 暂不实施。
- M8 WAL segment 与 retention 暂不实施。
- savepoint 暂未实现。

## 8. 实施原则

1. 每个阶段对应一个 OpenSpec change，完成验收后再开始依赖它的下一阶段。
2. Page format、WAL format 和 Recovery algorithm 不在同一个 change 中同时大改。
3. 每次持久化格式变化都必须升级 format version；本项目直接切换新格式，不提供旧文件迁移，打开旧文件时必须 fail fast。
4. 所有 WAL type 使用稳定 byte code，不持久化 Java `enum.ordinal()`。
5. `startLsn` 用于 record 定位和 transaction log chain；`endLsn` 用于 durable boundary 与 page LSN。
6. 每个阶段必须同时提交 focused tests、crash-point tests 和文档更新。

## 9. 路线总览

| 阶段 | OpenSpec change | 主要产出 | 依赖 |
|---|---|---|---|
| M1 | `persist-page-lsn` | 持久化 page LSN、幂等 REDO | 当前基线 |
| M2 | `introduce-wal-record-format` | typed payload codec、显式 type 分发、严格格式校验 | M1 |
| M3 | `add-transaction-log-chain` | BEGIN / COMMIT / ABORT / END、prevLsn、ATT | M2 |
| M4 | `add-analysis-redo-recovery` | Analysis、DPT 重建、repeat-history REDO | M3 |
| M5 | `add-restartable-undo` | CLR、undoNextLsn、可重启 Undo | M4 |
| M6 | `add-fuzzy-checkpoint` | checkpoint records、DPT / ATT snapshot、master pointer | M5 |
| M7 | `add-page-lifecycle-wal` | PAGE_ALLOCATE / PAGE_FREE | M6 |
| M8 | `add-wal-retention` | WAL segment 与安全截断 | M7 |

M1–M6 已完成实现与验收。M7 Page 生命周期与 M8 WAL retention 按当前范围暂停，
不创建对应实现任务。

## 10. M1：持久化 page LSN

### 代码范围

- 新增公共 `PageHeader`，在 Page 起始位置保存 8-byte page LSN。
- DataPage 的 FSO 由 offset 0 后移至 offset 8，record area 起点相应后移。
- `CachedPage` 从 Page bytes 读写 page LSN，移除独立易失副本。
- `WriteAheadLogger.Reader` 向 Recovery 暴露当前 record 的 `endLsn`。
- REDO 根据 page LSN 与 record `endLsn` 判断是否跳过。
- 增加 database page-format version；直接切换新格式，旧开发数据库删除后重建，打开旧文件必须 fail fast。

### 验收门禁

- 重启后能从 Page bytes 读取原 page LSN。
- WAL durable、Page 未落盘时能够 REDO。
- Page 已包含对应修改时跳过 REDO，Page bytes 保持不变。
- PageCleaner 写页前满足 `flushedLsn >= page LSN`。
- DataPage free-space 与边界计算覆盖新的公共 header。
- storage、transaction 与完整 Maven tests 通过。

## 11. M2：统一 WAL record format

### 目标结构

```text
engine.storage.wal
├── WriteAheadLogger
├── LogRecord
├── LogRecordType
├── LogRecordCodec
└── Recovery
```

目标 API：

```java
LogRecord append(byte[] payload);

LogRecord next();
```

`LogRecord` 是完整物理记录，直接包含 `startLsn`、`endLsn` 与 `payload`。调用者向
`append` 提交尚未分配位置的 payload；`WriteAheadLogger` 分配连续区间、完成 framing
并返回已定位的 `LogRecord`。不再增加 `LogEntry`、`LocatedLogRecord` 或读取结果
包装类。

### 代码范围

- 保留现有 WAL 外层 framing，字段统一为 `payloadLength`、`recordCrc`、`endLsn`。
- 将 `INSERT`、`UPDATE` payload 的 encode/decode 从 `Recovery` 移到 `LogRecordCodec`。
- 使用显式 `switch (record.getType())`；未知 type 和长度不合法必须报 malformed WAL。
- CRC 覆盖 `payloadLength`、`endLsn` 与 payload。
- 升级 WAL format version。

### 验收门禁

- 每种 record codec round-trip。
- unknown type、负长度、截断 payload、CRC 错误均被拒绝。
- 并发 append 返回的 `[startLsn, endLsn)` 互不重叠且连续。
- ring buffer wrap-around 后 record 边界保持正确。
- Recovery 行为与 M1 保持一致。

## 12. M3：Transaction log chain

状态：已完成。

### 代码范围

- 增加 `BEGIN`、`COMMIT`、`ABORT`、`END`。
- transaction record 公共头采用 `[type][XID][prevLsn]`。
- 新增顶层值对象 `ActiveTransaction`，保存 `XID`、status 与 `lastLsn`；
  `ActiveTransactionTable` 只负责并发维护与快照。
- `prevLsn`、`lastLsn` 统一保存 record `startLsn`。
- `ActiveTransactionTable` 独立维护 `lastLsn`，不再由 XID 状态组件承担。

### 写入顺序

```text
BEGIN:
allocate XID → append BEGIN → ATT.add → flush BEGIN.endLsn

COMMIT:
append COMMIT → flush COMMIT.endLsn
→ persist committed status → append END → ATT.remove → response

ABORT:
append ABORT → ATT.status = ABORTING → start Undo

END:
transaction cleanup complete → append END → ATT.remove
```

### 验收门禁

- 事务日志能从 `lastLsn` 沿 `prevLsn` 追溯到 `NO_LSN`。
- 多 transaction 交错 append 不破坏各自日志链。
- COMMIT 未 durable 时不得返回成功。
- COMMIT durable、XID status 未更新时，Recovery 能修复状态。
- END 之后 transaction 不再出现在 ATT。

## 13. M4：Analysis 与 repeat-history REDO

状态：已完成。

### 代码范围

- 将 Recovery 明确拆分为 `analyze`、`redo`、`undo` 三个阶段。
- Analysis 扫描 WAL，重建 ATT 与 DPT，识别 winner / loser。
- `redoLsn = min(DPT.recoveryLsn)`。
- REDO 重放所有需要重放的历史，不再只重放 non-active transaction。

### REDO 判定

```text
目标 Page 不在 DPT                         → skip
record.startLsn < DPT[PGNO].recoveryLsn   → skip
page LSN >= record.endLsn                 → skip
其他情况                                  → redo
```

### 验收门禁

- committed、aborted 与 active transaction 交错时，Analysis 分类正确。
- REDO 相同日志多次不会改变最终结果。
- Page 已落盘、部分落盘和完全未落盘三种状态均能恢复。
- 不再存在“非 INSERT 一律按 UPDATE 解析”的分支。

## 14. M5：CLR 与 restartable Undo

状态：已完成。

### 代码范围

- 增加 CLR：`[CLR][XID][prevLsn][undoNextLsn][PGNO][recordOffset][compensationImage]`。
- Undo 修改 Page 前先 append CLR。
- CLR 可以 REDO，但绝不再次 UNDO。
- 多 loser transaction 使用按 LSN 降序的 priority queue。
- Undo 完成后写 END 并持久化 aborted status。

### 验收门禁

- Undo 一条 `UPDATE` 和一条 `INSERT` 都会生成对应 CLR。
- CLR durable、compensation Page 未落盘时再次崩溃，下一次 Recovery 能 REDO CLR。
- compensation Page 已落盘、END 未写时再次崩溃，不会重复撤销原日志。
- CLR 的 `undoNextLsn` 能跳过已经完成的 Undo history。

## 15. M6：Fuzzy checkpoint

状态：已完成。

### 代码范围

- 将 dirty-page tracker 提升为可快照的 `DirtyPageTable`。
- 新增 `CheckpointManager`。
- 增加 `BEGIN_CHECKPOINT`、`CHECKPOINT_DPT`、`CHECKPOINT_ATT`、`END_CHECKPOINT`。
- DPT / ATT snapshot 支持分块，避免超过单条 WAL record 容量。
- WAL header 持久化 `checkpointLsn`，指向最近完整的 `BEGIN_CHECKPOINT.startLsn`。

### 写入顺序

```text
append BEGIN_CHECKPOINT
→ snapshot DPT / ATT
→ append checkpoint chunks
→ append END_CHECKPOINT
→ flush END_CHECKPOINT.endLsn
→ update checkpointLsn
→ force WAL header
```

### 验收门禁

- checkpoint 期间 transaction 和 PageCleaner 可以继续运行。
- END_CHECKPOINT 未 durable 时继续使用上一个 checkpoint。
- END_CHECKPOINT durable、master header 未更新时仍能使用上一个 checkpoint。
- WAL header 中的新 `checkpointLsn` durable 后，Analysis 从新的 checkpoint 开始。
- chunk 数量、entry 数量和归属的 `beginCheckpointLsn` 均被校验。

## 16. M7 / M8：Page 生命周期与 WAL retention

M1–M6 已实现。M7/M8 按当前范围决定暂不实施。

M7 在真正支持 Page 回收与重用时增加 `PAGE_ALLOCATE`、`PAGE_FREE`；在此之前不加入核心 WAL type。

M8 将 WAL 改为 segment，并根据以下引用计算最早可删除位置：

```text
checkpointLsn
ATT.lastLsn
DPT.recoveryLsn
尚未完成 Undo 的 undoNextLsn
```

这两个阶段不属于当前恢复正确性的最小闭环。

## 17. Crash-test 矩阵

每个涉及持久化顺序的 change 至少覆盖以下 crash point：

| Crash point | 必须验证 |
|---|---|
| WAL append 前 | Page 不得包含无日志修改 |
| WAL append 后、WAL force 前 | 未 durable record 不得被当作已提交 |
| WAL force 后、Page write 前 | Recovery 能 REDO |
| Page write 中 | page / WAL 校验能够检测不完整写入 |
| Page force 后、COMMIT 前 | loser transaction 能 Undo |
| COMMIT durable 后、XID status 前 | Recovery 修复 committed status |
| CLR append 后、compensation 前 | Recovery REDO CLR |
| compensation 后、END 前 | Recovery 不重复撤销 |
| checkpoint chunks 写入中 | 使用上一个完整 checkpoint |
| END_CHECKPOINT durable 后、master 更新前 | 使用上一个 checkpoint 仍然安全 |

Crash tests 应以真实临时文件和进程重启为主，mock 只用于 codec 与状态机单元测试。

## 18. 完成定义

一个阶段只有同时满足以下条件才可归档：

- OpenSpec requirements、design、tasks 与实现一致。
- focused tests、crash tests 和完整 Maven tests 通过。
- `git diff --check` 通过。
- 持久化 format version 与兼容性策略已更新。
- 架构文档、运行时图和术语表已同步。
- 没有通过兼容分支长期保留旧 WAL / Page format。

## 19. 参考依据

- [ARIES 原始论文](https://pages.cs.wisc.edu/~yxy/cs764-f20/papers/aries.pdf)：master record 的 `Chkpt_LSN`、Analysis 输出的 `RedoLSN`、DPT `RecLSN`、page LSN 比较与 CLR。
- [PostgreSQL Database Page Layout](https://www.postgresql.org/docs/17/storage-page-layout.html)：`pd_lsn` 表示最后一次页面修改对应 WAL record 之后的 byte position。
- [InnoDB Page Header 定义](https://dev.mysql.com/doc/dev/mysql-server/latest/fil0types_8h_source.html)：`FIL_PAGE_LSN` 使用最后一次页面修改日志的结束 LSN。
