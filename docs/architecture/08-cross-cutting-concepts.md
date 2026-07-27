# 8. 横切概念

## Transaction 与 MVCC

- `XidAllocator` 管理 `.xid` 文件、格式迁移和区间预留，并分配永不复用的正数 XID。
- `XidStatusTable` 维护 `UNUSED / IN_PROGRESS / COMMITTED / ABORTED` 状态。
- `ActiveTransactionTable` 属于 WAL，维护 transaction phase 与 `lastLsn`。
- `VersionManager` 负责 visibility、transaction begin / commit / abort 与 lock coordination。
- Entry 的 MVCC header 包含 `XMIN`、`XMAX`；其 payload 是 table 层编码的 row bytes。
- 独立的 `SELECT` / `SHOW` / `DESCRIBE` 使用负数进程内 read-only XID；它只参与
  READ COMMITTED 可见性判断，不写 XID 文件、Entry、ATT 或 WAL。

## 存储数据层级

```text
rowBytes
  └─ Entry bytes = [XMIN][XMAX][payload]
       └─ Record bytes = [valid flag][payload size][entry bytes]
            └─ DataPage bytes = [page header][record area]
```

`PageRecordManager` 管理 physical record 与 Page；`Table` 管理 row 和 field 的逻辑含义。不要将两者混为同一层的 Record。

## Cache、latch 与 I/O

- `AbstractCache` 负责按 key 的对象 cache、reference count、in-flight load coordination 与 eviction。
- `PageBufferPool` 管理 Page load、allocation、dirty-page tracker 与 background cleaner。
- Page 使用 read/write latch：读可并行；同页 physical write 串行，避免 FSO 与字节区域并发修改损坏。
- File I/O 的 access、allocation 与 `force` 有独立协调，避免普通 positioned read/write 被全局 force 串行化。

## WAL

- `startLsn`：一条 WAL record 的起始文件偏移。
- `endLsn`：完整 WAL record 的结束偏移，也是 page LSN 的目标值。
- `recoveryLsn`：页本轮变脏时的首条日志 `startLsn`。
- `flushedLsn`：已 durable 的 WAL 边界。
- `prevLsn` 与 ATT `lastLsn`：同一事务前一条/最新一条日志的 `startLsn`。
- `checkpointLsn`：最近完整 fuzzy checkpoint 的 `BEGIN_CHECKPOINT.startLsn`。

Recovery 执行 Analysis、repeat-history Redo 和基于 CLR 的 restartable Undo。
`DirtyPageTable` 与 `ActiveTransactionTable` 会写入可分块的 checkpoint records；
只有 `END_CHECKPOINT` durable 后才同步更新 WAL header 中的 `checkpointLsn`。

WAL invariant：数据页写入稳定存储前，对应 WAL 必须已 durable 至少到该页 page LSN。

## Transport 与结果

- `Transporter` 只在 socket 上发送和接收一行十六进制编码的 bytes。
- `PacketCodec` 处理 Packet 的 bytes 格式；`PacketChannel` 将 Packet 与 transport 组合。
- `ExecutionResultCodec` 处理执行结果的 JSON bytes；它不属于 transport 协议层。
