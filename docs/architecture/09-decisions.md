# 9. 架构决策

## ADR-001：使用 composition root 管理内核依赖

**决策**：由 `DatabaseManager` 创建并组装 transaction、record、MVCC 与 table 组件。

**原因**：项目不是依赖注入框架驱动的应用；集中组装使生命周期与关闭顺序可见，避免任意对象创建存储依赖。

## ADR-002：结果模型与传输模型分离

**决策**：SQL 层返回 `ExecutionResult` / `StatementResult` / `ResultSet`；server 通过 `ExecutionResultCodec` 编码，再由 `Packet` 传输。

**原因**：结果语义、JSON 表示和 socket framing 是三个独立变化轴。

## ADR-003：WAL record 使用 physical image

**决策**：Insert 日志存完整 Record bytes；Update 日志存 before image 与 after image。

**原因**：实现直接的 REDO / UNDO，适合当前单机教学型 recovery 模型；代价是日志体积较大。

## ADR-004：先实现最小 page LSN recovery 闭环

**决策**：在引入更复杂的 ARIES 概念前，先将 page LSN 写入 DataPage header，并以 `page LSN >= logEndLsn` 实现幂等 REDO。

**原因**：当前已有 WAL flush 与 dirty-page tracker，持久化 page LSN 是最小且必要的正确性补齐。

## ADR-005：事务链、CLR 与 fuzzy checkpoint 采用 ARIES 核心语义

**决策**：事务日志用 `prevLsn` 链接，Recovery 执行
Analysis → repeat-history Redo → CLR-based Undo；checkpoint 分块保存 DPT/ATT，
header `checkpointLsn` 只指向完整的 `BEGIN_CHECKPOINT`。

**原因**：这使 Undo 可重启、Redo 可由 page LSN 幂等跳过，并允许 checkpoint
期间事务和 PageCleaner 继续运行。
