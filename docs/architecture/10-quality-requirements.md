# 10. 质量需求

第 1 章说明质量目标的优先级；本章将其转化为可验证的场景。这里的内容既是架构验收依据，也是后续 OpenSpec change 的测试来源。

## 10.1 正确性与可恢复性

| 场景 | 刺激 | 预期结果 | 验证方式 |
|---|---|---|---|
| WAL 先行 | WAL 已追加但尚未 durable 时触发数据页 flush | 数据页不得先于对应 WAL 持久化 | 在 PageBufferPool / WriteAheadLogger test 中记录调用顺序 |
| 已落盘页的 REDO | 重启时日志 `endLsn <= page LSN` | Recovery 跳过该日志，页内容不变 | `persist-page-lsn` recovery test |
| 未落盘页的 REDO | 重启时日志 `endLsn > page LSN` | Recovery 重放 image 并推进 page LSN | `persist-page-lsn` recovery test |
| 活跃事务崩溃 | transaction 未 commit 即进程终止 | Recovery 逆序 UNDO，并将 XID 设为 aborted | transaction + recovery integration test |

## 10.2 并发性

| 场景 | 刺激 | 预期结果 | 验证方式 |
|---|---|---|---|
| 不同页并发 load | 多线程请求不同 page number | 不因单个全局 file lock 而串行等待 I/O | PageBufferPool concurrency benchmark |
| 相同页并发读取 | 多线程读取相同 Page 的不同 Record | Page read latch 允许并行读取 | concurrency test |
| 相同页并发写入 | 多线程修改同一 Page | Page write latch 串行修改，FSO 与 record bytes 不损坏 | stress test + page integrity check |

## 10.3 可维护性

| 场景 | 预期结果 | 验证方式 |
|---|---|---|
| 修改 WAL format | 常量、layout 文档、recovery parser 与测试同步变更 | code review + OpenSpec spec review |
| 新增 SQL statement | AST、parser、Executor、result model 的边界保持明确 | module-level tests |
| 修改架构行为 | 先更新 OpenSpec proposal/spec，再实现代码 | `openspec validate --strict` |

## 10.4 可观测性

当前最低要求是：SQL execution log 包含 client identifier 与 SQL 文本；recovery 输出关键阶段；异常在 server connection 边界被转换为 error Packet。性能指标、structured tracing 与 metrics 暂不属于当前范围。
