# MiniSQL 架构文档

本目录采用精简版 [arc42](https://arc42.org/overview) 组织，只描述当前代码的事实。尚未实现的目标设计维护在 `docs/design/`，当前正在实施的具体变更通过 `openspec/changes/` 跟踪。

| 文档 | 内容 |
|---|---|
| [01-goals.md](01-goals.md) | 目标、范围与质量属性 |
| [03-context.md](03-context.md) | 外部使用者、TCP / HTTP 边界 |
| [05-building-blocks.md](05-building-blocks.md) | 包与核心组件的静态分层 |
| [06-runtime.md](06-runtime.md) | SQL 执行与 WAL 刷页的运行时链路 |
| [08-cross-cutting-concepts.md](08-cross-cutting-concepts.md) | transaction、MVCC、cache、WAL 与 transport |
| [09-decisions.md](09-decisions.md) | 当前关键架构决策 |
| [10-quality-requirements.md](10-quality-requirements.md) | 可验证的质量场景 |
| [11-risks-and-debt.md](11-risks-and-debt.md) | 已知风险与技术债 |
| [glossary.md](glossary.md) | 项目术语表 |

`../../architecture/minisql.dsl` 是 C4 模型源文件。标识符使用英文，图中的名称与说明使用中文。

WAL / Recovery 的已实现 M1–M6 设计与暂缓的 M7/M8 计划统一维护在
[WAL 与 Recovery 设计及实施计划](../design/wal-recovery.md)，具体持久化布局见
[WAL Record 格式](../design/wal-record-format.md)。

## 阅读顺序

首次阅读建议按 `03 → 05 → 06 → 08 → 11` 进行；修改存储与恢复逻辑前，先阅读 [OpenSpec 的 page LSN 变更设计](../../openspec/changes/persist-page-lsn/design.md)。
