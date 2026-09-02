# 1. 目标与约束

## 目标

MiniSQL 是一个基于 Java 的教学型关系数据库实现。它提供 SQL 执行、事务、MVCC、B+Tree 索引、页式存储、WAL / recovery，以及 TCP 和 HTTP 两种访问入口。

首要质量目标：

1. **正确性**：事务状态、MVCC visibility 与 WAL ordering 可被测试验证。
2. **可理解性**：按 SQL、table、transaction、storage 分层；术语与数据格式明确。
3. **可演进性**：协议层、执行结果层和存储层边界清楚，局部重构不穿透全部调用方。
4. **并发性**：不同 Page 的 cache load 与 I/O 可并发；同一 Page 的物理修改由 latch 协调。

## 约束

- Java 11、Maven 构建。
- 数据库持久化格式由 Page、WAL 与 transaction status 文件共同定义；变更格式必须有恢复测试。
- 使用 established database terms：WAL、LSN、XID、UID、PGNO、FSO、MVCC。
- 注释和架构叙述使用中文；首次出现的专业术语附英文或缩写。

## 非目标

- 不追求 MySQL / PostgreSQL 的完整 SQL、优化器或并发控制能力。
- 当前不承诺跨版本数据文件在线升级。
- Spring HTTP API 是适配层，不参与数据库内核依赖关系。
