# 6. 运行时视图

## SQL 执行

```mermaid
sequenceDiagram
    participant C as Client / API
    participant S as Server
    participant P as PacketChannel
    participant E as Executor
    participant T as TableManager
    participant V as VersionManager

    C->>S: SQL bytes
    S->>P: receive Packet
    P-->>S: payload
    S->>E: execute(sql)
    E->>E: Parser.parse → Statement
    E->>T: DDL / DML / transaction command
    T->>V: MVCC read / write
    V-->>T: StatementResult
    T-->>E: StatementResult
    E-->>S: ExecutionResult
    S->>P: encode result and send Packet
    P-->>C: response Packet
```

`Executor` 绑定单个 client connection 的 transaction context。显式 `BEGIN` 保留持久化
XID；没有显式事务时，写语句使用普通 implicit transaction，查询语句使用进程内
read-only transaction。

## 物理修改与 WAL 刷页

```mermaid
sequenceDiagram
    participant R as PageRecordManager
    participant L as WriteAheadLogger
    participant P as Page
    participant C as PageBufferPool / PageCleaner
    participant F as Data file

    R->>L: append Insert / Update log
    L-->>R: startLsn, endLsn
    R->>P: write record bytes and set page LSN to endLsn
    R->>C: mark dirty page(recLSN = startLsn)
    C->>L: flush page LSN
    L-->>C: WAL durable
    C->>F: write page snapshot and force
```

page LSN 已持久化在 Page header；Redo 使用完整 record 的 `endLsn` 进行幂等跳过。

## Transaction 生命周期

### 显式事务

```mermaid
sequenceDiagram
    participant C as Client
    participant E as Executor
    participant T as TableManager
    participant V as VersionManager
    participant A as XidAllocator
    participant X as XidStatusTable
    participant W as WAL / ATT

    C->>E: BEGIN
    E->>T: begin
    T->>V: begin
    V->>A: allocate XID
    A-->>V: xid
    V->>X: record IN_PROGRESS
    V->>W: append BEGIN and register ATT
    V-->>E: xid retained by Executor
    C->>E: INSERT or UPDATE or SELECT
    E->>T: execute with xid
    T-->>E: StatementResult
    C->>E: COMMIT
    E->>T: commit xid
    T->>V: commit xid
    V->>W: append and flush COMMIT
    V->>X: record COMMITTED
    V->>W: append END and remove ATT
    E-->>C: commit result
```

`Executor` 保存 connection-bound XID。连接在 commit 前关闭时，`Executor.close()` 会 abort 该 XID，再释放 `DatabaseContext`。

### 隐式事务

```mermaid
sequenceDiagram
    participant E as Executor
    participant T as TableManager

    alt SELECT / SHOW / DESCRIBE
        E->>T: begin read-only transaction
        T-->>E: negative in-memory xid
        E->>T: execute query
        E->>T: end read-only transaction
    else write statement
        E->>T: begin durable transaction
        T-->>E: persistent xid
        E->>T: execute write
        alt statement succeeds
            E->>T: commit xid
        else statement fails
            E->>T: abort xid
        end
    end
    E->>E: clear xid
```

负数 read-only XID 只存在于 `VersionManager` 的 active transaction map，用于定位
READ COMMITTED read view；它不会写入 Entry、XID 文件、ATT 或 WAL。显式事务中的
查询仍使用显式事务的持久化 XID 和隔离级别。

## MVCC visibility

```mermaid
sequenceDiagram
    participant W as Writer transaction
    participant R as Reader transaction
    participant V as VersionManager
    participant E as Entry
    participant X as XidStatusTable

    W->>V: update record
    V->>E: write XMAX and new version
    V->>X: transaction remains active
    R->>V: read record
    V->>E: read XMIN and XMAX
    V->>X: inspect creator and deleter status
    alt version is visible
        V-->>R: return payload
    else version is invisible
        V-->>R: skip version or follow newer version
    end
```

visibility 由 XID status、reader snapshot 与 isolation level 共同决定。`READ COMMITTED` 和 `REPEATABLE READ` 的差异应在 transaction tests 中体现。

## PageBufferPool 并发加载

```mermaid
sequenceDiagram
    participant A as Thread A
    participant B as Thread B
    participant C as AbstractCache
    participant P as PageBufferPool
    participant F as Data file

    A->>C: get page key
    C->>C: register LoadState
    C->>P: load page
    P->>F: positioned read
    B->>C: get same page key
    C->>C: join existing LoadState
    F-->>P: page bytes
    P-->>C: CachedPage
    C-->>A: same cached Page
    C-->>B: same cached Page
```

相同 key 的并发请求共享一次 load；不同 Page key 的 load 不应被 cache-level key lock 串行化。Page 内部 read/write latch 只协调同一页的内存访问。

## Crash recovery

```mermaid
sequenceDiagram
    participant O as Database open
    participant R as Recovery
    participant L as WriteAheadLogger
    participant P as PageBufferPool
    participant X as XidStatusTable

    O->>R: recover
    R->>L: Analysis from checkpointLsn
    R->>R: rebuild DPT and ATT
    R->>L: Redo from min(DPT.recoveryLsn)
    R->>P: repeat history with page LSN check
    R->>L: follow loser prevLsn chains
    R->>L: append CLR before compensation
    R->>P: apply compensation image
    R->>X: persist ABORTED status
    R->>L: append and flush END
```

Recovery 从最近完整 fuzzy checkpoint 开始 Analysis。Redo 对 DPT 中可能缺失的
INSERT、UPDATE 与 CLR repeat history，并通过 page LSN 幂等跳过；Undo 使用
按 LSN 降序的 loser queue，CLR 的 `undoNextLsn` 避免重复撤销。

## PageCleaner and checkpoint

```mermaid
sequenceDiagram
    participant R as PageRecordManager
    participant D as DirtyPageTable
    participant C as PageCleaner
    participant P as PageBufferPool
    participant L as WriteAheadLogger
    participant F as Data file

    R->>D: mark page dirty with recLSN
    D-->>C: signal dirty page available
    C->>D: get batch ordered by recLSN
    loop for each dirty page
        C->>P: create page snapshot
        alt page LSN exceeds flushedLsn
            P->>L: flush page LSN
            L-->>P: WAL durable
        end
        P->>F: write snapshot
    end
    C->>F: force batch
    C->>P: compare page LSN and mark clean
    C->>L: append BEGIN_CHECKPOINT
    C->>D: snapshot DPT
    C->>L: append DPT / ATT chunks and END_CHECKPOINT
    C->>L: flush END and persist checkpointLsn
```

`recoveryLsn` 只记录某页从干净变为脏时的第一条 WAL `startLsn`。
Checkpoint snapshot 不停止事务或 PageCleaner；BEGIN 之后发生的并发日志会由
Analysis 顺序扫描并与 snapshot 合并。WAL durable 边界仍由 `flushedLsn` 表示。
