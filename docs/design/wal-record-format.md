# WAL Record 目标格式

> 状态：M1–M6 已实现；M7 Page lifecycle 与 M8 WAL retention 明确暂缓。
>
> 本文定义 MiniSQL 下一阶段 WAL 的持久化字节格式。总体恢复算法、实施顺序与
> crash-test 要求见 [WAL 与 Recovery 目标设计及实施计划](wal-recovery.md)。

## 1. 设计原则

1. `LogManager` 只识别 WAL file header、physical record header 和 `byte[] payload`。
2. `LogRecordCodec` 负责在结构化 payload 与 `byte[]` 之间 encode/decode。
3. 所有整数使用固定字节宽度；持久化的 type 使用显式 byte code，不使用
   `enum.ordinal()`。
4. 所有可变长字段前都保存明确的 `length`，不通过“剩余字节平分”等方式推断边界。
5. `startLsn` 用于定位 record；`endLsn` 用于 durable boundary 与 page LSN。
6. 当前直接升级到新 WAL format version，不兼容读取旧开发格式。

## 2. 基础类型

所有持久化字段采用统一编码规则：

```text
┌────────────┬────────────────┐
│ Property   │ Value          │
├────────────┼────────────────┤
│ Byte order │ BIG_ENDIAN     │
│ Padding    │ NONE           │
│ Alignment  │ PACKED         │
│ Checksum   │ CRC32C         │
└────────────┴────────────────┘
```

多字节整数从最高有效字节到最低有效字节依次写入。CRC 输入使用字段按照本文布局
编码后的原始 bytes，不使用 Java 对象值、字符串或平台本地字节序。

```text
┌──────────────────┬────────┬──────────────────────────────────────────┐
│ Type             │ Length │ Description                              │
├──────────────────┼────────┼──────────────────────────────────────────┤
│ byte             │  1B    │ WAL type, transaction status             │
│ short            │  2B    │ recordOffset within a Page               │
│ int              │  4B    │ PGNO, length, count, chunkIndex          │
│ long             │  8B    │ XID, LSN                                 │
└──────────────────┴────────┴──────────────────────────────────────────┘
```

```text
NO_LSN = 0L
```

WAL file header 占用前 32B，因此有效 record 的 `startLsn` 不可能是 `0`。

## 3. WAL 文件

```text
WAL file
┌──────────────────────────────┬────────────────────┬────────────────────┬─────┐
│        WAL file header       │    LogRecord 1     │    LogRecord 2     │ ... │
│             32B              │  variable length   │  variable length   │     │
└──────────────────────────────┴────────────────────┴────────────────────┴─────┘
```

### 3.1 WAL file header

```text
┌────────┬─────────┬───────────┬───────────────┬────────────┬──────────┐
│ magic  │ version │ headerCrc │ checkpointLsn │ flushedLsn │ reserved │
│   4B   │   4B    │    4B     │      8B       │     8B     │    4B    │
└────────┴─────────┴───────────┴───────────────┴────────────┴──────────┘
```

```text
totalLength = 4 + 4 + 4 + 8 + 8 + 4 = 32B
```

- `magic`：识别 MiniSQL WAL 文件。
- `version`：WAL format version。
- `headerCrc`：校验除自身之外的 header 字段。
- `checkpointLsn`：最近一个完整 fuzzy checkpoint 的
  `BEGIN_CHECKPOINT.startLsn`。
- `flushedLsn`：已经 durable 的 WAL byte boundary。
- `reserved`：未来扩展，写入时清零。

只有对应 `END_CHECKPOINT` durable 后，才能更新并 force `checkpointLsn`。

### 3.2 整体嵌套结构

下面的图从 WAL file 开始，逐层展开 physical record、payload、具体日志类型以及
最终写入 DataPage 的 PageRecord。方括号表示连续的持久化字节，树形缩进表示字段
内部承载的下一层结构。

```text
WAL file
│
├── WAL file header                                                32B
│   ├── magic                                                       4B
│   ├── version                                                     4B
│   ├── headerCrc                                                   4B
│   ├── checkpointLsn                                               8B
│   ├── flushedLsn                                                  8B
│   └── reserved                                                    4B
│
└── LogRecord                                          variable length
    ├── startLsn                                  in-memory start offset
    ├── endLsn                                      physical end offset
    │
    └── persisted bytes
        ├── physical record header                                 16B
        │   ├── payloadLength                                       4B
        │   ├── recordCrc                                           4B
        │   └── endLsn                                              8B
        │
        └── payload                                      payloadLength
            │
            ├── transaction payload
            │   ├── common header                                  17B
            │   │   ├── type                                        1B
            │   │   ├── XID                                         8B
            │   │   └── prevLsn                                     8B
            │   │
            │   └── type-specific body
            │       │
            │       ├── BEGIN / COMMIT / ABORT / END                0B
            │       │
            │       ├── INSERT
            │       │   ├── PGNO                                    4B
            │       │   ├── recordOffset                            2B
            │       │   ├── recordLength                            4B
            │       │   └── recordBytes                   recordLength
            │       │       └── PageRecord
            │       │           ├── validFlag                       1B
            │       │           ├── payloadLength                   2B
            │       │           └── PageRecord payload
            │       │               └── Entry
            │       │                   ├── XMIN                    8B
            │       │                   ├── XMAX                    8B
            │       │                   └── business payload        NB
            │       │
            │       ├── UPDATE
            │       │   ├── PGNO                                    4B
            │       │   ├── recordOffset                            2B
            │       │   ├── beforeImageLength                       4B
            │       │   ├── beforeImage              beforeImageLength
            │       │   │   └── PageRecord
            │       │   ├── afterImageLength                        4B
            │       │   └── afterImage                afterImageLength
            │       │       └── PageRecord
            │       │
            │       └── CLR
            │           ├── undoNextLsn                             8B
            │           ├── PGNO                                    4B
            │           ├── recordOffset                            2B
            │           ├── compensationImageLength                 4B
            │           └── compensationImage
            │               └── PageRecord
            │
            └── checkpoint payload
                ├── BEGIN_CHECKPOINT
                ├── CHECKPOINT_DPT
                │   └── repeated DPT entry
                │       ├── PGNO                                    4B
                │       └── recoveryLsn                             8B
                ├── CHECKPOINT_ATT
                │   └── repeated ATT entry
                │       ├── XID                                     8B
                │       ├── status                                  1B
                │       └── lastLsn                                 8B
                └── END_CHECKPOINT
```

图中的 `PageRecord payload → Entry` 表示当前正常 MVCC 写入路径；WAL 与
PageRecord 本身只保存 bytes，不解释最内层 business payload 的 Table、Field 或
其他上层语义。

从 INSERT 路径横向展开后，实际连续字节顺序为：

```text
[WAL file header]
        │
        ▼
[payloadLength][recordCrc][endLsn]
        │
        ▼
[INSERT][XID][prevLsn][PGNO][recordOffset][recordLength]
        │
        ▼
[validFlag][payloadLength]
        │
        ▼
[XMIN][XMAX][business payload]
```

## 4. Physical LogRecord

```text
LogRecord
┌───────────────┬───────────┬──────────┬────────────────────┐
│ payloadLength │ recordCrc │  endLsn  │      payload       │
│      4B       │    4B     │    8B    │        N B         │
└───────────────┴───────────┴──────────┴────────────────────┘
```

```text
physicalHeaderLength = 16B
recordLength         = 16B + payloadLength
```

`recordCrc` 的目标校验范围：

```text
payloadLength + endLsn + payload
```

`LogRecord` 的物理区间：

```text
              startLsn                              endLsn
                  │                                    │
                  ▼                                    ▼
WAL file ─────────┌────────────────────────────────────┐─────────
                  │             LogRecord              │
                  └────────────────────────────────────┘
                  [            LogRecord               )
```

- `startLsn` 由 record 在 WAL 文件中的起始位置得到，不重复写入 header。
- `endLsn` 写入 physical record header。
- `LogRecord` 使用半开区间 `[startLsn, endLsn)`。
- 内存对象 `LogRecord` 直接保存 `startLsn`、`endLsn` 与 `byte[] payload`。

目标 API：

```java
LogRecord append(byte[] payload);

LogRecord next();
```

## 5. Transaction WAL payload 公共头

所有属于 transaction log chain 的 payload 使用以下公共头：

```text
Transaction WAL payload
┌─────────┬──────────┬────────────┬─────────────────────────┐
│  type   │   XID    │  prevLsn   │      specific body      │
│   1B    │    8B    │     8B     │           N B           │
└─────────┴──────────┴────────────┴─────────────────────────┘
```

```text
commonHeaderLength = 1 + 8 + 8 = 17B
```

字段偏移：

```text
┌─────────┬────────┬────────┐
│ Field   │ Offset │ Length │
├─────────┼────────┼────────┤
│ type    │   0    │   1B   │
│ XID     │   1    │   8B   │
│ prevLsn │   9    │   8B   │
│ body    │  17    │   N B  │
└─────────┴────────┴────────┘
```

`prevLsn` 保存同一 XID 上一条 WAL record 的 `startLsn`：

```text
COMMIT@230 ── prevLsn=150 ──▶ UPDATE@150
    UPDATE@150 ── prevLsn=100 ──▶ BEGIN@100
        BEGIN@100 ── prevLsn=NO_LSN ──▶ END OF CHAIN
```

ATT 中的 `lastLsn` 同样保存该 transaction 最新 record 的 `startLsn`。

## 6. BEGIN

```text
BEGIN payload
┌─────────┬──────────┬────────────┐
│  type   │   XID    │  prevLsn   │
│   1B    │    8B    │     8B     │
└─────────┴──────────┴────────────┘
```

```text
payloadLength = 17B
prevLsn       = NO_LSN
```

BEGIN 创建 transaction log chain 的起点，不包含 Page address 或 image。

## 7. INSERT

```text
INSERT payload
┌──────┬─────┬─────────┬──────┬──────────────┬──────────────┬─────────────┐
│ type │ XID │ prevLsn │ PGNO │ recordOffset │ recordLength │ recordBytes │
│  1B  │ 8B  │   8B    │  4B  │      2B      │      4B      │     N B     │
└──────┴─────┴─────────┴──────┴──────────────┴──────────────┴─────────────┘
```

```text
payloadLength = 27B + recordLength
```

- `PGNO`：目标 DataPage。
- `recordOffset`：record 在 Page 内的起始偏移。
- `recordLength`：完整 `recordBytes` 长度。
- `recordBytes`：包含 PageRecord header 与 payload 的完整 physical record。

`recordBytes`：

```text
PageRecord bytes
┌───────────┬───────────────┬────────────────────┐
│ validFlag │ payloadLength │ PageRecord payload │
│    1B     │      2B       │        N B         │
└───────────┴───────────────┴────────────────────┘
```

INSERT REDO 将 `recordBytes` 写入 `[PGNO, recordOffset]`；INSERT Undo 通过 CLR
记录并重放对应的 invalid record image。

## 8. UPDATE

```text
UPDATE payload
┌──────┬─────┬─────────┬──────┬──────────────┬───────────────────┬─────────────┐
│ type │ XID │ prevLsn │ PGNO │ recordOffset │ beforeImageLength │ beforeImage │
│  1B  │ 8B  │   8B    │  4B  │      2B      │        4B         │     N B     │
└──────┴─────┴─────────┴──────┴──────────────┴───────────────────┴─────────────┘

┌──────────────────┬────────────┐
│ afterImageLength │ afterImage │
│        4B        │    M B     │
└──────────────────┴────────────┘
```

连续字节布局：

```text
[type][XID][prevLsn][PGNO][recordOffset]
[beforeImageLength][beforeImage]
[afterImageLength][afterImage]
```

```text
payloadLength = 31B + beforeImageLength + afterImageLength
```

- `beforeImage`：Undo 原 UPDATE 时恢复的完整 PageRecord bytes。
- `afterImage`：Redo UPDATE 时写入的完整 PageRecord bytes。
- 两个 image 分别保存长度，不再假设二者等长。

## 9. COMMIT

```text
COMMIT payload
┌─────────┬──────────┬────────────┐
│  type   │   XID    │  prevLsn   │
│   1B    │    8B    │     8B     │
└─────────┴──────────┴────────────┘
```

```text
payloadLength = 17B
```

COMMIT append 后必须执行：

```text
flush(COMMIT.endLsn)
```

只有完整 COMMIT record durable 后才能向客户端返回提交成功。

## 10. ABORT

```text
ABORT payload
┌─────────┬──────────┬────────────┐
│  type   │   XID    │  prevLsn   │
│   1B    │    8B    │     8B     │
└─────────┴──────────┴────────────┘
```

```text
payloadLength = 17B
```

ABORT 表示 transaction 进入 `ABORTING`，随后从该 transaction 的
`lastLsn` 开始执行 Undo。

## 11. END

```text
END payload
┌─────────┬──────────┬────────────┐
│  type   │   XID    │  prevLsn   │
│   1B    │    8B    │     8B     │
└─────────┴──────────┴────────────┘
```

```text
payloadLength = 17B
```

END 表示该 transaction 的提交或撤销清理已经完成。写入 END 后从 ATT 移除该
transaction。END 不需要单独 force，但 checkpoint 或 WAL retention 在删除其依赖的
日志前必须保证它 durable。

## 12. CLR

```text
CLR payload
┌──────┬─────┬─────────┬─────────────┬──────┬──────────────┬─────────────────────────┐
│ type │ XID │ prevLsn │ undoNextLsn │ PGNO │ recordOffset │ compensationImageLength │
│  1B  │ 8B  │   8B    │     8B      │  4B  │      2B      │           4B            │
└──────┴─────┴─────────┴─────────────┴──────┴──────────────┴─────────────────────────┘

┌───────────────────┐
│ compensationImage │
│        N B        │
└───────────────────┘
```

```text
payloadLength = 35B + compensationImageLength
```

- `prevLsn`：该 XID 在 CLR 之前最新 record 的 `startLsn`。
- `undoNextLsn`：完成本次 compensation 后，下一条待 Undo record 的
  `startLsn`。
- `compensationImage`：Redo CLR 时写入 Page 的完整 physical record image。

CLR 可以 Redo，但不能再次 Undo：

```text
UPDATE / INSERT → append CLR, then apply compensation
CLR             → skip compensation and follow undoNextLsn
```

## 13. Checkpoint payload

Checkpoint record 不属于某个 transaction，因此不使用 `[XID][prevLsn]` 公共头。

### 13.1 BEGIN_CHECKPOINT

```text
BEGIN_CHECKPOINT payload
┌──────────────────┐
│       type       │
│        1B        │
└──────────────────┘
```

```text
payloadLength = 1B
```

其 `startLsn` 在 checkpoint 执行期间保存为 `beginCheckpointLsn`。

### 13.2 CHECKPOINT_DPT

```text
CHECKPOINT_DPT payload
┌──────┬────────────────────┬────────────┬────────────┬────────────────────┐
│ type │ beginCheckpointLsn │ chunkIndex │ entryCount │    DPT entries     │
│  1B  │         8B         │     4B     │     4B     │ entryCount entries │
└──────┴────────────────────┴────────────┴────────────┴────────────────────┘
```

单个 DPT entry：

```text
DPT entry
┌──────────┬─────────────┐
│   PGNO   │ recoveryLsn │
│    4B    │     8B      │
└──────────┴─────────────┘
```

```text
payloadLength = 17B + entryCount * 12B
```

- `beginCheckpointLsn`：该 chunk 所属 `BEGIN_CHECKPOINT.startLsn`。
- `chunkIndex`：DPT chunk 从 `0` 开始的连续序号。
- `recoveryLsn`：首次使该 Page 变脏的 record `startLsn`。

### 13.3 CHECKPOINT_ATT

```text
CHECKPOINT_ATT payload
┌──────┬────────────────────┬────────────┬────────────┬────────────────────┐
│ type │ beginCheckpointLsn │ chunkIndex │ entryCount │    ATT entries     │
│  1B  │         8B         │     4B     │     4B     │ entryCount entries │
└──────┴────────────────────┴────────────┴────────────┴────────────────────┘
```

单个 ATT entry：

```text
ATT entry
┌──────────┬──────────┬────────────┐
│   XID    │  status  │  lastLsn   │
│    8B    │    1B    │     8B     │
└──────────┴──────────┴────────────┘
```

```text
payloadLength = 17B + entryCount * 17B
```

- `status`：显式 byte code，例如 `ACTIVE`、`COMMITTING`、`ABORTING`。
- `lastLsn`：该 transaction 最新 record 的 `startLsn`。

### 13.4 END_CHECKPOINT

```text
END_CHECKPOINT payload
┌──────┬────────────────────┬─────────────────┬─────────────────┐
│ type │ beginCheckpointLsn │ dptChunkCount   │ attChunkCount   │
│  1B  │         8B         │       4B        │       4B        │
└──────┴────────────────────┴─────────────────┴─────────────────┘
```

```text
payloadLength = 17B
```

Recovery 只有在满足以下条件时才接受该 checkpoint：

```text
BEGIN_CHECKPOINT exists
        │
        ▼
all chunks reference the same beginCheckpointLsn
        │
        ▼
DPT chunkIndex covers [0, dptChunkCount)
        │
        ▼
ATT chunkIndex covers [0, attChunkCount)
        │
        ▼
END_CHECKPOINT is complete and its CRC is valid
```

## 14. Page lifecycle record

`PAGE_ALLOCATE`、`PAGE_FREE` 只在 M7 真正引入 Page 回收与重用时加入。其 payload
必须至少表达 Page identity、transaction log chain 和幂等 Redo/Undo 所需状态，但
本文暂不冻结字段布局，避免在 Page lifecycle 尚未设计完成前形成错误兼容承诺。

## 15. startLsn 与 endLsn

```text
┌───────────────────────────────┬──────────────────────────────────────────┐
│ Field                         │ Stored value                             │
├───────────────────────────────┼──────────────────────────────────────────┤
│ LogRecord.startLsn            │ physical start of the current record     │
│ LogRecord.endLsn              │ physical end boundary of current record │
│ prevLsn                       │ previous transaction record startLsn     │
│ ATT.lastLsn                   │ latest transaction record startLsn       │
│ CLR.undoNextLsn               │ next record startLsn to undo             │
│ DPT.recoveryLsn               │ first dirtying record startLsn           │
│ checkpointLsn                 │ BEGIN_CHECKPOINT.startLsn                │
│ page LSN                      │ latest page-modifying record endLsn      │
│ current/written/flushed LSN   │ WAL byte boundary, a record endLsn       │
│ flush(targetLsn)              │ target record endLsn                     │
└───────────────────────────────┴──────────────────────────────────────────┘
```

写入一条 transaction record：

```text
read ATT.lastLsn
        │
        ▼
encode payload.prevLsn
        │
        ▼
LogManager.append(payload)
        │
        ├── LogRecord.startLsn
        └── LogRecord.endLsn
        │
        ▼
ATT.lastLsn = startLsn
        │
        ├── first dirtying: DPT.recoveryLsn = startLsn
        └── page modified: page LSN = endLsn
```

## 16. 长度与边界校验

Decoder 必须依次验证：

```text
payloadLength is non-negative and within the configured limit
        │
        ▼
startLsn + 16 + payloadLength == endLsn
        │
        ▼
record is complete within the WAL durable boundary
        │
        ▼
recordCrc is valid
        │
        ▼
type is a registered explicit byte code
        │
        ▼
all fixed-length fields are present
        │
        ▼
all length and count arithmetic is overflow-safe
        │
        ▼
variable field boundaries exactly match payloadLength
```

Decoder 必须拒绝：

- unknown type；
- 负 length、负 count 或越界 offset；
- 截断的 payload/image/chunk；
- UPDATE image 边界不吻合；
- checkpoint chunk 缺失、重复、乱序或归属不一致；
- CRC 错误；
- 解析结束后仍有未定义的尾随 bytes。
