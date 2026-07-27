# XID 文件格式

`.xid` 文件由 `XidAllocator` 持有，`XidStatusTable` 通过它提供的包内方法维护
事务状态。WAL 是崩溃恢复时提交与回滚结果的权威来源。

## 文件布局

```text
[magic:4][formatVersion:4][reservedUpperBound:8][status:1]...
```

- `magic`：`MXID`。
- `formatVersion`：当前为 `1`。
- `reservedUpperBound`：已预留 XID 区间的开区间上界。
- 第 `xid` 个状态位于 `16 + xid - 1`。

状态编码：

```text
0 = UNUSED
1 = IN_PROGRESS
2 = COMMITTED
3 = ABORTED
```

## 区间预留

默认一次预留 1024 个 XID。预留区间扩展并 `force` 后才会向调用方返回其中的
XID。进程崩溃或重启后从已持久化的上界继续分配，因此允许出现空洞，但不会复用
旧 XID。

```text
persisted range: [1, 1025)
used range:      [1, 17)
restart next:    1025
```

## 状态与 WAL 顺序

```text
BEGIN:
allocate → IN_PROGRESS → append BEGIN → flush BEGIN

COMMIT:
append COMMIT → flush COMMIT → COMMITTED → append END

ABORT:
append ABORT → Undo/CLR → flush END → ABORTED
```

状态字节不需要为每次转换单独执行 `force`：如果进程在 WAL durable 后、状态页
落盘前崩溃，Recovery 会根据 COMMIT、ABORT、CLR 和 END 修复状态表。

## 旧格式迁移

打开旧格式 `[xidCounter:8][legacyStatus:1]...` 时，会先写入临时迁移文件，
转换文件头和状态编码，再原子替换原 `.xid` 文件。旧状态映射为：

```text
ACTIVE    → IN_PROGRESS
COMMITTED → COMMITTED
ABORTED   → ABORTED
```
