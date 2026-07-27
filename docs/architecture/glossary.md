# 术语表

| 术语 | 含义 |
|---|---|
| Page | 固定大小的存储页，也是 PageCache 的缓存单位。 |
| Record | Page 内的 physical record，包含有效性标记、长度和 entry bytes。 |
| Entry | MVCC 层的版本条目，包含 XMIN、XMAX 与 payload。 |
| Row | table 层的一行逻辑数据；编码后得到 row bytes。 |
| WAL | Write-Ahead Logging，数据页持久化前先确保日志 durable。 |
| LSN | Log Sequence Number；本项目中为 WAL 文件 byte offset。 |
| page LSN | 最后修改某页的 WAL record `endLsn`。 |
| recLSN | 页本轮变脏的第一条 WAL record `startLsn`。 |
| XID | transaction identifier。 |
| UID | physical record identifier，由 PGNO 和页内偏移组成。 |
| PGNO | page number。 |
| FSO | free space offset，DataPage 当前未使用空间的起点。 |
| latch | 保护内存数据结构短临界区的锁；本项目中用于 Page read/write 协调。 |
