# 11. 风险与技术债

| 优先级 | 风险 / 债务 | 影响 | 跟踪方式 |
|---|---|---|---|
| P2 | physical image WAL 体积较大 | 写放大、恢复扫描成本 | 未来评估 logical logging 或更紧凑格式 |
| P2 | 尚无 Page allocation/free WAL | 暂不支持可恢复的 Page 回收与重用 | M7 暂缓 |
| P2 | WAL 仍是单文件且不回收 | WAL 长期运行会持续增长 | M8 暂缓 |
| P2 | HTTP API 与 TCP client 的会话、错误映射需持续对齐 | 用户可见的错误语义不一致 | API integration tests |

风险关闭条件必须包含可重复测试；只修改注释或命名不视为关闭。
