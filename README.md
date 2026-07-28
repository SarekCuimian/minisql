# MiniSQL

MiniSQL 是一个基于 Java 的小型数据库项目，当前代码包含三部分入口：

- `com.minisql.server.Launcher`：数据库 TCP 服务端
- `com.minisql.client.Launcher`：交互式命令行客户端
- `com.minisql.api.MiniSqlApplication`：基于 Spring Boot 的 HTTP API

项目当前实现了以下核心能力：

- ARIES-style WAL 与崩溃恢复：page LSN、事务日志链、Analysis、Redo、CLR Undo 和 fuzzy checkpoint
- 后台 PageCleaner、WAL-before-data 刷盘约束与 Dirty Page Table
- XID 分配、事务状态持久化和运行期事务管理
- MVCC ReadView，以及 `READ COMMITTED`、`REPEATABLE READ` 两种隔离级别
- 基于 PageRecord UID 的严格 2PL 记录锁、死锁检测与锁等待超时
- 显式事务与隐式事务
- 基础 DDL / DML / 聚合查询
- 基于 Socket 的数据库服务端与客户端
- 基于 Session 的 HTTP SQL 执行接口

## 环境要求

- JDK 11
- Maven 3.8+

`pom.xml` 当前配置的编译版本为 Java 11。

## 目录说明

- `src/main/java/com/minisql/engine`：数据库内核
- `src/main/java/com/minisql/engine/storage`：Page、Record、WAL 与恢复
- `src/main/java/com/minisql/engine/transaction`：事务状态、MVCC 与锁管理
- `src/main/java/com/minisql/server`：TCP 服务端
- `src/main/java/com/minisql/transport`：客户端与服务端的网络协议
- `src/main/java/com/minisql/client`：命令行客户端
- `src/main/java/com/minisql/api`：Spring Boot API
- `docs/architecture`：当前已实现架构
- `docs/design`：存储、WAL 与 Recovery 的目标设计和实施记录
- `openspec/changes`：可独立验收的设计变更
- `scripts/restart.sh`：一键重启后端并启动客户端
- `scripts/start-client.sh`：在后端已启动时打开客户端

## 快速开始

### 1. 编译项目

```bash
mvn clean compile
```

运行测试：

```bash
mvn test
```

### 2. 初始化数据目录

下面命令会在指定目录创建数据库根目录，并自动创建默认库：

```bash
mvn exec:java -Dexec.mainClass="com.minisql.server.Launcher" -Dexec.args="-create /tmp/minisql"
```

### 3. 启动数据库服务端

服务端固定监听 `9999` 端口：

```bash
mvn exec:java -Dexec.mainClass="com.minisql.server.Launcher" -Dexec.args="-open /tmp/minisql"
```

也可以直接使用脚本：

```bash
./scripts/restart.sh /tmp/minisql
```

### 4. 启动命令行客户端

```bash
mvn exec:java -Dexec.mainClass="com.minisql.client.Launcher"
```

或在后端已经启动后执行：

```bash
./scripts/start-client.sh
```

客户端连接地址固定为 `127.0.0.1:9999`。

## 客户端使用

客户端是交互式 Shell，输入以分号 `;` 结尾的 SQL 后执行。

退出方式：

- 输入 `exit`
- 输入 `quit`
- 按 `Ctrl+D`

`Ctrl+C` 不会退出客户端，只会中断当前输入。

## 事务说明

支持显式事务：

```sql
begin;
begin isolation level read committed;
begin isolation level repeatable read;
commit;
rollback;
```

其中：

- `rollback` 与 `abort` 等价
- 如果不显式开启事务，普通 SQL 会被自动包装进隐式事务
- 当前事务未 `commit` 时，其他事务读不到其写入的数据
- 客户端异常断开时，未完成事务会自动回滚

## 示例 SQL

```sql
show databases;
create database shop;
use shop;

create table users (
    id int64 primary key,
    name string unique,
    age int32
);

show tables;
describe users;

insert into users (id, name, age) values (1, 'Alice', 20);
insert into users (id, name, age) values (2, 'Bob', 25);
insert into users (id, name, age) values (3, 'Carol', 20);

select * from users;
select id, name from users where age >= 20;
select age, count(*) as total
from users
group by age
having total >= 2;

begin isolation level read committed;
update users set age = 26 where id = 2;
commit;

begin isolation level repeatable read;
delete from users where id = 3;
rollback;

select * from users where id = 3;

drop table users;
drop database shop;
```

## HTTP API

Spring Boot API 默认监听 `9906`，配置见 `src/main/resources/application.yml`。

启动方式：

```bash
mvn spring-boot:run
```

默认会转发到数据库 TCP 服务端 `127.0.0.1:9999`，所以启动 API 前请先启动后端。

当前接口：

- `POST /api/sessions`：创建会话
- `POST /api/sessions/{sessionId}/sql`：执行 SQL
- `DELETE /api/sessions/{sessionId}`：关闭会话

示例：

```bash
curl -X POST http://127.0.0.1:9906/api/sessions
```

```bash
curl -X POST http://127.0.0.1:9906/api/sessions/{sessionId}/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"show databases;","format":"TEXT"}'
```

## 存储与恢复

MiniSQL 使用 Write-Ahead Logging 保证事务修改能够在崩溃后恢复。Page Header
持久化 page LSN，Redo 根据 page LSN 判断日志是否需要重放。每个更新事务维护
`BEGIN → UPDATE/INSERT → COMMIT/ABORT → END` 日志链。

恢复过程包含 Analysis、repeat-history Redo 和 Undo。Analysis 从最近一次完整
checkpoint 开始重建 Dirty Page Table 与 Active Transaction Table；Undo 使用
CLR 和 `undoNextLsn` 记录补偿操作，因此恢复过程中再次崩溃后仍可继续。

运行时的核心写入顺序：

```text
生成 PageRecord 修改及 before/after image
        ↓
追加 WAL
        ↓
修改缓存页、更新 Page LSN，并登记 Dirty Page Table
        ↓
PageCleaner 刷 WAL
        ↓
PageCleaner 写数据页
```

`DatabaseContext` 负责组装和关闭单个数据库的 XID、WAL、BufferPool、
Checkpoint、PageRecord、MVCC 与 Table 组件。数据库正常关闭时会停止后台任务、
刷出脏页、记录最终 checkpoint，再关闭 WAL 和 XID 文件。

详细设计：

- [WAL 与 Recovery 设计](docs/design/wal-recovery.md)
- [WAL Record 持久化格式](docs/design/wal-record-format.md)
- [当前架构文档](docs/architecture/README.md)

## 正常关闭

建议按下面顺序关闭：

1. 先退出客户端：`exit` / `quit` / `Ctrl+D`
2. 再停止服务端：在服务端终端按 `Ctrl+C`

服务端注册了 shutdown hook，会关闭线程池、Socket 和数据库上下文。

如果只想关闭某个数据目录对应的数据库资源，可以执行：

```bash
mvn exec:java -Dexec.mainClass="com.minisql.server.Launcher" -Dexec.args="-shutdown /tmp/minisql"
```

## 当前边界

- `jps` 默认只显示短类名，所以服务端和客户端都会显示为 `Launcher`
- 想区分具体进程时，建议使用 `jps -lv`
- Checkpoint 当前每 30 秒调度一次，正常关闭时还会记录最终 checkpoint
- WAL 当前仍是单文件，不进行 segment 切分和历史日志回收
- 当前只追加新 Page，不回收或重用已经释放的 Page
- 记录锁以 PageRecord UID 为资源，不包含 MySQL 风格的 gap lock 或 next-key lock
- 未提交事务会在恢复时被 Analysis/Redo/Undo 流程回滚
