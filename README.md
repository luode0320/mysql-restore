# mysql-restore

MySQL 原始 IBD 表空间恢复控制台。

## 入口

页面入口是 [index.html](index.html)，实际发起恢复任务需要运行本地服务：

```sh
python server.py
```

默认 API 服务地址：

```text
http://127.0.0.1:33061
```

启动后直接访问后端页面：

```text
http://服务器IP:33061/
```

也可以访问 `/index.html`，不再需要通过 nginx 静态目录引用页面文件。

## Docker 运行

工具容器只连接目标 MySQL 并执行 DDL / `ALTER TABLE`，不移动 `.ibd` 文件。因为新的 MySQL 是另一个容器，`.ibd` 文件或库目录需要人工在宿主机上移动到新 MySQL 的 data 目录。

```sh
docker build -t mysql-restore:latest .
docker run -d \
  --network host \
  --name mysql-restore \
  -e DB_HOST=127.0.0.1 \
  -e DB_PORT=33060 \
  -e DB_USER=root \
  -e DB_PASSWORD="$DB_PASSWORD" \
  -e DB_SSL_MODE=DISABLED \
  -e PORT=33061 \
  -e RESTORE_ROOT=/usr/local/src/restoredb \
  -e DDL_DIR=/usr/local/src/restoredb/ddl \
  -e DDL_BACKUP_DIR=/usr/local/src/restoredb/ddl-backup \
  -e RESTORE_OUTPUT_DIR=/usr/local/src/restoredb/restore-jobs \
  -e DDL_SYNC_ENABLED=true \
  -e DDL_SYNC_DAILY_TIME=00:00 \
  -e DDL_BACKUP_RETENTION_DAYS=31 \
  -v /usr/local/src/restoredb:/usr/local/src/restoredb \
  mysql-restore:latest
```

## 需要准备的东西

- 目标 MySQL 连接信息：`DB_HOST`、`DB_PORT=33060`、`DB_USER`、`DB_PASSWORD`、`DB_SSL_MODE=DISABLED`
- 程序 HTTP/API 服务端口：`PORT=33061`
- 已同步的 DDL 文件目录：默认 `/usr/local/src/restoredb/ddl`
- 旧 DDL 备份目录：默认 `/usr/local/src/restoredb/ddl-backup`
- 原始 `.ibd` 文件或库目录，由人工移动到新 MySQL 的 data 目录

## 页面参数

页面只输入一个参数：恢复对象。

```text
binance
binance/user.ibd
```

- 输入 `binance`：恢复 `binance` 库 DDL 目录下的所有表。
- 输入 `binance/user.ibd`：只恢复 `binance.user` 单表。

不要输入宿主机绝对路径。工具容器不会读取或移动这些 `.ibd` 文件。

## 恢复步骤

1. 页面输入 `binance` 或 `binance/user.ibd`，点击“准备恢复”。
2. 工具会根据 DDL 创建库表，并执行 `ALTER TABLE ... DISCARD TABLESPACE`。
3. 在宿主机上手动移动原始 IBD 文件或库目录到新 MySQL data 目录。

```sh
mv -f /apps/mysql8.1/data/zuokong /usr/local/src/mysql/data
chown -R 999:999 /usr/local/src/mysql/data/zuokong
chmod 660 /usr/local/src/mysql/data/zuokong/*.ibd
```

4. 确认新 MySQL 容器内部的 `datadir` 能看到这些 `.ibd` 文件，并且 MySQL 进程有读写权限。
5. 回到页面使用同一个恢复对象，点击“导入表空间”。
6. 工具执行 `ALTER TABLE ... IMPORT TABLESPACE` 完成恢复。

整库导入如果中途失败，可以修复问题后再次输入同一个库名执行导入。工具会先检查每张表是否已经可读，已成功导入的表会自动跳过。

大表导入时如果 MySQL 客户端连接断开，工具会等待并重新检查该表是否已经可读。默认检查 6 次，每次间隔 5 秒，可通过 `IMPORT_CONNECTION_RECHECK_ATTEMPTS` 和 `IMPORT_CONNECTION_RECHECK_SECONDS` 调整。

如果连接断开但 MySQL 仍在执行同一张表的 `IMPORT TABLESPACE`，工具会先查询 `performance_schema.processlist` 并等待原导入进程结束，避免重复导入同一张表。默认最多等待 720 次，每次间隔 5 秒，可通过 `IMPORT_PROCESS_WAIT_ATTEMPTS` 和 `IMPORT_PROCESS_WAIT_SECONDS` 调整。

## DDL 同步

服务启动后会立刻同步一次 DDL，之后默认每天 00:00 再同步一次：

```text
DDL_SYNC_ENABLED=true
DDL_SYNC_DAILY_TIME=00:00
DDL_BACKUP_RETENTION_DAYS=31
```

生成的 DDL 固定按库名创建目录，一个表对应一个 SQL 文件：

```text
/usr/local/src/restoredb/ddl/<database>/<table>.sql
```

例如：

```text
/usr/local/src/restoredb/ddl/binance/user.sql
```

每次同步会先生成一份新的临时 DDL。生成成功后：

1. 如果当前 `/usr/local/src/restoredb/ddl` 下存在有效的 `库/表.sql`，先备份到 `/usr/local/src/restoredb/ddl-backup/YYYYmmdd-HHMMSS`
2. 清理超过 `DDL_BACKUP_RETENTION_DAYS` 天的历史备份
3. 把新生成的 DDL 发布为 `/usr/local/src/restoredb/ddl`

如果新 DDL 生成失败，当前 `/usr/local/src/restoredb/ddl` 不会被替换。

如果当前 DDL 目录没有任何有效的数据库和表 SQL 文件，本次同步不会创建空备份。DDL 同步逻辑已经内置在 `server.py` 中，不再依赖 `scripts/export_ddl.sh`。设置 `DDL_SYNC_ENABLED=false` 可以关闭服务内置的同步线程。

## API

- `POST /api/restore/prepare`：建库建表并丢弃表空间。
- `POST /api/restore/import`：导入已经人工放置好的 IBD 表空间。
- `GET /api/tasks/<taskId>`：查看任务状态和日志。

请求体示例：

```json
{
  "restoreTarget": "binance/user.ibd"
}
```

## 说明

这个版本不再依赖 `docker-compose.yml`。`index.html` 是页面入口，`server.py` 负责 HTTP API 和 MySQL 恢复语句执行。
