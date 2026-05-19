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

启动后会把页面发布到：

```text
/usr/local/src/nginx/public/restoredb/index.html
```

外部 nginx 可以直接引用这个文件作为页面入口。

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
  -e PORT=33061 \
  -e RESTORE_ROOT=/usr/local/src/restoredb \
  -e PUBLIC_INDEX_FILE=/usr/local/src/nginx/public/restoredb/index.html \
  -e DDL_DIR=/usr/local/src/restoredb/ddl \
  -e DDL_BACKUP_DIR=/usr/local/src/restoredb/ddl-backup \
  -e RESTORE_OUTPUT_DIR=/usr/local/src/restoredb/restore-jobs \
  -e SYNC_INTERVAL_SECONDS=3600 \
  -v /usr/local/src/restoredb:/usr/local/src/restoredb \
  -v /usr/local/src/nginx/public/restoredb:/usr/local/src/nginx/public/restoredb \
  mysql-restore:latest
```

## 需要准备的东西

- 目标 MySQL 连接信息：`DB_HOST`、`DB_PORT=33060`、`DB_USER`、`DB_PASSWORD`
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
mv /old-mysql-data/binance /new-mysql-data/binance
mv /old-mysql-data/binance/user.ibd /new-mysql-data/binance/user.ibd
```

4. 回到页面使用同一个恢复对象，点击“导入表空间”。
5. 工具执行 `ALTER TABLE ... IMPORT TABLESPACE` 完成恢复。

## DDL 同步

服务启动后会自动同步 DDL，默认每 1 小时执行一次：

```text
SYNC_INTERVAL_SECONDS=3600
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

1. 删除上一次的 `/usr/local/src/restoredb/ddl-backup`
2. 把当前 `/usr/local/src/restoredb/ddl` 移到 `/usr/local/src/restoredb/ddl-backup`
3. 把新生成的 DDL 发布为 `/usr/local/src/restoredb/ddl`

如果新 DDL 生成失败，当前 `/usr/local/src/restoredb/ddl` 不会被替换。

也可以手动执行 [scripts/export_ddl.sh](scripts/export_ddl.sh) 立即同步一次。设置 `SYNC_INTERVAL_SECONDS=0` 可以关闭服务内置的定时同步线程。

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
