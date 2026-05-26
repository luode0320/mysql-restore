#!/usr/bin/env python3
"""mysql-restore 本地控制台服务。

[参数]
- 环境变量 DB_HOST / DB_PORT / DB_USER / DB_PASSWORD: 目标 MySQL 连接信息
- 环境变量 DDL_DIR: 已同步 DDL 的根目录
- 环境变量 RESTORE_OUTPUT_DIR: 恢复任务产物目录
- 环境变量 PORT: HTTP 服务端口

[返回]
- 启动一个可打开 index.html 的本地 HTTP 服务，并提供恢复任务 API

最近修改时间: 2026-05-20 00:20:00
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import subprocess
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
APP_INDEX_FILE = ROOT_DIR / "index.html"
RESTORE_ROOT = Path(os.getenv("RESTORE_ROOT", "/usr/local/src/restoredb"))
DDL_ROOT = Path(os.getenv("DDL_DIR", str(RESTORE_ROOT / "ddl")))
RESTORE_OUTPUT_DIR = Path(os.getenv("RESTORE_OUTPUT_DIR", str(RESTORE_ROOT / "restore-jobs")))
DDL_BACKUP_ROOT = Path(os.getenv("DDL_BACKUP_DIR", str(RESTORE_ROOT / "ddl-backup")))
DDL_SYNC_DAILY_TIME = os.getenv("DDL_SYNC_DAILY_TIME", "00:00")
DDL_BACKUP_RETENTION_DAYS = int(os.getenv("DDL_BACKUP_RETENTION_DAYS", "31"))
DDL_SYNC_ENABLED = os.getenv("DDL_SYNC_ENABLED", "true").lower() not in {"0", "false", "no", "off"}
MYSQL_HOST = os.getenv("DB_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("DB_PORT", "33060")
MYSQL_USER = os.getenv("DB_USER", "root")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD", "")
MYSQL_BIN = os.getenv("MYSQL_BIN", "mysql")
MYSQLDUMP_BIN = os.getenv("MYSQLDUMP_BIN", "mysqldump")
DB_NAME_PATTERN = os.getenv("DB_NAME_PATTERN", "%")
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "DISABLED")
HTTP_HOST = os.getenv("HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("PORT", "33061"))
APP_VERSION = os.getenv("APP_VERSION", "dev")
IMPORT_CONNECTION_RECHECK_SECONDS = int(os.getenv("IMPORT_CONNECTION_RECHECK_SECONDS", "5"))
IMPORT_CONNECTION_RECHECK_ATTEMPTS = int(os.getenv("IMPORT_CONNECTION_RECHECK_ATTEMPTS", "6"))
IMPORT_PROCESS_WAIT_SECONDS = int(os.getenv("IMPORT_PROCESS_WAIT_SECONDS", "5"))
IMPORT_PROCESS_WAIT_ATTEMPTS = int(os.getenv("IMPORT_PROCESS_WAIT_ATTEMPTS", "720"))


@dataclass
class RestoreTarget:
    """页面输入解析后的恢复对象。

    [参数]
    - raw: 页面原始输入
    - database: 需要恢复的库名
    - table: 需要恢复的表名；为空表示恢复整个库

    [返回]
    - 统一后的恢复对象

    最近修改时间: 2026-05-20 00:20:00
    """

    raw: str
    database: str
    table: str | None = None


@dataclass
class RestoreTask:
    """恢复任务状态对象。

    [参数]
    - task_id: 任务编号
    - phase: prepare / import
    - restore_target: 页面输入的库或表对象
    - database: 解析出的库名
    - tables: 本次处理的表名列表

    [返回]
    - 记录任务状态、日志、结果和错误信息

    最近修改时间: 2026-05-20 00:20:00
    """

    task_id: str
    phase: str
    restore_target: str
    database: str
    tables: list[str]
    status: str = "pending"
    logs: list[str] = field(default_factory=list)
    result: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    def append_log(self, message: str) -> None:
        """追加一条带时间戳的任务日志。

        [参数]
        - message: 日志文本

        [返回]
        - 无

        最近修改时间: 2026-05-20 00:20:00
        """

        stamp = time.strftime("%F %T")
        line = f"[{stamp}] {message}"
        with self.lock:
            self.logs.append(line)
            self.updated_at = time.time()

    def set_status(self, status: str) -> None:
        """更新任务状态。

        [参数]
        - status: pending / running / success / failed

        [返回]
        - 无

        最近修改时间: 2026-05-20 00:20:00
        """

        with self.lock:
            self.status = status
            self.updated_at = time.time()

    def snapshot(self) -> dict[str, Any]:
        """导出任务快照。

        [参数]
        - 无

        [返回]
        - 可直接 JSON 序列化的任务视图

        最近修改时间: 2026-05-20 00:20:00
        """

        with self.lock:
            return {
                "id": self.task_id,
                "phase": self.phase,
                "restoreTarget": self.restore_target,
                "database": self.database,
                "tables": list(self.tables),
                "status": self.status,
                "logs": list(self.logs),
                "result": dict(self.result),
                "error": self.error,
                "createdAt": self.created_at,
                "updatedAt": self.updated_at,
            }


TASKS: dict[str, RestoreTask] = {}
TASK_LOCK = threading.Lock()


def ensure_runtime_dirs() -> None:
    """初始化运行目录。

    [参数]
    - 无

    [返回]
    - 无

    最近修改时间: 2026-05-20 00:20:00
    """

    RESTORE_ROOT.mkdir(parents=True, exist_ok=True)
    DDL_ROOT.mkdir(parents=True, exist_ok=True)
    DDL_BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    RESTORE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def json_response(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status_code: int = 200) -> None:
    """发送 JSON 响应。

    [参数]
    - handler: HTTP 处理器
    - payload: 响应体
    - status_code: HTTP 状态码

    [返回]
    - 无

    最近修改时间: 2026-05-20 00:20:00
    """

    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(
    handler: BaseHTTPRequestHandler,
    text: str,
    status_code: int = 200,
    content_type: str = "text/plain; charset=utf-8",
) -> None:
    """发送文本响应。

    [参数]
    - handler: HTTP 处理器
    - text: 响应文本
    - status_code: HTTP 状态码
    - content_type: 响应类型

    [返回]
    - 无

    最近修改时间: 2026-05-20 00:20:00
    """

    body = text.encode("utf-8")
    handler.send_response(status_code)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def read_json(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    """读取请求体 JSON。

    [参数]
    - handler: HTTP 处理器

    [返回]
    - 解析后的 JSON 字典

    最近修改时间: 2026-05-20 00:20:00
    """

    length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(length)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def quote_identifier(value: str) -> str:
    """转义 MySQL 标识符。

    [参数]
    - value: 库名或表名

    [返回]
    - 可放入 SQL 的反引号标识符

    最近修改时间: 2026-05-20 00:20:00
    """

    return f"`{value.replace('`', '``')}`"


def validate_name(value: str, label: str) -> str:
    """校验页面输入中的库名或表名片段。

    [参数]
    - value: 待校验名称
    - label: 错误提示中的字段名

    [返回]
    - 原始名称

    最近修改时间: 2026-05-20 00:20:00
    """

    if not value or value in {".", ".."}:
        raise ValueError(f"{label}不能为空")
    if "/" in value or "\\" in value or "\x00" in value:
        raise ValueError(f"{label}不能包含路径分隔符")
    return value


def parse_restore_target(value: str) -> RestoreTarget:
    """解析页面输入的恢复对象。

    [参数]
    - value: 例如 binance 或 binance/user.ibd

    [返回]
    - RestoreTarget

    最近修改时间: 2026-05-20 00:20:00
    """

    raw = value.strip().strip("/")
    if not raw:
        raise ValueError("恢复对象不能为空")
    if raw.startswith(".") or raw.startswith("\\") or ":" in raw:
        raise ValueError("恢复对象只允许填写库名或 库名/表名.ibd，不要填写宿主机绝对路径")

    parts = [item for item in raw.replace("\\", "/").split("/") if item]
    if len(parts) == 1:
        database = validate_name(parts[0], "库名")
        return RestoreTarget(raw=raw, database=database)
    if len(parts) == 2:
        database = validate_name(parts[0], "库名")
        table = parts[1]
        if table.lower().endswith(".ibd"):
            table = table[:-4]
        table = validate_name(table, "表名")
        return RestoreTarget(raw=raw, database=database, table=table)
    raise ValueError("恢复对象格式错误，请填写 binance 或 binance/user.ibd")


def detect_ssl_args(client_bin: str) -> list[str]:
    """根据客户端能力生成 SSL 参数。

    [参数]
    - client_bin: mysql 或 mysqldump 可执行文件

    [返回]
    - 当前客户端支持的 SSL 选项

    最近修改时间: 2026-05-20 14:20:00
    """

    if not DB_SSL_MODE:
        return []
    try:
        help_result = subprocess.run(
            [client_bin, "--help"],
            text=True,
            encoding="utf-8",
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return []
    help_text = f"{help_result.stdout}\n{help_result.stderr}"
    normalized_mode = DB_SSL_MODE.upper()
    if "--ssl-mode" in help_text:
        return [f"--ssl-mode={normalized_mode}"]
    if normalized_mode == "DISABLED" and "--ssl=0" in help_text:
        return ["--ssl=0"]
    if normalized_mode == "DISABLED" and "--skip-ssl" in help_text:
        return ["--skip-ssl"]
    return []


def build_mysql_base_args(client_bin: str) -> list[str]:
    """构造 mysql 客户端基础连接参数。

    [参数]
    - client_bin: mysql 或 mysqldump 可执行文件

    [返回]
    - 基础命令参数列表

    最近修改时间: 2026-05-20 14:20:00
    """

    return [
        client_bin,
        "-h",
        MYSQL_HOST,
        "-P",
        str(MYSQL_PORT),
        "-u",
        MYSQL_USER,
        "--protocol=tcp",
        *detect_ssl_args(client_bin),
    ]


def mysql_run(sql_text: str) -> subprocess.CompletedProcess[str]:
    """执行一段 SQL。

    [参数]
    - sql_text: 要发送给 mysql 客户端的 SQL 文本

    [返回]
    - subprocess.CompletedProcess 对象

    最近修改时间: 2026-05-20 00:20:00
    """

    env = os.environ.copy()
    env["MYSQL_PWD"] = MYSQL_PASSWORD
    cmd = build_mysql_base_args(MYSQL_BIN)
    return subprocess.run(
        cmd,
        input=sql_text,
        text=True,
        encoding="utf-8",
        capture_output=True,
        env=env,
        check=False,
    )


def mysql_query_lines(sql_text: str) -> list[str]:
    """执行查询 SQL 并按行返回结果。

    [参数]
    - sql_text: 查询 SQL 文本

    [返回]
    - 去掉空行后的结果行列表

    最近修改时间: 2026-05-20 01:10:00
    """

    env = os.environ.copy()
    env["MYSQL_PWD"] = MYSQL_PASSWORD
    cmd = [
        *build_mysql_base_args(MYSQL_BIN),
        "--batch",
        "--skip-column-names",
        "-e",
        sql_text,
    ]
    result = subprocess.run(
        cmd,
        text=True,
        encoding="utf-8",
        capture_output=True,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "mysql query returned non-zero exit code"
        raise RuntimeError(stderr)
    if result.stderr.strip():
        print(result.stderr.strip(), flush=True)
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def escape_sql_string(value: str) -> str:
    """转义 SQL 字符串字面量。

    [参数]
    - value: 原始字符串

    [返回]
    - 可放入单引号字符串的内容

    最近修改时间: 2026-05-20 01:10:00
    """

    return value.replace("\\", "\\\\").replace("'", "''")


def build_mysqldump_args() -> list[str]:
    """构造当前 mysqldump 支持的参数列表。

    [参数]
    - 无

    [返回]
    - mysqldump 基础参数

    最近修改时间: 2026-05-20 01:10:00
    """

    args = [
        *build_mysql_base_args(MYSQLDUMP_BIN),
        "--no-data",
        "--skip-lock-tables",
        "--single-transaction",
        "--routines=false",
        "--events=false",
        "--triggers",
    ]
    help_result = subprocess.run(
        [MYSQLDUMP_BIN, "--help"],
        text=True,
        encoding="utf-8",
        capture_output=True,
        check=False,
    )
    help_text = f"{help_result.stdout}\n{help_result.stderr}"
    if "--set-gtid-purged" in help_text:
        args.append("--set-gtid-purged=OFF")
    else:
        print(f"[{time.strftime('%F %T')}] mysqldump does not support --set-gtid-purged, skip it", flush=True)
    if "--column-statistics" in help_text:
        args.append("--column-statistics=0")
    else:
        print(f"[{time.strftime('%F %T')}] mysqldump does not support --column-statistics, skip it", flush=True)
    return args


def dump_table_ddl(dump_args: list[str], database: str, table: str, output_file: Path) -> None:
    """导出单表 DDL 到目标文件。

    [参数]
    - dump_args: mysqldump 基础参数
    - database: 库名
    - table: 表名
    - output_file: 输出 SQL 文件

    [返回]
    - 无，失败时抛出异常

    最近修改时间: 2026-05-20 01:10:00
    """

    env = os.environ.copy()
    env["MYSQL_PWD"] = MYSQL_PASSWORD
    cmd = [*dump_args, database, table]
    with output_file.open("w", encoding="utf-8") as fp:
        result = subprocess.run(
            cmd,
            stdout=fp,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=env,
            check=False,
        )
    if result.returncode != 0:
        if output_file.exists():
            output_file.unlink()
        stderr = result.stderr.strip() or "mysqldump returned non-zero exit code"
        raise RuntimeError(stderr)
    if result.stderr.strip():
        print(result.stderr.strip(), flush=True)


def has_valid_ddl(root: Path) -> bool:
    """Return whether a DDL root contains at least one database/table SQL file."""

    if not root.exists() or not root.is_dir():
        return False
    return any(sql_file.is_file() for sql_file in root.glob("*/*.sql"))


def build_backup_dir(now: datetime | None = None) -> Path:
    """Build a unique timestamped DDL backup directory path."""

    backup_time = now or datetime.now()
    base_name = backup_time.strftime("%Y%m%d-%H%M%S")
    backup_dir = DDL_BACKUP_ROOT / base_name
    index = 1
    while backup_dir.exists():
        backup_dir = DDL_BACKUP_ROOT / f"{base_name}-{index}"
        index += 1
    return backup_dir


def backup_current_ddl() -> None:
    """Copy current DDL into a timestamped backup directory when it is valid."""

    DDL_BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    if not has_valid_ddl(DDL_ROOT):
        print(f"[{time.strftime('%F %T')}] current ddl has no valid database/table sql, skip backup", flush=True)
        return

    backup_dir = build_backup_dir()
    print(f"[{time.strftime('%F %T')}] backup current ddl to: {backup_dir}", flush=True)
    shutil.copytree(DDL_ROOT, backup_dir)


def cleanup_old_ddl_backups() -> None:
    """Remove timestamped DDL backups older than the configured retention days."""

    if DDL_BACKUP_RETENTION_DAYS <= 0:
        print(f"[{time.strftime('%F %T')}] ddl backup retention disabled, skip cleanup", flush=True)
        return
    if not DDL_BACKUP_ROOT.exists():
        return

    expire_before = datetime.now() - timedelta(days=DDL_BACKUP_RETENTION_DAYS)
    removed_count = 0
    for backup_dir in DDL_BACKUP_ROOT.iterdir():
        if not backup_dir.is_dir():
            continue
        try:
            backup_time = datetime.strptime(backup_dir.name[:15], "%Y%m%d-%H%M%S")
        except ValueError:
            backup_time = datetime.fromtimestamp(backup_dir.stat().st_mtime)
        if backup_time < expire_before:
            shutil.rmtree(backup_dir)
            removed_count += 1

    print(f"[{time.strftime('%F %T')}] old ddl backup cleanup removed: {removed_count}", flush=True)


def publish_synced_ddl(tmp_dir: Path) -> None:
    """发布新 DDL 并刷新旧 DDL 备份。

    [参数]
    - tmp_dir: 已生成完成的新 DDL 临时目录

    [返回]
    - 无

    最近修改时间: 2026-05-20 01:10:00
    """

    print(f"[{time.strftime('%F %T')}] refresh ddl backup", flush=True)
    backup_current_ddl()
    cleanup_old_ddl_backups()

    if DDL_ROOT.exists():
        shutil.rmtree(DDL_ROOT)
    print(f"[{time.strftime('%F %T')}] publish new ddl: {DDL_ROOT}", flush=True)
    tmp_dir.replace(DDL_ROOT)


def run_ddl_sync_once() -> None:
    """执行一次 DDL 同步。

    [参数]
    - 无

    [返回]
    - 无，失败时抛出异常

    最近修改时间: 2026-05-20 01:10:00
    """

    tmp_dir = DDL_ROOT.with_name(f"{DDL_ROOT.name}.sync-tmp.{os.getpid()}.{int(time.time())}")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    try:
        print(f"[{time.strftime('%F %T')}] query database list with pattern: {DB_NAME_PATTERN}", flush=True)
        database_sql = f"""
            SELECT SCHEMA_NAME
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME LIKE '{escape_sql_string(DB_NAME_PATTERN)}'
              AND SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY SCHEMA_NAME;
        """
        databases = mysql_query_lines(database_sql)
        if not databases:
            print(f"[{time.strftime('%F %T')}] no databases matched pattern: {DB_NAME_PATTERN}", flush=True)
            shutil.rmtree(tmp_dir)
            return

        print(f"[{time.strftime('%F %T')}] matched database count: {len(databases)}", flush=True)
        print(f"[{time.strftime('%F %T')}] start ddl sync from {MYSQL_HOST}:{MYSQL_PORT}", flush=True)
        dump_args = build_mysqldump_args()
        for database in databases:
            db_dir = tmp_dir / database
            db_dir.mkdir(parents=True, exist_ok=True)
            print(f"[{time.strftime('%F %T')}] sync database: {database}", flush=True)
            table_sql = f"""
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = '{escape_sql_string(database)}'
                  AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME;
            """
            tables = mysql_query_lines(table_sql)
            print(f"[{time.strftime('%F %T')}] database {database} table count: {len(tables)}", flush=True)
            for table in tables:
                output_file = db_dir / f"{table}.sql"
                print(f"[{time.strftime('%F %T')}] dump table: {database}.{table} -> {output_file}", flush=True)
                dump_table_ddl(dump_args, database, table, output_file)

        publish_synced_ddl(tmp_dir)
        print(f"[{time.strftime('%F %T')}] ddl sync done", flush=True)
    except Exception:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        raise


def parse_daily_sync_time() -> tuple[int, int]:
    """Parse DDL_SYNC_DAILY_TIME into hour and minute."""

    try:
        hour_text, minute_text = DDL_SYNC_DAILY_TIME.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except ValueError as exc:
        raise ValueError(f"invalid DDL_SYNC_DAILY_TIME: {DDL_SYNC_DAILY_TIME}") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"invalid DDL_SYNC_DAILY_TIME: {DDL_SYNC_DAILY_TIME}")
    return hour, minute


def seconds_until_next_daily_sync() -> tuple[float, datetime]:
    """Calculate seconds until the next configured daily DDL sync time."""

    hour, minute = parse_daily_sync_time()
    now = datetime.now()
    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    return max((next_run - now).total_seconds(), 1), next_run


def ddl_sync_loop() -> None:
    """按配置间隔循环同步 DDL。

    [参数]
    - 无

    [返回]
    - 无

    最近修改时间: 2026-05-20 00:35:00
    """

    while True:
        try:
            run_ddl_sync_once()
        except Exception as exc:  # noqa: BLE001
            print(f"[{time.strftime('%F %T')}] ddl sync failed: {exc}", flush=True)
        wait_seconds, next_run = seconds_until_next_daily_sync()
        print(
            f"[{time.strftime('%F %T')}] next ddl sync scheduled at {next_run.strftime('%F %T')}",
            flush=True,
        )
        time.sleep(wait_seconds)


def start_ddl_sync_worker() -> None:
    """启动 DDL 定时同步后台线程。

    [参数]
    - 无

    [返回]
    - 无

    最近修改时间: 2026-05-20 00:35:00
    """

    if not DDL_SYNC_ENABLED:
        print("DDL sync worker disabled because DDL_SYNC_ENABLED is false", flush=True)
        return
    parse_daily_sync_time()
    worker = threading.Thread(target=ddl_sync_loop, daemon=True)
    worker.start()


def mysql_exec(sql_text: str, task: RestoreTask, step_name: str) -> None:
    """执行 SQL 并记录结果。

    [参数]
    - sql_text: SQL 文本
    - task: 当前恢复任务
    - step_name: 该 SQL 的步骤描述

    [返回]
    - 无，失败时抛出异常

    最近修改时间: 2026-05-20 00:20:00
    """

    task.append_log(f"{step_name} -> 发送到 MySQL")
    result = mysql_run(sql_text)
    if result.stdout.strip():
        task.append_log(result.stdout.strip())
    if result.returncode != 0:
        if "FOREIGN_KEY_CHECKS=0" in sql_text:
            mysql_run("SET FOREIGN_KEY_CHECKS=1;")
        stderr = result.stderr.strip() or "mysql client returned non-zero exit code"
        raise RuntimeError(f"{step_name} 失败: {stderr}")
    task.append_log(f"{step_name} -> 完成")


def build_import_error_hint(database: str, table: str, stderr: str) -> str:
    """根据导入表空间错误生成排查提示。

    [参数]
    - database: 库名
    - table: 表名
    - stderr: mysql 客户端错误输出

    [返回]
    - 面向页面日志的中文排查提示

    最近修改时间: 2026-05-20 01:25:00
    """

    if "ERROR 2013" in stderr or "Lost connection to server during query" in stderr:
        return "\n".join(
            [
                f"排查提示: {database}.{table} 导入过程中 MySQL 连接断开。",
                "这常见于大表 IMPORT TABLESPACE 执行时间较长、MySQL 重启或网络连接被中断。",
                "工具会在断连后重新检查该表是否已经可读；如果不可读，可以修复 MySQL 状态后重新执行整库导入，已成功的表会自动跳过。",
            ]
        )

    if "ERROR 2026" in stderr or "TLS/SSL error" in stderr:
        return "\n".join(
            [
                f"排查提示: {database}.{table} 导入时发生 TLS/SSL 连接错误。",
                "请确认部署参数包含 DB_SSL_MODE=DISABLED，并重新构建部署最新镜像。",
                "修复连接问题后重新执行整库导入，已成功的表会自动跳过。",
            ]
        )

    if "ERROR 1812" not in stderr and "Tablespace is missing" not in stderr:
        return ""

    return "\n".join(
        [
            f"排查提示: MySQL 没有在目标 data 目录中找到 {database}/{table}.ibd。",
            "请确认已经先执行“准备恢复”，再手动移动原始 IBD，最后才执行“导入表空间”。",
            f"请在新 MySQL 容器或宿主机 data 卷中检查文件是否存在: <mysql-data>/{database}/{table}.ibd",
            "如果文件存在，请检查文件属主和权限是否允许 MySQL 进程读取，例如 mysql:mysql。",
            "如果是整库恢复，请确认 DDL 目录中的每张表都已经放入对应的 .ibd 文件；缺少任意一张表都会导致导入停在该表。",
        ]
    )


def read_sql_file(path: Path) -> str:
    """读取 DDL 文件内容，兼容压缩归档。

    [参数]
    - path: SQL 或 SQL.gz 文件路径

    [返回]
    - SQL 文本

    最近修改时间: 2026-05-20 00:20:00
    """

    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fp:
            return fp.read()
    return path.read_text(encoding="utf-8-sig")


def resolve_ddl_file(database: str, table: str) -> Path:
    """定位表对应的 DDL 文件。

    [参数]
    - database: 库名
    - table: 表名

    [返回]
    - DDL 文件路径

    最近修改时间: 2026-05-20 00:20:00
    """

    direct = DDL_ROOT / database / f"{table}.sql"
    if direct.exists():
        return direct

    latest = DDL_ROOT / "latest" / database / f"{table}.sql"
    if latest.exists():
        return latest

    archive_root = DDL_ROOT / "archive" / database
    if archive_root.exists():
        candidates = sorted(
            archive_root.rglob(f"{table}.sql.gz"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            return candidates[0]

    raise FileNotFoundError(f"未找到 {database}.{table} 的 DDL 文件")


def list_database_tables(database: str) -> list[str]:
    """根据 DDL 目录列出一个库下的表。

    [参数]
    - database: 库名

    [返回]
    - 表名列表

    最近修改时间: 2026-05-20 00:20:00
    """

    database_dir = DDL_ROOT / database
    if not database_dir.exists():
        raise FileNotFoundError(f"未找到库 {database} 的 DDL 目录: {database_dir}")
    tables = sorted(path.stem for path in database_dir.glob("*.sql") if path.is_file())
    if not tables:
        raise FileNotFoundError(f"库 {database} 的 DDL 目录中没有表 SQL 文件")
    return tables


def resolve_tables(target: RestoreTarget) -> list[str]:
    """解析本次恢复需要处理的表列表。

    [参数]
    - target: 页面输入解析后的恢复对象

    [返回]
    - 表名列表

    最近修改时间: 2026-05-20 00:20:00
    """

    if target.table:
        resolve_ddl_file(target.database, target.table)
        return [target.table]
    return list_database_tables(target.database)


def build_create_table_sql(database: str, ddl_text: str) -> str:
    """构造创建库和创建表的 SQL。

    [参数]
    - database: 库名
    - ddl_text: 表结构 SQL

    [返回]
    - 可直接执行的 SQL 文本

    最近修改时间: 2026-05-20 00:20:00
    """

    database_name = quote_identifier(database)
    return f"SET FOREIGN_KEY_CHECKS=0;\nCREATE DATABASE IF NOT EXISTS {database_name};\nUSE {database_name};\n{ddl_text}\nSET FOREIGN_KEY_CHECKS=1;\n"


def build_discard_sql(database: str, table: str) -> str:
    """构造丢弃表空间 SQL。

    [参数]
    - database: 库名
    - table: 表名

    [返回]
    - ALTER TABLE DISCARD TABLESPACE SQL

    最近修改时间: 2026-05-20 00:20:00
    """

    return f"USE {quote_identifier(database)};\nSET FOREIGN_KEY_CHECKS=0;\nALTER TABLE {quote_identifier(table)} DISCARD TABLESPACE;\nSET FOREIGN_KEY_CHECKS=1;"


def build_import_sql(database: str, table: str) -> str:
    """构造导入表空间 SQL。

    [参数]
    - database: 库名
    - table: 表名

    [返回]
    - ALTER TABLE IMPORT TABLESPACE SQL

    最近修改时间: 2026-05-20 00:20:00
    """

    return f"USE {quote_identifier(database)};\nSET FOREIGN_KEY_CHECKS=0;\nALTER TABLE {quote_identifier(table)} IMPORT TABLESPACE;\nSET FOREIGN_KEY_CHECKS=1;"


def build_table_probe_sql(database: str, table: str) -> str:
    """构造表可读性探测 SQL。

    [参数]
    - database: 库名
    - table: 表名

    [返回]
    - SELECT 探测 SQL

    最近修改时间: 2026-05-20 14:40:00
    """

    return f"SELECT 1 FROM {quote_identifier(database)}.{quote_identifier(table)} LIMIT 1;"


def is_table_already_imported(database: str, table: str, task: RestoreTask) -> bool:
    """判断表是否已经成功导入且可读。

    [参数]
    - database: 库名
    - table: 表名
    - task: 当前恢复任务

    [返回]
    - True 表示表已经可读，可以跳过导入

    最近修改时间: 2026-05-20 14:40:00
    """

    result = mysql_run(build_table_probe_sql(database, table))
    if result.returncode == 0:
        return True

    stderr = result.stderr.strip()
    if stderr:
        task.append_log(f"{database}.{table} 可读性检查未通过，将尝试导入: {stderr}")
    return False


def find_running_import_processes(database: str, table: str) -> list[str]:
    """查询正在执行的同表导入表空间进程。

    [参数]
    - database: 库名
    - table: 表名

    [返回]
    - processlist 描述行列表

    最近修改时间: 2026-05-20 15:10:00
    """

    table_token = f"ALTER TABLE `{table}` IMPORT TABLESPACE"
    fallback_token = f"ALTER TABLE {table} IMPORT TABLESPACE"
    sql = f"""
        SELECT CONCAT(ID, '|', DB, '|', TIME, '|', STATE, '|', INFO)
        FROM performance_schema.processlist
        WHERE COMMAND = 'Query'
          AND DB = '{escape_sql_string(database)}'
          AND (
            INFO LIKE '%{escape_sql_string(table_token)}%'
            OR INFO LIKE '%{escape_sql_string(fallback_token)}%'
            OR (INFO LIKE '%IMPORT TABLESPACE%' AND INFO LIKE '%{escape_sql_string(table)}%')
          )
        ORDER BY TIME DESC;
    """
    try:
        return mysql_query_lines(sql)
    except Exception as exc:  # noqa: BLE001
        print(f"[{time.strftime('%F %T')}] query running import process failed: {exc}", flush=True)
        return []


def wait_for_running_import_process(database: str, table: str, task: RestoreTask) -> bool:
    """等待同表正在执行的导入进程结束。

    [参数]
    - database: 库名
    - table: 表名
    - task: 当前恢复任务

    [返回]
    - True 表示曾经发现并等待了导入进程

    最近修改时间: 2026-05-20 15:10:00
    """

    waited = False
    for attempt in range(1, IMPORT_PROCESS_WAIT_ATTEMPTS + 1):
        processes = find_running_import_processes(database, table)
        if not processes:
            return waited
        waited = True
        task.append_log(
            f"{database}.{table} 已有 IMPORT TABLESPACE 正在执行，等待 {IMPORT_PROCESS_WAIT_SECONDS}s 后第 {attempt}/{IMPORT_PROCESS_WAIT_ATTEMPTS} 次复查: {processes[0]}"
        )
        time.sleep(IMPORT_PROCESS_WAIT_SECONDS)
    task.append_log(f"{database}.{table} 等待已有 IMPORT TABLESPACE 超时，将停止本次任务以避免重复导入")
    raise RuntimeError(f"{database}.{table} 已有 IMPORT TABLESPACE 长时间未结束")


def wait_for_table_import_after_disconnect(database: str, table: str, task: RestoreTask) -> bool:
    """导入断连后等待并确认表是否已经可读。

    [参数]
    - database: 库名
    - table: 表名
    - task: 当前恢复任务

    [返回]
    - True 表示断连后确认表已经可读

    最近修改时间: 2026-05-20 14:55:00
    """

    wait_for_running_import_process(database, table, task)

    for attempt in range(1, IMPORT_CONNECTION_RECHECK_ATTEMPTS + 1):
        task.append_log(
            f"{database}.{table} 导入连接断开，等待 {IMPORT_CONNECTION_RECHECK_SECONDS}s 后第 {attempt}/{IMPORT_CONNECTION_RECHECK_ATTEMPTS} 次检查表是否可读"
        )
        time.sleep(IMPORT_CONNECTION_RECHECK_SECONDS)
        if is_table_already_imported(database, table, task):
            task.append_log(f"{database}.{table} 断连后检查已可读，视为导入成功")
            return True
    return False


def create_restore_artifact(task: RestoreTask, table_name: str, ddl_path: Path) -> None:
    """记录单表恢复产物，方便追溯。

    [参数]
    - task: 恢复任务
    - table_name: 表名
    - ddl_path: DDL 文件

    [返回]
    - 无

    最近修改时间: 2026-05-20 00:20:00
    """

    artifact_dir = RESTORE_OUTPUT_DIR / task.task_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    plan_file = artifact_dir / f"{task.database}_{table_name}_{task.phase}.json"
    plan = {
        "phase": task.phase,
        "restoreTarget": task.restore_target,
        "database": task.database,
        "table": table_name,
        "ddlPath": str(ddl_path),
        "mysqlHost": MYSQL_HOST,
        "mysqlPort": MYSQL_PORT,
        "manualIbdStep": f"请人工把 {task.database}/{table_name}.ibd 放到新 MySQL data 目录对应库目录后，再执行导入表空间",
    }
    plan_file.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")


def run_prepare_task(task: RestoreTask) -> None:
    """执行准备阶段：建库建表并丢弃表空间。

    [参数]
    - task: 恢复任务

    [返回]
    - 无，失败时抛出异常

    最近修改时间: 2026-05-20 00:20:00
    """

    for table_name in task.tables:
        ddl_path = resolve_ddl_file(task.database, table_name)
        task.append_log(f"准备表 {task.database}.{table_name}")
        ddl_text = read_sql_file(ddl_path)
        mysql_exec(
            build_create_table_sql(task.database, ddl_text),
            task,
            f"{task.database}.{table_name} 创建库表",
        )
        mysql_exec(
            build_discard_sql(task.database, table_name),
            task,
            f"{task.database}.{table_name} 丢弃表空间",
        )
        create_restore_artifact(task, table_name, ddl_path)


def run_import_task(task: RestoreTask) -> None:
    """执行导入阶段：导入已经人工放置好的 IBD 表空间。

    [参数]
    - task: 恢复任务

    [返回]
    - 无，失败时抛出异常

    最近修改时间: 2026-05-20 00:20:00
    """

    for table_name in task.tables:
        ddl_path = resolve_ddl_file(task.database, table_name)
        if wait_for_running_import_process(task.database, table_name, task):
            if is_table_already_imported(task.database, table_name, task):
                task.append_log(f"跳过表 {task.database}.{table_name}: 已有导入进程结束后表可读，视为已成功导入")
                create_restore_artifact(task, table_name, ddl_path)
                continue

        if is_table_already_imported(task.database, table_name, task):
            task.append_log(f"跳过表 {task.database}.{table_name}: 表已经可读，视为已成功导入")
            create_restore_artifact(task, table_name, ddl_path)
            continue

        task.append_log(f"导入表 {task.database}.{table_name}")
        try:
            mysql_exec(
                build_import_sql(task.database, table_name),
                task,
                f"{task.database}.{table_name} 导入表空间",
            )
        except RuntimeError as exc:
            error_text = str(exc)
            hint = build_import_error_hint(task.database, table_name, str(exc))
            if hint:
                task.append_log(hint)
            if "ERROR 2013" in error_text or "Lost connection to server during query" in error_text:
                if wait_for_table_import_after_disconnect(task.database, table_name, task):
                    create_restore_artifact(task, table_name, ddl_path)
                    continue
            raise
        create_restore_artifact(task, table_name, ddl_path)


def run_restore_task(task: RestoreTask) -> None:
    """执行恢复任务主流程。

    [参数]
    - task: 任务对象

    [返回]
    - 无，失败时会更新任务状态并记录错误

    最近修改时间: 2026-05-20 00:20:00
    """

    try:
        task.set_status("running")
        task.append_log(f"开始{task.phase}任务")
        task.append_log(f"恢复对象: {task.restore_target}")
        task.append_log(f"数据库: {task.database}")
        task.append_log(f"待处理表数量: {len(task.tables)}")

        if task.phase == "prepare":
            run_prepare_task(task)
            task.append_log("准备阶段完成。现在请人工移动 IBD 文件或库目录到新 MySQL data 目录，再执行导入。")
        elif task.phase == "import":
            run_import_task(task)
        else:
            raise ValueError(f"未知恢复阶段: {task.phase}")

        task.result = {
            "phase": task.phase,
            "database": task.database,
            "tables": list(task.tables),
            "tableCount": len(task.tables),
        }
        task.set_status("success")
        task.append_log(f"{task.phase}任务完成")
    except Exception as exc:  # noqa: BLE001
        task.error = f"{exc}"
        task.set_status("failed")
        task.append_log(f"{task.phase}任务失败: {exc}")
        task.append_log(traceback.format_exc())


def create_task(phase: str, restore_target: str) -> RestoreTask:
    """创建并启动一个恢复任务。

    [参数]
    - phase: prepare / import
    - restore_target: 页面输入的库或表对象

    [返回]
    - 新建任务对象

    最近修改时间: 2026-05-20 00:20:00
    """

    if phase not in {"prepare", "import"}:
        raise ValueError("phase 只能是 prepare 或 import")

    target = parse_restore_target(restore_target)
    tables = resolve_tables(target)
    task = RestoreTask(
        task_id=uuid.uuid4().hex[:12],
        phase=phase,
        restore_target=target.raw,
        database=target.database,
        tables=tables,
    )
    with TASK_LOCK:
        TASKS[task.task_id] = task

    worker = threading.Thread(target=run_restore_task, args=(task,), daemon=True)
    worker.start()
    return task


class RestoreHandler(BaseHTTPRequestHandler):
    """HTTP 控制器。

    [参数]
    - BaseHTTPRequestHandler 的标准参数

    [返回]
    - 提供页面和 API

    最近修改时间: 2026-05-20 00:20:00
    """

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """关闭默认访问日志。

        [参数]
        - format: 日志格式
        - *args: 日志参数

        [返回]
        - 无

        最近修改时间: 2026-05-20 00:20:00
        """

    def do_GET(self) -> None:  # noqa: N802
        """处理 GET 请求。

        [参数]
        - 无

        [返回]
        - 无

        最近修改时间: 2026-05-20 00:20:00
        """

        if self.path == "/" or self.path == "/index.html":
            text_response(self, APP_INDEX_FILE.read_text(encoding="utf-8"), content_type="text/html; charset=utf-8")
            return

        if self.path == "/api/config":
            json_response(
                self,
                {
                    "dbHost": MYSQL_HOST,
                    "dbPort": MYSQL_PORT,
                    "dbUser": MYSQL_USER,
                    "restoreRoot": str(RESTORE_ROOT),
                    "indexUrl": f"http://{HTTP_HOST}:{HTTP_PORT}/",
                    "ddlRoot": str(DDL_ROOT),
                    "ddlBackupRoot": str(DDL_BACKUP_ROOT),
                    "restoreOutputDir": str(RESTORE_OUTPUT_DIR),
                    "ddlSyncEnabled": DDL_SYNC_ENABLED,
                    "ddlSyncDailyTime": DDL_SYNC_DAILY_TIME,
                    "ddlBackupRetentionDays": DDL_BACKUP_RETENTION_DAYS,
                },
            )
            return

        if self.path.startswith("/api/tasks/"):
            task_id = self.path.rsplit("/", 1)[-1]
            with TASK_LOCK:
                task = TASKS.get(task_id)
            if not task:
                json_response(self, {"error": "task not found"}, 404)
                return
            json_response(self, task.snapshot())
            return

        json_response(self, {"error": "not found"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        """处理 POST 请求。

        [参数]
        - 无

        [返回]
        - 无

        最近修改时间: 2026-05-20 00:20:00
        """

        phase_by_path = {
            "/api/restore/prepare": "prepare",
            "/api/restore/import": "import",
        }
        if self.path not in phase_by_path:
            json_response(self, {"error": "not found"}, 404)
            return

        try:
            payload = read_json(self)
            restore_target = str(payload.get("restoreTarget", "")).strip()
            if not restore_target:
                json_response(self, {"error": "restoreTarget 不能为空"}, 400)
                return

            task = create_task(phase_by_path[self.path], restore_target)
            json_response(
                self,
                {
                    "taskId": task.task_id,
                    "phase": task.phase,
                    "status": task.status,
                    "restoreTarget": task.restore_target,
                    "database": task.database,
                    "tables": task.tables,
                },
                202,
            )
        except Exception as exc:  # noqa: BLE001
            json_response(self, {"error": str(exc)}, 400)


def main() -> None:
    """服务入口。

    [参数]
    - 无

    [返回]
    - 无

    最近修改时间: 2026-05-20 00:20:00
    """

    ensure_runtime_dirs()
    start_ddl_sync_worker()
    server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), RestoreHandler)
    print(f"mysql-restore version: {APP_VERSION}")
    print(f"mysql-restore listening on http://{HTTP_HOST}:{HTTP_PORT}")
    print(f"DDL root: {DDL_ROOT}")
    print(f"DDL backup root: {DDL_BACKUP_ROOT}")
    print(f"DDL sync enabled: {DDL_SYNC_ENABLED}")
    print(f"DDL sync on startup: true")
    print(f"DDL sync daily time: {DDL_SYNC_DAILY_TIME}")
    print(f"DDL backup retention days: {DDL_BACKUP_RETENTION_DAYS}")
    print(f"Restore output: {RESTORE_OUTPUT_DIR}")
    server.serve_forever()


if __name__ == "__main__":
    main()
