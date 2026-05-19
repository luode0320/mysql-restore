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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
APP_INDEX_FILE = ROOT_DIR / "index.html"
RESTORE_ROOT = Path(os.getenv("RESTORE_ROOT", "/usr/local/src/restoredb"))
DDL_ROOT = Path(os.getenv("DDL_DIR", str(RESTORE_ROOT / "ddl")))
RESTORE_OUTPUT_DIR = Path(os.getenv("RESTORE_OUTPUT_DIR", str(RESTORE_ROOT / "restore-jobs")))
DDL_BACKUP_ROOT = Path(os.getenv("DDL_BACKUP_DIR", str(RESTORE_ROOT / "ddl-backup")))
DDL_SYNC_SCRIPT = Path(os.getenv("DDL_SYNC_SCRIPT", str(ROOT_DIR / "scripts" / "export_ddl.sh")))
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "3600"))
MYSQL_HOST = os.getenv("DB_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("DB_PORT", "33060")
MYSQL_USER = os.getenv("DB_USER", "root")
MYSQL_PASSWORD = os.getenv("DB_PASSWORD", "")
MYSQL_BIN = os.getenv("MYSQL_BIN", "mysql")
HTTP_HOST = os.getenv("HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("PORT", "33061"))


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
    cmd = [
        MYSQL_BIN,
        "-h",
        MYSQL_HOST,
        "-P",
        str(MYSQL_PORT),
        "-u",
        MYSQL_USER,
        "--protocol=tcp",
    ]
    return subprocess.run(
        cmd,
        input=sql_text,
        text=True,
        encoding="utf-8",
        capture_output=True,
        env=env,
        check=False,
    )


def run_ddl_sync_once() -> None:
    """执行一次 DDL 同步脚本。

    [参数]
    - 无

    [返回]
    - 无，失败时抛出异常

    最近修改时间: 2026-05-20 00:35:00
    """

    if not DDL_SYNC_SCRIPT.exists():
        raise FileNotFoundError(f"DDL 同步脚本不存在: {DDL_SYNC_SCRIPT}")

    env = os.environ.copy()
    env["RESTORE_ROOT"] = str(RESTORE_ROOT)
    env["DDL_DIR"] = str(DDL_ROOT)
    env["DDL_BACKUP_DIR"] = str(DDL_BACKUP_ROOT)
    env["DB_HOST"] = MYSQL_HOST
    env["DB_PORT"] = str(MYSQL_PORT)
    env["DB_USER"] = MYSQL_USER
    env["DB_PASSWORD"] = MYSQL_PASSWORD

    bash_path = shutil.which("bash")
    if bash_path:
        cmd = [bash_path, str(DDL_SYNC_SCRIPT)]
    else:
        cmd = [str(DDL_SYNC_SCRIPT)]

    print(f"[{time.strftime('%F %T')}] ddl sync command: {' '.join(cmd)}", flush=True)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        env=env,
    )
    assert process.stdout is not None
    for line in process.stdout:
        print(line.rstrip(), flush=True)

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"DDL sync script returned exit code {return_code}")


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
        time.sleep(max(SYNC_INTERVAL_SECONDS, 60))


def start_ddl_sync_worker() -> None:
    """启动 DDL 定时同步后台线程。

    [参数]
    - 无

    [返回]
    - 无

    最近修改时间: 2026-05-20 00:35:00
    """

    if SYNC_INTERVAL_SECONDS <= 0:
        print("DDL sync worker disabled because SYNC_INTERVAL_SECONDS <= 0", flush=True)
        return
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
        stderr = result.stderr.strip() or "mysql client returned non-zero exit code"
        raise RuntimeError(f"{step_name} 失败: {stderr}")
    task.append_log(f"{step_name} -> 完成")


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
    return f"CREATE DATABASE IF NOT EXISTS {database_name};\nUSE {database_name};\n{ddl_text}\n"


def build_discard_sql(database: str, table: str) -> str:
    """构造丢弃表空间 SQL。

    [参数]
    - database: 库名
    - table: 表名

    [返回]
    - ALTER TABLE DISCARD TABLESPACE SQL

    最近修改时间: 2026-05-20 00:20:00
    """

    return f"USE {quote_identifier(database)};\nALTER TABLE {quote_identifier(table)} DISCARD TABLESPACE;"


def build_import_sql(database: str, table: str) -> str:
    """构造导入表空间 SQL。

    [参数]
    - database: 库名
    - table: 表名

    [返回]
    - ALTER TABLE IMPORT TABLESPACE SQL

    最近修改时间: 2026-05-20 00:20:00
    """

    return f"USE {quote_identifier(database)};\nALTER TABLE {quote_identifier(table)} IMPORT TABLESPACE;"


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
        task.append_log(f"导入表 {task.database}.{table_name}")
        mysql_exec(
            build_import_sql(task.database, table_name),
            task,
            f"{task.database}.{table_name} 导入表空间",
        )
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
                    "syncIntervalSeconds": SYNC_INTERVAL_SECONDS,
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
    print(f"mysql-restore listening on http://{HTTP_HOST}:{HTTP_PORT}")
    print(f"DDL root: {DDL_ROOT}")
    print(f"DDL backup root: {DDL_BACKUP_ROOT}")
    print(f"DDL sync interval seconds: {SYNC_INTERVAL_SECONDS}")
    print(f"Restore output: {RESTORE_OUTPUT_DIR}")
    server.serve_forever()


if __name__ == "__main__":
    main()
