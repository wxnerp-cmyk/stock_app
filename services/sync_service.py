# -*- coding: utf-8 -*-
"""
同步服务 - 后台定时拉取期货/期权到期日数据写入 MySQL
"""
import threading
import time
import traceback
from datetime import datetime


class SyncService:
    """后台同步管理器，支持手动/自动触发，进度追踪，状态查询"""

    def __init__(self):
        self._lock = threading.Lock()
        self._states = {}  # key -> dict

    def _init_state(self, key):
        with self._lock:
            if key not in self._states:
                self._states[key] = {
                    "running": False,
                    "last_result": None,
                    "last_error": "",
                    "last_time": None,
                    "total": 0,
                    "done": 0,
                    "msg": "",
                }

    def start_sync(self, key, sync_func, *args, **kwargs):
        """
        启动一个后台同步任务
        key: str, 同步任务标识 (如 "futures", "options")
        sync_func: callable, 同步函数, 签名 sync_func(*args, progress_cb=progress_cb, **kwargs)
        返回 (ok: bool, msg: str)
        """
        self._init_state(key)
        with self._lock:
            if self._states[key]["running"]:
                return False, "同步正在进行中，请稍后再试"
            self._states[key]["running"] = True
            self._states[key]["last_error"] = ""
            self._states[key]["msg"] = "启动中..."
            self._states[key]["total"] = 0
            self._states[key]["done"] = 0

        def _progress_cb(total, done, msg):
            with self._lock:
                s = self._states[key]
                s["total"] = total
                s["done"] = done
                s["msg"] = msg

        def _runner(extra=kwargs):
            try:
                result = sync_func(*args, progress_cb=_progress_cb, **extra)
                with self._lock:
                    self._states[key]["running"] = False
                    self._states[key]["last_result"] = result
                    self._states[key]["last_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._states[key]["msg"] = f"完成: 共入库 {result} 条"
            except Exception as e:
                traceback.print_exc()
                with self._lock:
                    self._states[key]["running"] = False
                    self._states[key]["last_error"] = str(e)
                    self._states[key]["last_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._states[key]["msg"] = f"失败: {e}"

        threading.Thread(target=_runner, daemon=True).start()
        return True, "同步任务已启动"

    def get_state(self, key):
        """获取同步状态"""
        self._init_state(key)
        with self._lock:
            s = dict(self._states[key])
        return s

    def start_auto_timer(self, hour=2, incremental=True, run_on_start=False):
        """每日定时同步 (默认凌晨 hour:00 整点).
        incremental=True 时只同步库中尚未存在的合约(增量), 大幅减少天勤请求量.
        run_on_start=True 时启动时先跑一次全量(用于首次/空库初始化); 否则启动不自动拉取,
        避免"每次启动都去刷天勤"造成浪费."""
        import datetime as _dt
        def _next_delay(h):
            now = _dt.datetime.now()
            target = now.replace(hour=h, minute=0, second=0, microsecond=0)
            if target <= now:
                target += _dt.timedelta(days=1)
            return max(1.0, (target - now).total_seconds())
        def _auto():
            if run_on_start:
                # 仅首次/空库: 全量回填补全历史(含已到期合约)
                try:
                    from services.futures_db import sync_from_tqsdk, sync_options_from_tqsdk
                    self.start_sync("futures", sync_from_tqsdk, 12, 12)
                    time.sleep(30)
                    self.start_sync("options", sync_options_from_tqsdk, 12, 12)
                except Exception:
                    traceback.print_exc()
            while True:
                delay = _next_delay(hour)
                print(f"[同步] 距下次自动同步(每日 {hour}:00) 还有 {int(delay)} 秒")
                time.sleep(delay)
                try:
                    from services.futures_db import sync_from_tqsdk, sync_options_from_tqsdk
                    if incremental:
                        self.start_sync("futures", sync_from_tqsdk, 12, 12, incremental=True)
                    else:
                        self.start_sync("futures", sync_from_tqsdk, 12, 12)
                    time.sleep(30)
                    if incremental:
                        self.start_sync("options", sync_options_from_tqsdk, 12, 12, incremental=True)
                    else:
                        self.start_sync("options", sync_options_from_tqsdk, 12, 12)
                except Exception:
                    traceback.print_exc()
        threading.Thread(target=_auto, daemon=True).start()
        print(f"[同步] 每日 {hour}:00 自动{'增量' if incremental else '全量'}同步已设定 "
              f"(启动{'会' if run_on_start else '不'}自动拉取)")


# 全局单例
sync_service = SyncService()