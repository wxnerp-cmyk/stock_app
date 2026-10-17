# -*- coding: utf-8 -*-
"""
定时爬取任务持久化模块。

一条 schedule = 把某个扫描模板在指定时间段内自动启动/停止：
{
  id, name,
  template_id   -> 关联 t_scan_template(周期/规则/阈值),
  start_hm      -> "HH:MM" 开始(北京时间),
  end_hm        -> "HH:MM" 结束, 支持跨午夜(如 21:00-02:30),
  days_bit      -> 周几生效位掩码(Mon=1<<0 ... Sun=1<<6, 默认 127=每天),
  enabled,
  create_time, update_time
}
"""
import traceback
from datetime import datetime

from services import futures_db


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS t_scan_schedule (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    template_id BIGINT NULL,
    start_hm VARCHAR(8) NOT NULL DEFAULT '09:00',
    end_hm VARCHAR(8) NOT NULL DEFAULT '15:00',
    days_bit INT NOT NULL DEFAULT 127,
    enabled TINYINT(1) DEFAULT 1,
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW(),
    INDEX idx_enabled (enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _get_conn():
    return futures_db.get_connection()


def ensure_table():
    """幂等建表。"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def list_schedules():
    """返回所有定时任务, 并 JOIN 模板名。"""
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT s.id, s.name, s.template_id, t.name AS tpl_name,
                          s.start_hm, s.end_hm, s.days_bit, s.enabled
                   FROM t_scan_schedule s
                   LEFT JOIN t_scan_template t ON s.template_id = t.id
                   ORDER BY s.enabled DESC, s.start_hm ASC"""
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        out.append({
            "id": r[0],
            "name": r[1],
            "template_id": r[2],
            "template_name": r[3],
            "start_hm": str(r[4]) if r[4] else "09:00",
            "end_hm": str(r[5]) if r[5] else "15:00",
            "days_bit": int(r[6]) if r[6] is not None else 127,
            "enabled": bool(r[7]),
        })
    return out


def save_schedule(name, template_id, start_hm, end_hm, days_bit, enabled=1, sid=None):
    """保存定时任务。sid 存在则更新, 否则新增。"""
    ensure_table()
    if not name:
        return {"ok": False, "error": "任务名不能为空"}
    try:
        sh = str(start_hm).strip()
        eh = str(end_hm).strip()
        db = int(days_bit) if days_bit is not None else 127
        en = 1 if enabled else 0
        tid = int(template_id) if template_id else None
    except Exception as e:
        return {"ok": False, "error": f"参数解析失败: {e}"}

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            if sid:
                cur.execute(
                    """UPDATE t_scan_schedule
                       SET name=%s, template_id=%s, start_hm=%s, end_hm=%s,
                           days_bit=%s, enabled=%s, update_time=NOW()
                       WHERE id=%s""",
                    (name, tid, sh, eh, db, en, int(sid))
                )
                if cur.rowcount == 0:
                    return {"ok": False, "error": "任务不存在"}
                affected = int(sid)
            else:
                cur.execute(
                    """INSERT INTO t_scan_schedule
                           (name, template_id, start_hm, end_hm, days_bit, enabled)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (name, tid, sh, eh, db, en)
                )
                affected = cur.lastrowid
        conn.commit()
        return {"ok": True, "id": affected, "name": name}
    except Exception as e:
        print(f"[scan_schedule] 保存失败: {e}\n{traceback.format_exc()}")
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def delete_schedule(sid):
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM t_scan_schedule WHERE id = %s", (int(sid),))
            affected = cur.rowcount
        conn.commit()
        return {"ok": True, "deleted": affected}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def toggle_schedule(sid, enabled=None):
    """切换启用状态; enabled 为 None 时翻转。"""
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            if enabled is None:
                cur.execute(
                    "UPDATE t_scan_schedule SET enabled = 1 - enabled, update_time=NOW() WHERE id=%s",
                    (int(sid),)
                )
            else:
                cur.execute(
                    "UPDATE t_scan_schedule SET enabled=%s, update_time=NOW() WHERE id=%s",
                    (1 if enabled else 0, int(sid))
                )
            affected = cur.rowcount
        conn.commit()
        return {"ok": True, "toggled": affected}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()
