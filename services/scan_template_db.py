# -*- coding: utf-8 -*-
"""
扫描模板持久化模块。

用途：
1. 保存用户自定义的扫描配置模板（规则勾选/参数、周期勾选、模板名）。
2. 加载模板恢复为完整的扫描配置。
3. 支持重命名与删除。

模板 JSON 结构示例：
{
  "name": "MACD超短",
  "periods": ["1m", "5m", "15m"],
  "rules": [ { ... }, ... ]
}
"""
import json
import re
import traceback
from datetime import datetime

from services import futures_db


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS t_scan_template (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    template_json JSON NOT NULL,
    is_default TINYINT(1) DEFAULT 0,
    priority INT NOT NULL DEFAULT 0,
    create_time DATETIME DEFAULT NOW(),
    update_time DATETIME DEFAULT NOW(),
    INDEX idx_name (name),
    INDEX idx_default (is_default),
    INDEX idx_priority (priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def _get_conn():
    return futures_db.get_connection()


def ensure_table():
    """幂等建表 + 补齐 priority 列（老表兼容）。"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    except Exception:
        pass
    try:
        _ensure_priority_col(conn)
    except Exception as e:
        print(f"[scan_template] ensure priority col fail: {e}")
    finally:
        conn.close()


def _ensure_priority_col(conn):
    """老表可能没有 priority 列，安全补列（避免元数据锁永久阻塞）。"""
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION lock_wait_timeout=5")
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_schema=DATABASE() AND table_name='t_scan_template' AND column_name='priority'"
            )
            if cur.fetchone()[0] == 0:
                cur.execute("ALTER TABLE t_scan_template ADD COLUMN priority INT NOT NULL DEFAULT 0")
                conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def list_templates():
    """返回所有用户保存的模板(含 periods/rules/products 完整配置)。

    排序：优先级高者在前(priority DESC)；优先级相同则后创建(更新时间新)的在前。
    即“级别高的、后创建的放前面”。
    rules 按各模板保存时的 enabled 标记, 前端可据此构建条件并集; products 空数组=全市场。
    """
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, is_default, priority, update_time, template_json "
                "FROM t_scan_template ORDER BY priority DESC, create_time DESC"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for row in rows:
        body = row[5]
        if isinstance(body, (bytes, bytearray)):
            try:
                body = body.decode("utf-8")
            except Exception:
                body = "{}"
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                body = {}
        if not isinstance(body, dict):
            body = {}
        out.append({
            "id": row[0],
            "name": row[1],
            "is_default": bool(row[2]),
            "priority": int(row[3]) if row[3] is not None else 0,
            "update_time": row[4].strftime("%Y-%m-%d %H:%M:%S") if row[4] else None,
            "periods": body.get("periods", []),
            "rules": body.get("rules", []),
            "products": body.get("products") or [],
        })
    return out


def monitor_rules():
    """自动监控 / 手动 merge 用的【条件并集】: DEFAULT_RULES + 全部用户模板的已勾选(enabled)规则。

    口径(2026-09-23 用户确认: 模版默认全选、勾选的条件全抓取):
      * 以 DEFAULT_RULES 打底(默认全部生效);
      * 按 rule.id 并集各模板规则 —— 同 id 高优先级模板的参数覆盖默认/低优先级;
      * 模板里 enabled=False(未勾选)的规则【不抓取】;
      * 模板没有的默认规则继续保留。
    返回规则列表; 异常时返回 []（调用方应回落到 DEFAULT_RULES）。
    """
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT template_json, priority FROM t_scan_template "
                "ORDER BY priority DESC, create_time DESC"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    out = {}
    try:
        from services import scan_engine as _se
        for r in (_se.DEFAULT_RULES or []):
            rid = r.get("id")
            if rid is not None:
                out[rid] = dict(r)
    except Exception:
        pass
    for tj, _pri in rows:
        if isinstance(tj, (bytes, bytearray)):
            try:
                tj = tj.decode("utf-8")
            except Exception:
                tj = "{}"
        if isinstance(tj, str):
            try:
                tj = json.loads(tj)
            except Exception:
                tj = {}
        if not isinstance(tj, dict):
            continue
        for r in (tj.get("rules") or []):
            if not isinstance(r, dict):
                continue
            rid = r.get("id")
            if rid is None:
                # 无 id 的自定义规则: 仅当它被勾选时追加(避免与默认重名污染)
                if r.get("enabled", True):
                    out.setdefault("__custom_%d" % id(r), dict(r))
                continue
            if not r.get("enabled", True):
                continue            # 未勾选 -> 不抓取
            out[rid] = dict(r)     # 高优先级模板(先遍历)覆盖低优先级/默认
    return list(out.values())


# ===== 逐标的"最严规则"映射(2026-10-10 新增) =====
# 语义(用户裁定): 每个模板监控"自己的模板规则"; 当某个标的落在多个模板里(重叠)时,
#   不各自出多条信号, 而是按"最严(最保守、最难触发)那一套"规则来算, 去重只留一套。
# 数据获取层仍一次性批量取(不变), 仅规则评估层按标的隔离 + 最严合并。
#
# "最严"方向依规则语义而定(非单纯 max/min), 以各 _detect_* 实际比较算子为准:
#   _STRICT_MAX(阈值越大越难触发): overbought 类 / upper 侧 / 异常类(>=, 阈值越大越严);
#   _STRICT_MIN(阈值越小越难触发): oversold 类 / lower 侧(death 侧, 阈值越小越严)。
#   无 threshold 的规则(背离/BOLL/OBV/WVAD)直接保留, 不比数值。
_STRICT_MAX = {
    "rsi_overbought", "kdj_overbought", "cci_overbought",
    "macd_golden",      # op=gt: value>threshold, 阈值越大越难触发
    "vol_ratio", "vroc",  # 异常类 >=, 阈值越大越严
}
_STRICT_MIN = {
    "rsi_oversold", "kdj_oversold", "cci_oversold",
    "macd_death",      # op=le: value<=threshold, 阈值越小越难触发
}
# 既非 _STRICT_MAX 也非 _STRICT_MIN(无阈值可比, 直接保留一份)的规则
_KEEP_DIRECT = {
    "top_divergence", "bottom_divergence",
    "boll_lower", "boll_upper",
    "obv", "wvad",
}


def _prod_code_of(symbol):
    """合约代码 -> 品种码(交易所 + 品种字母, 不含月份)。

    模板 products 里配的是【品种码】(如 'CFFEX.T' / 'SHFE.cu' / 'CZCE.TA'), 而监控池里混着两类代码:
      * 主连  'KQ.m@CFFEX.T2612' -> 剥 '@' 前缀得 'CFFEX.T2612'(带月份)
      * 子合约'CFFEX.T2612'      -> 本身就是带月份的完整代码
    [2026-10-15 修复] 原实现只剥 '@' 前缀, 不去月份 -> 'CFFEX.T2612' 永远匹配不上模板里的
    'CFFEX.T', 导致【主连与子合约都拿不到模板规则】(实测 monitor_rules_by_symbol 把它们
    直接排除, 回落 DEFAULT_RULES, 于是"设置里配的范围"对主连/子合约完全失效)。

    正确口径: 只砍掉【月份数字后缀】, 字母部分原样保留(品种字母 1~2 位都可能有):
      CFFEX.T2612 -> CFFEX.T / SHFE.cu2611 -> SHFE.cu / CZCE.TA610 -> CZCE.TA / DCE.i2609 -> DCE.i
    实现: 正则取 '交易所.' + 其后的连续字母, 数字起(含)全部丢弃。
    """
    if not symbol:
        return symbol
    code = symbol.split("@", 1)[-1].strip() if "@" in symbol else symbol.strip()
    m = re.match(r"^([A-Za-z]+)\.([A-Za-z]+)", code)
    if m:
        return f"{m.group(1)}.{m.group(2)}"
    return code


def _coerce_json(tj):
    if isinstance(tj, (bytes, bytearray)):
        try:
            tj = tj.decode("utf-8")
        except Exception:
            tj = "{}"
    if isinstance(tj, str):
        try:
            tj = json.loads(tj)
        except Exception:
            tj = {}
    if not isinstance(tj, dict):
        tj = {}
    return tj


def _effective_rules(tpl_json):
    """单个模板的有效规则集(含默认兜底): {rid: rule}。

    口径: 以 DEFAULT_RULES 全 enabled 打底; 模板 rules 中:
      * enabled=True  -> 覆盖默认(用模板参数);
      * enabled=False -> 保留在集里但标 enabled=False(供"最严合并"识别显式禁用);
      * 无 id 的自定义规则 -> enabled=True 时追加。
    模板未提及的规则 -> 保留默认 enabled。
    """
    eff = {}
    try:
        from services import scan_engine as _se
        for r in (_se.DEFAULT_RULES or []):
            rid = r.get("id")
            if rid is not None:
                eff[rid] = dict(r)
    except Exception:
        pass
    for r in (tpl_json.get("rules") or []):
        if not isinstance(r, dict):
            continue
        rid = r.get("id")
        if rid is None:
            if r.get("enabled", True):
                eff["__custom_%d" % id(r)] = dict(r)
            continue
        eff[rid] = dict(r)   # 保留(含 enabled=False 标记)
    return eff


def _rule_threshold(rule):
    """取规则阈值; 取不到/无 threshold 返回 None(如背离/BOLL/OBV)。"""
    try:
        tp = rule.get("params") or {}
        if "threshold" not in tp:
            return None
        v = tp.get("threshold")
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _merge_one_rule(ra, rb):
    """两模板对同一 rule_id 的严格合并: 取最严(难触发)那套参数(整体保留其 params)。"""
    rid = ra.get("id")
    ta = _rule_threshold(ra)
    tb = _rule_threshold(rb)
    # 任一方无阈值 -> 取有阈值的那方; 两方都无 -> 保留 ra(等价, 不比数值)
    if ta is None or tb is None:
        return dict(rb) if (ta is None and tb is not None) else dict(ra)
    if rid in _STRICT_MAX:
        chosen = ra if ta >= tb else rb   # 大阈值=更严
    elif rid in _STRICT_MIN:
        chosen = ra if ta <= tb else rb   # 小阈值=更严
    else:
        # 未分类但有阈值: 保守取更严(大阈值, 与 overbought/异常类一致)
        chosen = ra if ta >= tb else rb
    return dict(chosen)


def monitor_rules_by_symbol(symbols):
    """按品种→模板建【最严规则】映射。

    返回 {symbol: {"rules": [...], "templates": [{"id":.., "name":..}, ...]}}。
    映射口径:
      * 每个标的按品种码(EX.PC)归类;
      * 模板 products 为空 = 全市场(应用到 symbols 全部); 否则只命中其 products 的标的;
      * 单模板: 直接用该模板有效规则(DEFAULT_RULES + 该模板已勾选覆盖);
      * 多模板重叠: 按"最严"合并(阈值方向依规则类型定 max/min; 任一模板显式禁用→该标的全局关);
      * 无 threshold 的规则(背离/BOLL/OBV/WVAD/量比类)直接保留, 不比数值。
    异常时返回 {}（调用方回落单合并集 / 默认规则）。
    """
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, template_json, priority FROM t_scan_template "
                "ORDER BY priority DESC, create_time DESC"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    # 解析所有模板(已按优先级高->低排序): {id, name, products, eff}
    tpls = []
    for tid, tname, tj, _pri in rows:
        tj = _coerce_json(tj)
        if not tj:
            continue
        prods = [str(p).strip() for p in (tj.get("products") or []) if str(p).strip()]
        eff = _effective_rules(tj)
        tpls.append({"id": tid, "name": tname, "products": prods, "eff": eff})
    return _merge_rules_by_symbol(symbols, tpls)


def _merge_rules_by_symbol(symbols, tpls):
    """纯函数: 给定已解析模板列表, 建逐标的最严规则映射(可被单测直接驱动, 无需 DB)。

    tpls: [{"id", "name", "products": [品种码...], "eff": {rid: rule}}], 需按优先级高->低排序。
    """
    out = {}
    if not tpls:
        return out
    for sym in (symbols or []):
        pcode = _prod_code_of(sym)
        # 收集命中该标的的模板(全市场模板 products 为空)
        hit = [t for t in tpls if (not t["products"]) or (pcode in t["products"])]
        if not hit:
            continue
        merged = {}
        disabled = set()
        # hit 已按优先级高->低; 高优先级模板的规则先入, 低优先级按"最严"合并/禁用覆盖
        for t in hit:
            for rid, r in t["eff"].items():
                if not r.get("enabled", True):
                    disabled.add(rid)   # 任一模板显式禁用 -> 该标的全局关
                    continue
                if rid in disabled:
                    continue            # 已被禁用 -> 不再启(不被其他模板的 enabled 复活)
                if rid in merged:
                    merged[rid] = _merge_one_rule(merged[rid], r)
                else:
                    merged[rid] = dict(r)
        for rid in disabled:
            merged.pop(rid, None)
        rules = list(merged.values())
        if not rules:
            continue
        tnames = [{"id": t["id"], "name": t["name"]} for t in hit]
        out[sym] = {"rules": rules, "templates": tnames}
    return out


def _rules_by_symbol_sig(symbol_rules):
    """逐标的规则映射的稳定签名(任一标的规则/阈值变化 -> 签名变), 用于"条件变化即重建"判定。"""
    try:
        parts = []
        for sym in sorted(symbol_rules.keys()):
            info = symbol_rules[sym]
            rids = []
            for r in (info.get("rules") or []):
                rid = r.get("id")
                if rid is None:
                    continue
                th = _rule_threshold(r)
                rids.append("%s=%s" % (rid, th))
            parts.append("%s{%s}" % (sym, "|".join(sorted(rids))))
        return "#".join(parts)
    except Exception:
        return ""


def monitor_products():
    """监控标的池用的「品种白名单」(有序去重)。

    规则(2026-09-23 与用户敲定):
      * 没有模板            -> 返回 [] 表示"全市场";
      * 任一模板没勾品种     -> 视为该模板要"全市场" -> 返回 [];
      * 否则                -> 所有模板 products 的并集, 按模板优先级
                               (priority DESC, create_time DESC) 去重排序。
    返回的顺序即"预算截断时的优先次序" —— 优先级高的模板, 其品种先入选。
    """
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT template_json FROM t_scan_template "
                "ORDER BY priority DESC, create_time DESC"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return []
    out, seen = [], set()
    for row in rows:
        tj = row[0]
        if isinstance(tj, (bytes, bytearray)):
            tj = tj.decode("utf-8")
        if isinstance(tj, str):
            try:
                tj = json.loads(tj)
            except Exception:
                tj = {}
        if not isinstance(tj, dict):
            tj = {}
        prods = [str(p).strip() for p in (tj.get("products") or []) if str(p).strip()]
        if not prods:
            return []          # 空模板 = 全市场
        for p in prods:
            if p not in seen:
                seen.add(p)
                out.append(p)
    return out


def monitor_periods():
    """自动监控用的【周期并集】: 所有模板 periods 的并集(priority 顺序去重)。

    口径(2026-10-09 新增, 对齐 monitor_products 的并集语义, 修复"模板未勾 1 分却出 1 分"):
      * 没有模板            -> 返回 None (调用方回落 AUTO_PERIODS 全集);
      * 全部模板都未勾周期   -> 返回 None (同上, 防空模板时彻底没监控);
      * 否则                -> 所有【勾了周期】的模板 periods 并集,
                               按优先级(priority DESC, create_time DESC)去重排序。
    注意: 空 periods 的模板不参与并集(它视为"不限定周期", 由视图层回落全周期)。
    """
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT template_json FROM t_scan_template "
                "ORDER BY priority DESC, create_time DESC"
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    out, seen = [], set()
    for row in rows:
        tj = row[0]
        if isinstance(tj, (bytes, bytearray)):
            tj = tj.decode("utf-8")
        if isinstance(tj, str):
            try:
                tj = json.loads(tj)
            except Exception:
                tj = {}
        if not isinstance(tj, dict):
            tj = {}
        ps = [str(p).strip() for p in (tj.get("periods") or []) if str(p).strip()]
        for p in ps:
            if p not in seen:
                seen.add(p)
                out.append(p)
    return out if out else None


def load_template(tid):
    """按 ID 加载完整模板配置。"""
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, template_json, is_default FROM t_scan_template WHERE id = %s",
                (int(tid),)
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    try:
        body = json.loads(row[2])
    except Exception:
        body = {}
    return {
        "id": row[0],
        "name": row[1],
        "is_default": bool(row[3]),
        "periods": body.get("periods", []),
        "rules": body.get("rules", []),
        "products": body.get("products") or [],
    }


def save_template(name, periods, rules, products=None, tid=None, is_default=False, priority=None):
    """保存模板。tid 存在则更新，否则新增。priority 仅在显式传入时使用。

    products: 品种白名单列表，如 ["CZCE.MA", "SHFE.rb"]。空列表表示不限制（监控全部）。
    """
    ensure_table()
    if not name:
        return {"ok": False, "error": "模板名不能为空"}
    try:
        body = json.dumps({
            "name": name,
            "periods": periods or [],
            "rules": rules or [],
            "products": products or [],
        }, ensure_ascii=False, default=str)
    except Exception as e:
        return {"ok": False, "error": f"JSON 序列化失败: {e}"}

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            if tid:
                cur.execute(
                    "UPDATE t_scan_template SET name=%s, template_json=%s, update_time=NOW() WHERE id=%s",
                    (name, body, int(tid))
                )
                if cur.rowcount == 0:
                    return {"ok": False, "error": "模板不存在"}
                affected = cur.rowcount
            else:
                if priority is not None:
                    pri = int(priority)
                else:
                    # 新模板默认拿到“当前最大优先级+1”，保证“后创建的排在最前面”；
                    # 同时显式优先级(max+1)避免同秒创建时 create_time 并列退化成主键顺序。
                    cur.execute("SELECT COALESCE(MAX(priority),0) FROM t_scan_template")
                    pri = int(cur.fetchone()[0]) + 1
                cur.execute(
                    "INSERT INTO t_scan_template (name, template_json, is_default, priority) VALUES (%s, %s, %s, %s)",
                    (name, body, 1 if is_default else 0, pri)
                )
                affected = cur.lastrowid
        conn.commit()
        return {"ok": True, "id": affected, "name": name}
    except Exception as e:
        print(f"[scan_template] 保存失败: {e}\n{traceback.format_exc()}")
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def move_template(tid, direction):
    """上移/下移模板（direction='up'|'down'）。

    实现：取出当前按 (priority DESC, create_time DESC) 排序的全部 id，
    把目标 id 与相邻项交换位置后，按新顺序重排 priority（越靠前 priority 越大），
    从而保证“级别高的、后创建的放前面”的排序稳定生效。
    """
    ensure_table()
    if direction not in ("up", "down"):
        return {"ok": False, "error": "direction 必须为 up/down"}
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM t_scan_template ORDER BY priority DESC, create_time DESC"
            )
            ids = [r[0] for r in cur.fetchall()]
        if tid not in ids:
            return {"ok": False, "error": "模板不存在"}
        i = ids.index(tid)
        j = i - 1 if direction == "up" else i + 1
        if j < 0 or j >= len(ids):
            return {"ok": True}  # 已在顶端/底端，无需移动
        ids[i], ids[j] = ids[j], ids[i]
        n = len(ids)
        with conn.cursor() as cur:
            for pos, tid2 in enumerate(ids):
                # 越靠前 priority 越大 -> 排在最前
                cur.execute(
                    "UPDATE t_scan_template SET priority=%s, update_time=update_time WHERE id=%s",
                    (n - pos, int(tid2))
                )
            conn.commit()
        return {"ok": True}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def delete_template(tid):
    """删除指定模板。"""
    ensure_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM t_scan_template WHERE id = %s", (int(tid),))
            affected = cur.rowcount
        conn.commit()
        return {"ok": True, "deleted": affected}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()
