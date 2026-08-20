# -*- coding: utf-8 -*-
"""
期权/期货到期日历 - 从 MySQL 数据库读取
数据来源: services/futures_db.py -> t_futures_expiry(期货到期) / t_option_expiry(期权行权)
"""
import datetime
import calendar as _cal

WEEKDAY_NAMES = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def fetch_expiry_data(year, month, include_options=True):
    """从 MySQL 查询期货 + 期权数据, 返回合并字典(无需天勤 client, 数据来自同步入库的库表)

    返回结构:
      {
        'futures': {date: {'exchange':.., 'products':[(symbol,name),...]}},
        'options': {date: {'exchange':.., 'underlyings': {name:{'calls':[..],'puts':[..]}}}}
      }
    """
    from services import futures_db
    data = {'futures': {}, 'options': {}}
    try:
        data['futures'] = futures_db.query_by_month(year, month) or {}
    except Exception as e:
        print(f"[到期日历] 期货查询失败: {e}")
    if include_options:
        try:
            data['options'] = futures_db.query_option_by_month(year, month) or {}
        except Exception as e:
            print(f"[到期日历] 期权查询失败: {e}")
    return data


def build_calendar_data(futures, options, year, month, view='all'):
    """生成月历网格(周一为首列, 与表头一致), 每天挂载 futures/options 信息, 按 view 过滤。
    新增 is_expired 标记: 该天所有挂载的合约/期权到期日均 < 今天, 则该格子视为已过期, 显示灰色。
    """
    today = datetime.date.today()
    cal = _cal.Calendar(firstweekday=0)  # 周一为首, 与页面表头(周一..周日)一致
    weeks = []
    month_days = list(cal.itermonthdates(year, month))
    week = []
    for d in month_days:
        d_str = d.strftime("%Y-%m-%d")
        entry = {
            "day": d.day,
            "is_current_month": d.month == month,
            "is_weekend": d.weekday() >= 5,
            "futures": None,
            "options": None,
            "is_expired": False,
        }
        has_data = False
        all_dates = []
        if view in ('all', 'futures') and d_str in futures:
            f = futures[d_str]
            entry["futures"] = {"exchange": f.get("exchange", ""), "products": f.get("products", [])}
            has_data = True
            all_dates.append(d)
        if view in ('all', 'options') and d_str in options:
            o = options[d_str]
            entry["options"] = {"exchange": o.get("exchange", ""), "underlyings": o.get("underlyings", {})}
            has_data = True
            all_dates.append(d)
        # 判断过期: 只要该格子的日期 < 今天, 且挂载了数据, 就标为过期
        if has_data and d < today:
            entry["is_expired"] = True
        week.append(entry)
        if len(week) == 7:
            weeks.append(week)
            week = []
    if week:
        while len(week) < 7:
            week.append({"day": 0, "is_current_month": False, "is_weekend": True,
                         "futures": None, "options": None, "is_expired": False})
        weeks.append(week)
    return weeks


def build_weekly_summary(futures, options, year, month, view='all'):
    """按 ISO 周汇总有数据的日期, 同时纳入期货/期权; 增加 is_expired 标记"""
    today = datetime.date.today()
    dates = set()
    if view in ('all', 'futures'):
        dates |= set(futures.keys())
    if view in ('all', 'options'):
        dates |= set(options.keys())
    items = []
    for d_str in sorted(dates):
        dt = datetime.datetime.strptime(d_str, "%Y-%m-%d")
        item = {
            "date": d_str,
            "day": dt.day,
            "weekday_name": WEEKDAY_NAMES[dt.weekday()],
            "exchange": "",
            "futures_products": [],
            "options_underlyings": [],
            "is_expired": dt.date() < today,
        }
        if view in ('all', 'futures') and d_str in futures:
            f = futures[d_str]
            item["exchange"] = f.get("exchange", "")
            item["futures_products"] = [p[1] for p in f.get("products", [])]
        if view in ('all', 'options') and d_str in options:
            o = options[d_str]
            if not item["exchange"]:
                item["exchange"] = o.get("exchange", "")
            for und, cpl in o.get("underlyings", {}).items():
                item["options_underlyings"].append({
                    "name": und,
                    "calls": len(cpl.get("calls", [])),
                    "puts": len(cpl.get("puts", [])),
                })
        items.append(item)
    weeks = []
    current = None
    for item in items:
        dt = datetime.datetime.strptime(item["date"], "%Y-%m-%d")
        wl = f"第{dt.isocalendar()[1]}周"
        if current is None or current["label"] != wl:
            current = {"label": wl, "date_range": "", "days": []}
            weeks.append(current)
        current["days"].append(item)
        days = current["days"]
        current["date_range"] = f"{days[0]['day']}日-{days[-1]['day']}日"
    return weeks
