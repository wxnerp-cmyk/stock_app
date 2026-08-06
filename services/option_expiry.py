# -*- coding: utf-8 -*-
"""
期货到期一览表 - 从 MySQL 数据库读取
数据来源: services/futures_db.py -> t_futures_expiry
"""
import datetime
from collections import defaultdict

WEEKDAY_NAMES = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def fetch_expiry_data(client, year, month):
    """从 MySQL 查询到期日数据"""
    from services.futures_db import query_by_month
    return query_by_month(year, month)


def build_calendar_data(query_result, year, month):
    import calendar
    cal = calendar.Calendar(firstweekday=6)
    weeks = []
    month_days = list(cal.itermonthdates(year, month))
    week = []
    for d in month_days:
        d_str = d.strftime("%Y-%m-%d")
        day_num = d.day
        is_current = d.month == month
        is_weekend = d.weekday() >= 5
        entry = {
            "day": day_num,
            "is_current_month": is_current,
            "is_weekend": is_weekend,
            "expiry": None,
        }
        if d_str in query_result:
            info = query_result[d_str]
            entry["expiry"] = {
                "exchange": info.get("exchange", ""),
                "underlying": f"{year}年{month}月标的",
                "products": [p[1] for p in info.get("products", [])],
                "symbols": [p[0] for p in info.get("products", [])],
            }
        week.append(entry)
        if len(week) == 7:
            weeks.append(week)
            week = []
    if week:
        while len(week) < 7:
            week.append({"day": 0, "is_current_month": False, "is_weekend": True, "expiry": None})
        weeks.append(week)
    return weeks


def build_weekly_summary(query_result, year, month):
    result = []
    d_list = sorted(query_result.keys())
    for d_str in d_list:
        info = query_result[d_str]
        dt = datetime.datetime.strptime(d_str, "%Y-%m-%d")
        weekday_name = WEEKDAY_NAMES[dt.weekday()]
        day = dt.day
        exchange = info.get("exchange", "")
        products_list = info.get("products", [])
        products_names = [p[1] for p in products_list]
        result.append({
            "date": d_str,
            "day": day,
            "weekday_name": weekday_name,
            "exchange": exchange,
            "underlying": f"{year}年{month}月",
            "products": products_names,
        })
    weeks = []
    current_week = None
    for item in result:
        dt = datetime.datetime.strptime(item["date"], "%Y-%m-%d")
        week_label = f"第{dt.isocalendar()[1]}周"
        if current_week is None or current_week["label"] != week_label:
            current_week = {"label": week_label, "date_range": "", "days": []}
            weeks.append(current_week)
        if current_week is not None:
            current_week["days"].append(item)
        days = current_week["days"]
        current_week["date_range"] = f"{days[0]['day']}日-{days[-1]['day']}日"
    return weeks