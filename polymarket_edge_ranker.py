#!/usr/bin/env python3
"""
Build a broad Polymarket wallet universe from every official leaderboard mode,
then rank wallets with the Edge Rally × Net Edge score:

where:
    win_edge  = 1 - avgPrice  for profitable closed positions
    loss_risk = avgPrice      for losing closed positions
    Net Edge  = sum(win_edge) - sum(loss_risk)
    Edge Rally Raw = sum(win_edge^2) / (sum(loss_risk^2) + 1)
    Final Edge Rally Score = Edge Rally Raw × Net Edge
    adjustedWinRate = Wilson lower-bound win rate - average resolved entry price

This is a statistical ranking tool, not proof of insider trading.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import math
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from typing import Any

try:
    import requests
except ImportError:  # urllib fallback remains available
    requests = None


# =============================================================================
# تنظیمات اصلی برنامه
# =============================================================================
# فقط همین بخش را تغییر بده. بقیه کد برای اجرای همین تنظیمات است.
#
# روش اجرا:
#   1. برای استخراج والت‌ها، RUN_MODE را بگذار 1 و بزن:
#      py polymarket_edge_ranker.py
#
#   2. برای تست/امتیازدهی والت‌های استخراج‌شده، RUN_MODE را بگذار 2 و بزن:
#      py polymarket_edge_ranker.py
#
# فایل‌های مهم خروجی:
#   wallet_universe.csv        لیست والت‌های استخراج‌شده از لیدربوردها
#   1.xlsx                     ورودی رتبه‌بندی‌شده مود 2؛ همان edge_scores_by_oneShareNetPnlAfterCosts.xlsx که اسمش را عوض کرده‌ای
#   wallet_test_memory.csv     حافظه تست؛ اگر پاکش کنی تست از اول شروع می‌شود
#   closed_positions_raw.jsonl دیتای خام کامل هر والت؛ برای فرمول‌های بعدی نگهش دار
#   closed_positions_pages.jsonl حافظه قدیمی صفحه‌ای
#   polymarket_complete_fetch_cache.sqlite3 حافظه دقیق market/activity؛ اگر وسط والت بزرگ قطع شد ادامه می‌دهد
#   closed_positions_raw2.jsonl دیتای آفلاین دوم؛ فقط وقتی fallback روشن باشد استفاده می‌شود
#   closed_positions_pages2.jsonl cache صفحه‌ای دوم؛ فقط برای داده‌های گمشده استفاده می‌شود
#   edge_scores_progress.csv   خروجی زنده CSV؛ بعد از هر والت مرتب و آپدیت می‌شود
#   edge_scores.xlsx           خروجی آماری XLSX؛ بعد از هر والت آپدیت می‌شود
#   wallet_not_saved_reasons.xlsx دلیل ذخیره نشدن والت‌ها در خروجی آماری
#   wallet_not_saved_reason_stats.xlsx آمار تعداد تکرار هر دلیل حذف
# =============================================================================

# آدرس API عمومی پلی‌مارکت. معمولاً لازم نیست تغییرش بدهی.
BASE_URL = "https://data-api.polymarket.com"

# مود اجرا:
#   1 = استخراج والت‌ها از همه حالت‌های لیدربورد
#   2 = تست کردن والت‌های استخراج‌شده و محاسبه Edge Rally × Net Edge Score
RUN_MODE = 2

# پوشه خروجی. برای اینکه مود 2 بتواند خروجی مود 1 را بخواند، بین دو مود تغییرش نده.
OUT_DIR = "polymarket_edge_output"

# اسم فایل حافظه مود 2.
# هر والت بعد از تست شدن داخل این فایل ثبت می‌شود.
# اگر برنامه را ببندی و دوباره اجرا کنی، والت‌های ثبت‌شده را رد می‌کند و ادامه می‌دهد.
# اگر می‌خواهی تست از اول شروع شود، فقط همین فایل را پاک کن.
TEST_MEMORY_FILE_NAME = "wallet_test_memory.csv"

# اسم فایل گزارش والت‌هایی که تحلیل شدند ولی وارد خروجی آماری نشدند.
NOT_SAVED_REASONS_FILE_NAME = "wallet_not_saved_reasons.xlsx"

# اسم فایل آمار تعداد تکرار دلیل‌های ذخیره نشدن والت‌ها.
NOT_SAVED_REASON_STATS_FILE_NAME = "wallet_not_saved_reason_stats.xlsx"

# اسم فایل دیتای خام کامل.
# هر وقت یک والت کامل گرفته شد، همه پوزیشن‌های بسته‌شده‌اش اینجا ذخیره می‌شود.
# اگر بعداً فرمول را عوض کردی، این فایل را پاک نکن تا دوباره API نگیری.
RAW_CLOSED_POSITIONS_LOG_FILE_NAME = "closed_positions_raw.jsonl"

# اسم فایل cache صفحه‌ای پوزیشن‌ها.
# اگر وسط گرفتن یک والت بزرگ قطع شود، صفحه‌های گرفته‌شده داخل این فایل می‌ماند.
# اجرای بعدی همان والت را از offset بعدی ادامه می‌دهد، نه از اول.
CLOSED_POSITION_PAGE_CACHE_FILE_NAME = "closed_positions_pages.jsonl"

# اگر روشن باشد، مود 2 علاوه بر فایل‌های اصلی بالا، فایل‌های آفلاین دوم را هم می‌خواند.
# فقط والت/صفحه‌هایی که داخل فایل‌های اصلی نبودند از این دو فایل fallback برداشته می‌شوند.
USE_SECONDARY_OFFLINE_POSITION_BACKUPS = False
SECONDARY_RAW_CLOSED_POSITIONS_LOG_FILE_NAME = "closed_positions_raw2.jsonl"
SECONDARY_CLOSED_POSITION_PAGE_CACHE_FILE_NAME = "closed_positions_pages2.jsonl"

# اگر روشن باشد، مود 2 به جای wallet_universe.csv از فایل رتبه‌بندی‌شده زیر استفاده می‌کند.
# فایل edge_scores_by_oneShareNetPnlAfterCosts.xlsx را به این اسم تغییر بده تا برنامه از بالای لیست شروع کند.
USE_ONE_SHARE_RANKING_INPUT = False
ONE_SHARE_RANKING_INPUT_FILE_NAME = "1.xlsx"

# تاخیر بین درخواست‌ها به API، بر حسب ثانیه.
# عدد بالاتر = کندتر ولی امن‌تر برای rate limit / Cloudflare.
HTTP_DELAY = 0.2

# حداکثر زمان انتظار برای هر درخواست API، بر حسب ثانیه.
HTTP_TIMEOUT = 30.0

# تعداد تلاش دوباره وقتی API موقتاً خطا می‌دهد.
HTTP_RETRIES = 8

# دسته‌بندی‌هایی که از لیدربورد پلی‌مارکت گرفته می‌شوند.
# اگر دسته‌ای را نمی‌خواهی، از لیست حذفش کن.
CATEGORIES = [
    "OVERALL",
    "POLITICS",
    "SPORTS",
    "CRYPTO",
    "CULTURE",
    "MENTIONS",
    "WEATHER",
    "ECONOMICS",
    "TECH",
    "FINANCE",
]

# بازه‌های زمانی لیدربورد:
#   DAY   روزانه
#   WEEK  هفتگی
#   MONTH ماهانه
#   ALL   کل تاریخ
TIME_PERIODS = ["DAY", "WEEK", "MONTH", "ALL"]

# نوع مرتب‌سازی لیدربورد:
#   PNL = بر اساس سود
#   VOL = بر اساس حجم معامله
ORDER_BY = ["PNL", "VOL"]

# حداکثر offset برای هر حالت لیدربورد.
# API رسمی معمولاً تا offset حدود 1000 اجازه می‌دهد.
MAX_LEADERBOARD_OFFSET = 1000

# تعداد ردیف در هر درخواست لیدربورد.
# API رسمی معمولاً حداکثر 50 می‌دهد؛ بهتر است تغییرش ندهی.
LEADERBOARD_LIMIT = 50

# حداکثر چند والت از wallet_universe.csv تست شود.
# برای تست سریع عدد کم بگذار، مثلاً 100.
# برای تست همه والت‌ها بگذار None.
MAX_WALLETS_TO_SCORE = 1000000000000000

# حداکثر چند closed position برای هر والت گرفته شود.
# عدد کمتر = سریع‌تر ولی ممکن است دیتای والت‌های خیلی بزرگ کامل نباشد.
# عدد بیشتر = کامل‌تر ولی کندتر.
MAX_POSITIONS_PER_WALLET = 100000000000000

# حداقل تعداد پوزیشن بسته‌شده/نتیجه‌دار برای اینکه والت وارد خروجی score شود.
# اگر 30 باشد، والت‌هایی با کمتر از 30 پوزیشن حذف می‌شوند.
MIN_RESOLVED_POSITIONS = 1

# حداقل تعداد باخت لازم.
# این کمک می‌کند والت‌هایی که فقط چند برد و تقریباً بدون باخت دارند الکی امتیاز نگیرند.
MIN_LOSING_POSITIONS = 0

# حداقل سود بسته‌شده.
# اگر 0 باشد، فقط والت‌هایی وارد خروجی می‌شوند که سودشان منفی نیست.
# برای تست آزاد می‌توانی عدد منفی خیلی بزرگ بگذاری.
MIN_CLOSED_REALIZED_PNL = 0.0

# مخرج پیش‌فرض فرمول Edge Rally فقط وقتی هیچ ضرری وجود ندارد.
# وقتی ضرر وجود دارد، مخرج همان sumLossRiskSq است و smoothing اضافه نمی‌شود.
SMOOTHING = 1.0

# فیلتر منفی بودن موجودی اخیر همه معاملات؛ اگر روشن باشد والت‌هایی که مقدار فعلی همه معاملاتشان منفی است حذف می‌شوند.
FILTER_ALL_RECENT_BALANCES_NEGATIVE = False

# فیلتر Net Edge منفی؛ اگر روشن باشد والت‌هایی که نت اج منفی دارند حذف می‌شوند.
FILTER_NEGATIVE_NET_EDGE = True

# فیلتر سود یک‌سهمی غیرمثبت؛ اگر روشن باشد والت‌هایی که oneShareNetPnlAfterCosts آن‌ها منفی یا صفر است حذف می‌شوند.
FILTER_NON_POSITIVE_ONE_SHARE_NET_PNL_AFTER_COSTS = True

# فیلتر حداقل Recovery Factor؛ اگر روشن باشد والت‌هایی که کمتر از مقدار زیر باشند حذف می‌شوند.
FILTER_MIN_RECOVERY_FACTOR = False

# حداقل Recovery Factor قابل قبول وقتی فیلتر بالا روشن باشد.
MIN_RECOVERY_FACTOR = 5

# فیلتر فعالیت ۷ روز اخیر؛ اگر روشن باشد والت بدون معامله باز/بسته‌شده در ۷ روز اخیر حذف می‌شود.
FILTER_NO_RECENT_7D_OPEN_OR_CLOSE = True

# تعداد روز برای فیلتر فعالیت اخیر.
RECENT_ACTIVITY_DAYS = 20

# فیلتر معاملات کوتاه‌مدت؛ اگر روشن باشد والت‌هایی که درصد زیادی معامله زیر زمان مشخص دارند حذف می‌شوند.
FILTER_SHORT_HOLD_RATIO = False

# حداکثر درصد معاملات کوتاه‌مدت مجاز؛ 0.25 یعنی ۲۵ درصد.
MAX_SHORT_HOLD_RATIO = 0.25

# مرز زمانی معامله کوتاه‌مدت بر حسب ساعت؛ 24 یعنی کمتر از ۲۴ ساعت.
SHORT_HOLD_MAX_HOURS = 24.0

# حذف همزمان والت‌های فیلترشده از دو فایل دیتای پوزیشن jsonl؛ پیش‌فرض خاموش است تا دیتای خام حفظ شود.
PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS = False

# حذف همزمان والت‌های فیلترشده از فایل wallet_universe.csv؛ پیش‌فرض خاموش است تا لیست اولیه دست‌نخورده بماند.
PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE = True

# آپدیت همه فایل‌های آماری بعد از اسکن هر والت؛ خروجی‌ها را زنده نگه می‌دارد ولی کندتر است.
UPDATE_ALL_RESULT_FILES_AFTER_EACH_WALLET = False

# آپدیت edge_scores_progress.csv بعد از اسکن هر والت؛ خروجی زنده CSV می‌دهد ولی روی دیتای زیاد کندتر است.
UPDATE_PROGRESS_CSV_AFTER_EACH_WALLET = False

# اگر روشن باشد، فایل‌های خروجی CSV/XLSX موجود با خروجی اجرای جدید جایگزین می‌شوند.
OVERWRITE_OUTPUT_FILES = True

# تنظیمات سرعت مود 2:
# گزارش‌های سنگین XLSX فقط هر چند والت یک‌بار checkpoint می‌شوند و در پایان حتماً نوشته می‌شوند.
# صفر یعنی فقط در پایان اجرا.
NOT_SAVED_XLSX_CHECKPOINT_EVERY = 250

# cache صفحه‌ای به‌جای flush بعد از تک‌تک درخواست‌ها، هر چند صفحه یک‌بار flush می‌شود.
# در پایان هر والت نیز حتماً flush انجام می‌شود.
PAGE_CACHE_FLUSH_EVERY = 10

# طبق مستندات رسمی، بیشترین offset مجاز endpoint بسته‌شده‌ها 100000 است.
# بالاتر رفتن از این مقدار ممکن است باعث برگشت صفحه تکراری و شمارش جعلی میلیون‌ها پوزیشن شود.
CLOSED_POSITIONS_MAX_API_OFFSET = 100000


# =============================================================================
# دریافت کامل و سریع Closed Positions در مود 2
# =============================================================================
# برای والت‌های کوچک ابتدا صفحه‌بندی مستقیم و مرتب‌سازی ASC استفاده می‌شود.
# برای والت‌های بزرگ، فهرست کامل marketها از Activity گرفته می‌شود و سپس
# closed-positions با marketهای دسته‌بندی‌شده خوانده می‌شود؛ بنابراین سقف offset
# صد هزار باعث ناقص شدن اطلاعات نمی‌شود.
COMPLETE_CLOSED_POSITION_FETCH = True

# اگر تعداد marketهای رسمی والت از این مقدار بیشتر باشد، مستقیم وارد روش کامل
# market-batch می‌شویم و 100 هزار ردیف مستقیم را بیهوده دانلود نمی‌کنیم.
DIRECT_FAST_PATH_MAX_TRADED_MARKETS = 5000

# هر درخواست closed-positions حداکثر 50 ردیف برمی‌گرداند. 36 market معمولاً
# در یک صفحه جا می‌شود و نسبت به 24 market تعداد batchها را کمتر می‌کند؛ اگر یک
# batch بیش از 50 ردیف داشته باشد، همان batch خودکار صفحه‌بندی می‌شود و چیزی حذف نمی‌شود.
CLOSED_MARKET_BATCH_SIZE = 36

# تعداد دانلودهای همزمان. محدودکننده داخلی اجازه عبور از rate limit رسمی را نمی‌دهد.
CLOSED_FETCH_WORKERS = 12
ACTIVITY_FETCH_WORKERS = 10

# حاشیه امن زیر rate limit رسمی Data API.
CLOSED_RATE_LIMIT_CALLS = 135
CLOSED_RATE_LIMIT_PERIOD_SECONDS = 10.0
ACTIVITY_RATE_LIMIT_CALLS = 800
ACTIVITY_RATE_LIMIT_PERIOD_SECONDS = 10.0

# پارامترهای رسمی Activity.
ACTIVITY_PAGE_LIMIT = 500
ACTIVITY_MAX_OFFSET = 5000

# پارامترهای Current Positions برای کنترل کامل بودن پوشش marketها.
CURRENT_POSITION_PAGE_LIMIT = 500
CURRENT_POSITION_MAX_OFFSET = 10000

# marketهایی که در Activity هستند ولی در Closed/Current دیده نمی‌شوند یک بار
# تازه‌سازی می‌شوند. باقی‌ماندن آن‌ها فقط هشدار است، چون Activity الزاماً برای هر
# TRADE یک ردیف Current یا Closed متناظر ایجاد نمی‌کند.
COVERAGE_REPAIR_PASSES = 1

# کش SQLite برای ادامه دادن والت‌های بسیار بزرگ بعد از توقف برنامه.
COMPLETE_FETCH_CACHE_DB_FILE_NAME = "polymarket_complete_fetch_cache.sqlite3"

# کش‌های قدیمی closed_positions_raw.jsonl که metadata کامل بودن ندارند، ممکن است
# همان داده 37 هزار تایی یا داده تکراری باشند؛ برای اولویت صحت به آن‌ها اعتماد نکن.
TRUST_LEGACY_RAW_CLOSED_POSITION_CACHE = False

COMPLETE_FETCH_VERSION = "complete-market-v4"

# =============================================================================
# اجرای چند مسیر پروکسی با IPهای متفاوت
# =============================================================================
# True  = لینک‌های vless / vmess / trojan / ss هم‌زمان اجرا می‌شوند و والت‌ها
#         بین IPهای خروجی متفاوت تقسیم می‌شوند.
# False = برنامه بدون پروکسی و با اینترنت مستقیم اجرا می‌شود.
# نام متغیر برای سازگاری با نسخه قبلی حفظ شده است.
USE_VLESS_MULTI = True

# لینک‌های VPN دیگر داخل فایل پایتون قرار نمی‌گیرند.
# فایل زیر باید کنار همین فایل پایتون باشد و هر لینک در یک خط نوشته شود.
# پروتکل‌های پشتیبانی‌شده: vless://  vmess://  trojan://  ss://
# خطوط خالی و خطوطی که با # شروع شوند نادیده گرفته می‌شوند.
# یک subscription معمولی Base64 هم می‌تواند کامل داخل همین فایل قرار بگیرد.
VPN_LINKS_FILE_NAME = "vpn_list.txt"

# اگر فایل وجود نداشته باشد، برنامه خودش یک قالب خالی کنار فایل پایتون می‌سازد.
AUTO_CREATE_VPN_LINKS_FILE = True

# xray.exe را کنار همین فایل بگذار؛ یا مسیر کامل آن را اینجا بنویس.
XRAY_EXECUTABLE = "xray.exe"

# هر نود یک HTTP proxy محلی جدا و یک shard جدا می‌گیرد.
VLESS_LOCAL_HTTP_PORT_START = 18080
# نام پوشه برای سازگاری با cache اجرای قبلی تغییر نکرده است.
VLESS_OUTPUT_ROOT = "polymarket_edge_output_vless"

# پوشه اجرای قدیمی به‌عنوان fallback فقط‌خواندنی استفاده می‌شود تا حافظه، score و
# cache قبلی دوباره دانلود نشوند. خروجی‌های جدید هر shard جدا هستند.
VLESS_FALLBACK_OUT_DIR = OUT_DIR

# پیش از اجرا IP خروجی هر نود بررسی می‌شود. نودهای خراب یا IPهای تکراری کنار گذاشته می‌شوند.
VLESS_CHECK_OUTBOUND_IP = True
VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS = True
VLESS_IP_CHECK_URL = "https://api.ipify.org"
VLESS_START_TIMEOUT_SECONDS = 15.0

# پس از پایان همه shardها خروجی آماری نهایی به‌صورت خودکار ادغام می‌شود.
VLESS_AUTO_MERGE_OUTPUTS = True

# فایل‌های raw بسیار بزرگ داخل shardها باقی می‌مانند و برای جلوگیری از مصرف دوباره دیسک
# به merged کپی نمی‌شوند. scoreها، memory و خطاها ادغام می‌شوند.
VLESS_MERGE_RAW_JSONL = False

# اگر یک نود وسط اجرا از کار بیفتد، Worker آن متوقف می‌شود و همان shard با cache قبلی
# به یکی از نودهای سالم و بیکار سپرده می‌شود. نود سالم ابتدا shard خودش را تمام می‌کند
# و بعد shard نیمه‌تمام را ادامه می‌دهد تا دو Worker هم‌زمان از یک IP استفاده نکنند.
PROXY_FAILOVER_ENABLED = True

# هر چند ثانیه سلامت Xray و IP خروجی نودها بررسی شود.
# این اعداد عمداً کمی محافظه‌کارانه‌اند تا Timeout لحظه‌ای، نود سالم را dead نکند.
PROXY_HEALTH_CHECK_INTERVAL_SECONDS = 15.0
PROXY_HEALTH_CHECK_TIMEOUT_SECONDS = 10.0

# چند Health Check پیاپی ناموفق باشد تا نود موقتاً dead اعلام شود.
PROXY_HEALTH_FAILURE_THRESHOLD = 4

# نودهای dead حذف دائمی نمی‌شوند؛ هر 60 ثانیه Xray آن‌ها Restart و دوباره تست می‌شود.
# اگر سالم شوند دوباره وارد Pool می‌شوند و می‌توانند shardهای منتظر را تحویل بگیرند.
PROXY_DEAD_RECHECK_ENABLED = True
PROXY_DEAD_RECHECK_INTERVAL_SECONDS = 60.0
PROXY_DEAD_RESTART_BEFORE_CHECK = True

# حداکثر تعداد اجرای دوباره هر shard روی نودهای سالم دیگر.
PROXY_FAILOVER_MAX_ATTEMPTS_PER_SHARD = 8
PROXY_FAILOVER_RETRY_DELAY_SECONDS = 2.0

# خروجی CMD تمیز: جزئیات داخل فایل لاگ می‌روند و فقط Dashboard نشان داده می‌شود.
CLEAN_CONSOLE_DASHBOARD = True
CONSOLE_STATUS_INTERVAL_SECONDS = 10.0
CONSOLE_ERROR_NOTICE_INTERVAL_SECONDS = 60.0
ALL_LOG_FILE_NAME = "all_logs.txt"
ERROR_LOG_FILE_NAME = "errors.txt"

# اگر Worker پروکسی در چند والت پیاپی Fetch Failure بگیرد، خودش با کد مخصوص خارج
# می‌شود تا Manager سریع‌تر آن shard را به یک نود سالم منتقل کند.
PROXY_WORKER_MAX_CONSECUTIVE_FETCH_FAILURES = 3

# ژورنال append-only امتیازها؛ در قطع ناگهانی Worker، والت‌های امتیازگرفته‌شده گم نمی‌شوند.
SCORE_JOURNAL_FILE_NAME = "edge_scores_journal.csv"

# تنظیمات هزینه محافظه‌کارانه بک‌تست کپی‌ترید.
# چون orderbook تاریخی دقیق نداریم، هر ورود/خروج کپی‌شده با اسپرد فرضی بدتر از والت اصلی حساب می‌شود.
ASSUMED_SPREAD = 0.10
USE_POLYMARKET_FEES = True
USE_ASSUMED_SPREAD = True
DEFAULT_FEE_RATE = 0.05

POLYMARKET_FEE_RATES = {
    "CRYPTO": 0.07,
    "SPORTS": 0.05,
    "ECONOMICS": 0.05,
    "CULTURE": 0.05,
    "WEATHER": 0.05,
    "OTHER": 0.05,
    "POLITICS": 0.04,
    "FINANCE": 0.04,
    "MENTIONS": 0.04,
    "TECH": 0.04,
    "GEOPOLITICS": 0.00,
}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _log_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _looks_like_error(message: str) -> bool:
    text = message.lower()
    markers = (
        "[error",
        "traceback",
        "exception",
        "request failed",
        "fetch_failed",
        "fetch failure",
        "timed out",
        "timeout error",
        "connecttimeout",
        "readtimeout",
        "proxyerror",
        "http 429",
        "status 429",
        "http 403",
        "status 403",
        "[proxy:health-fail]",
        "[proxy:dead]",
        "[proxy:skip]",
        "[worker:proxy-failed]",
        "[worker:retry-required]",
        "[failover:give-up]",
        "[fatal]",
    )
    return any(marker in text for marker in markers)


class RunLogRouter:
    """Thread-safe timestamped master/error logs plus a new-error counter."""

    def __init__(self, all_log_path: Path, error_log_path: Path) -> None:
        ensure_dir(all_log_path.parent)
        self.all_log_path = all_log_path
        self.error_log_path = error_log_path
        self._all = all_log_path.open("a", encoding="utf-8", buffering=1)
        self._errors = error_log_path.open("a", encoding="utf-8", buffering=1)
        self._lock = threading.Lock()
        self._new_errors = 0
        self._total_errors = 0

    def log(
        self,
        message: str,
        *,
        source: str = "MANAGER",
        force_error: bool = False,
    ) -> None:
        message = str(message).rstrip("\r\n")
        if not message:
            return
        raw_lines = message.splitlines() or [message]
        with self._lock:
            for raw_line in raw_lines:
                if not raw_line:
                    continue
                line = f"[{_log_timestamp()}] [{source}] {raw_line}"
                is_error = bool(force_error or _looks_like_error(raw_line))
                self._all.write(line + "\n")
                if is_error:
                    self._errors.write(line + "\n")
                    self._new_errors += 1
                    self._total_errors += 1
            self._all.flush()
            self._errors.flush()

    def consume_new_errors(self) -> int:
        with self._lock:
            value = self._new_errors
            self._new_errors = 0
            return value

    @property
    def total_errors(self) -> int:
        with self._lock:
            return self._total_errors

    def close(self) -> None:
        with self._lock:
            try:
                self._all.flush()
                self._all.close()
            finally:
                self._errors.flush()
                self._errors.close()


class RoutedLogStream:
    """Routes existing print() calls to RunLogRouter instead of cluttering CMD."""

    def __init__(self, logger: RunLogRouter, source: str, force_error: bool = False) -> None:
        self.logger = logger
        self.source = source
        self.force_error = force_error
        self._buffer = ""
        self.encoding = "utf-8"

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.rstrip("\r"):
                self.logger.log(
                    line.rstrip("\r"),
                    source=self.source,
                    force_error=self.force_error,
                )
        return len(text)

    def flush(self) -> None:
        if self._buffer.strip():
            self.logger.log(
                self._buffer.rstrip("\r\n"),
                source=self.source,
                force_error=self.force_error,
            )
        self._buffer = ""

    def isatty(self) -> bool:
        return False


def remove_obsolete_trade_dedup_files(out_dir: Path) -> None:
    patterns = (
        "wallet_trade_activity_*.jsonl",
        "edge_scores_by_*VolumeAdditions*.xlsx",
        "edge_scores_by_sameOutcomeVolumeAdditions.xlsx",
        "edge_scores_by_hedgedMarketCount.xlsx",
    )
    for pattern in patterns:
        for path in out_dir.glob(pattern):
            path.unlink(missing_ok=True)


def open_csv_append(path: Path, fieldnames: list[str]):
    exists = path.exists() and path.stat().st_size > 0
    file = path.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    if not exists:
        writer.writeheader()
        file.flush()
    return file, writer


class SlidingWindowRateLimiter:
    """Thread-safe sliding-window limiter used by concurrent API workers."""

    def __init__(self, max_calls: int, period_seconds: float):
        self.max_calls = max(int(max_calls), 1)
        self.period_seconds = max(float(period_seconds), 0.001)
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            wait_for = 0.0
            with self._lock:
                now = time.monotonic()
                cutoff = now - self.period_seconds
                while self._timestamps and self._timestamps[0] <= cutoff:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    return
                wait_for = self.period_seconds - (now - self._timestamps[0]) + 0.002
            time.sleep(max(wait_for, 0.002))


CLOSED_API_LIMITER = SlidingWindowRateLimiter(
    CLOSED_RATE_LIMIT_CALLS, CLOSED_RATE_LIMIT_PERIOD_SECONDS
)
ACTIVITY_API_LIMITER = SlidingWindowRateLimiter(
    ACTIVITY_RATE_LIMIT_CALLS, ACTIVITY_RATE_LIMIT_PERIOD_SECONDS
)


class WorkerProxyFailure(RuntimeError):
    """Worker stopped because its assigned proxy appears unavailable."""


class WorkerRetryRequired(RuntimeError):
    """Worker finished a pass but some wallets still need another fetch pass."""


class PolymarketClient:
    def __init__(
        self,
        delay: float = 0.12,
        timeout: float = 30.0,
        retries: int = 3,
        proxy_url: str | None = None,
    ):
        self.delay = delay
        self.timeout = timeout
        self.retries = retries
        self.proxy_url = str(proxy_url or "").strip() or None
        self._thread_local = threading.local()

    def _requests_session(self):
        if requests is None:
            return None
        session = getattr(self._thread_local, "requests_session", None)
        if session is None:
            session = requests.Session()
            session.trust_env = False
            session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150 Safari/537.36"
                    ),
                    "Accept": "application/json,text/plain,*/*",
                    "Origin": "https://polymarket.com",
                    "Referer": "https://polymarket.com/",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                }
            )
            if self.proxy_url:
                session.proxies.update(
                    {"http": self.proxy_url, "https": self.proxy_url}
                )
            self._thread_local.requests_session = session
        return session

    def _urllib_opener(self):
        opener = getattr(self._thread_local, "urllib_opener", None)
        if opener is None:
            handlers: list[Any] = []
            if self.proxy_url:
                handlers.append(
                    urllib.request.ProxyHandler(
                        {"http": self.proxy_url, "https": self.proxy_url}
                    )
                )
            else:
                # Ignore Windows/system proxy settings in direct worker mode.
                handlers.append(urllib.request.ProxyHandler({}))
            opener = urllib.request.build_opener(*handlers)
            self._thread_local.urllib_opener = opener
        return opener

    def get_json(
        self,
        path: str,
        params: dict[str, Any],
        *,
        delay_override: float | None = None,
        rate_limiter: SlidingWindowRateLimiter | None = None,
    ) -> Any:
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{BASE_URL}{path}?{query}"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Origin": "https://polymarket.com",
            "Referer": "https://polymarket.com/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        request_delay = self.delay if delay_override is None else max(delay_override, 0.0)
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            if rate_limiter is not None:
                rate_limiter.acquire()
            if request_delay:
                time.sleep(request_delay)

            try:
                session = self._requests_session()
                if session is not None:
                    response = session.get(url, timeout=self.timeout)
                    if response.status_code in (429, 500, 502, 503, 504):
                        last_error = RuntimeError(
                            f"temporary HTTP {response.status_code}: {response.text[:200]}"
                        )
                        time.sleep(min(1.5 * (attempt + 1), 12.0))
                        continue
                    response.raise_for_status()
                    return response.json()

                req = urllib.request.Request(url, headers=headers)
                with self._urllib_opener().open(req, timeout=self.timeout) as response:
                    body = response.read().decode("utf-8")
                    return json.loads(body)

            except Exception as exc:
                last_error = exc
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                http_code = getattr(exc, "code", None)
                code = status_code if status_code is not None else http_code
                if code is not None and code not in (429, 500, 502, 503, 504):
                    raise
                if attempt < self.retries:
                    time.sleep(min(1.5 * (attempt + 1), 12.0))

        raise RuntimeError(f"Request failed after retries: {url}: {last_error}")


@dataclass
class WalletSeed:
    proxy_wallet: str
    user_name: str = ""
    x_username: str = ""
    verified_badge: bool = False
    best_pnl: float = 0.0
    best_vol: float = 0.0
    profile_views: int = 0
    leaderboard_hits: int = 0
    best_rank_seen: int = 10**9
    modes: set[str] = field(default_factory=set)

    def update(self, row: dict[str, Any], mode: str) -> None:
        self.user_name = self.user_name or str(row.get("userName") or "")
        self.x_username = self.x_username or str(row.get("xUsername") or "")
        self.verified_badge = bool(self.verified_badge or row.get("verifiedBadge"))
        self.best_pnl = max(self.best_pnl, safe_float(row.get("pnl")))
        self.best_vol = max(self.best_vol, safe_float(row.get("vol")))
        self.profile_views = max(
            self.profile_views,
            int(
                safe_float(
                    row.get("profileViews")
                    or row.get("profileViewCount")
                    or row.get("views")
                    or row.get("viewCount")
                )
            ),
        )
        self.leaderboard_hits += 1
        self.modes.add(mode)
        try:
            rank = int(str(row.get("rank") or "999999").replace(",", ""))
            self.best_rank_seen = min(self.best_rank_seen, rank)
        except ValueError:
            pass


def collect_leaderboard_universe(
    client: PolymarketClient,
    out_dir: Path,
    max_offset: int = 1000,
    limit: int = 50,
) -> dict[str, WalletSeed]:
    raw_path = out_dir / "leaderboard_raw.jsonl"
    fail_path = out_dir / "leaderboard_failed.csv"
    wallets, done_offsets = load_leaderboard_cache(raw_path)
    if wallets:
        print(f"[resume] loaded {len(wallets)} wallets from existing leaderboard cache", flush=True)

    fail_file, fail_writer = open_csv_append(
        fail_path, ["category", "timePeriod", "orderBy", "offset", "error"]
    )
    with raw_path.open("a", encoding="utf-8") as raw_file, fail_file:
        for category in CATEGORIES:
            for period in TIME_PERIODS:
                for order_by in ORDER_BY:
                    mode = f"{category}:{period}:{order_by}"
                    print(f"[leaderboard] {mode}", flush=True)
                    for offset in range(0, max_offset + 1, limit):
                        if (mode, offset) in done_offsets:
                            continue
                        try:
                            rows = client.get_json(
                                "/v1/leaderboard",
                                {
                                    "category": category,
                                    "timePeriod": period,
                                    "orderBy": order_by,
                                    "limit": limit,
                                    "offset": offset,
                                },
                            )
                        except Exception as exc:
                            fail_writer.writerow(
                                {
                                    "category": category,
                                    "timePeriod": period,
                                    "orderBy": order_by,
                                    "offset": offset,
                                    "error": repr(exc),
                                }
                            )
                            print(f"[leaderboard:error] {mode} offset={offset}: {exc}", flush=True)
                            break
                        if not rows:
                            break
                        for row in rows:
                            row["_mode"] = mode
                            row["_offset"] = offset
                            raw_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                            wallet = str(row.get("proxyWallet") or "").lower()
                            if not wallet:
                                continue
                            if wallet not in wallets:
                                wallets[wallet] = WalletSeed(proxy_wallet=wallet)
                            wallets[wallet].update(row, mode)
                        raw_file.flush()
                        write_wallet_universe_csv(wallets, out_dir / "wallet_universe.csv")
                        if len(rows) < limit:
                            break

    write_wallet_universe_csv(wallets, out_dir / "wallet_universe.csv")
    return wallets


def load_leaderboard_cache(raw_path: Path) -> tuple[dict[str, WalletSeed], set[tuple[str, int]]]:
    wallets: dict[str, WalletSeed] = {}
    done_offsets: set[tuple[str, int]] = set()
    if not raw_path.exists():
        return wallets, done_offsets

    with raw_path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            mode = str(row.get("_mode") or "")
            offset = int(safe_float(row.get("_offset"), -1))
            if mode and offset >= 0:
                done_offsets.add((mode, offset))
            wallet = str(row.get("proxyWallet") or "").lower()
            if not wallet:
                continue
            if wallet not in wallets:
                wallets[wallet] = WalletSeed(proxy_wallet=wallet)
            wallets[wallet].update(row, mode)
    return wallets, done_offsets


def write_wallet_universe_csv(wallets: dict[str, WalletSeed], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "proxyWallet",
                "userName",
                "xUsername",
                "verifiedBadge",
                "bestPnl",
                "bestVol",
                "profileViews",
                "roiProxy",
                "leaderboardHits",
                "bestRankSeen",
                "modes",
            ],
        )
        writer.writeheader()
        for wallet in sorted(
            wallets.values(),
            key=lambda item: (item.best_pnl, item.leaderboard_hits),
            reverse=True,
        ):
            roi_proxy = wallet.best_pnl / wallet.best_vol if wallet.best_vol > 0 else 0.0
            writer.writerow(
                {
                    "proxyWallet": wallet.proxy_wallet,
                    "userName": wallet.user_name,
                    "xUsername": wallet.x_username,
                    "verifiedBadge": wallet.verified_badge,
                    "bestPnl": wallet.best_pnl,
                    "bestVol": wallet.best_vol,
                    "profileViews": wallet.profile_views,
                    "roiProxy": roi_proxy,
                    "leaderboardHits": wallet.leaderboard_hits,
                    "bestRankSeen": wallet.best_rank_seen,
                    "modes": "|".join(sorted(wallet.modes)),
                }
            )


def load_wallet_universe(path: Path) -> dict[str, WalletSeed]:
    wallets: dict[str, WalletSeed] = {}

    # utf-8-sig removes a possible UTF-8 BOM from the first CSV header.
    # Without this, the first field can become "\ufeffproxyWallet" and cause KeyError.
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        # Normalize accidental spaces around column names and validate the required field.
        if reader.fieldnames:
            reader.fieldnames = [str(name or "").strip() for name in reader.fieldnames]
        if not reader.fieldnames or "proxyWallet" not in reader.fieldnames:
            raise ValueError(
                f"Invalid wallet universe CSV header in {path}. "
                f"Expected 'proxyWallet'; found: {reader.fieldnames or []}"
            )

        for row in reader:
            wallet = str(row.get("proxyWallet") or "").strip().lower()
            if not wallet:
                continue

            wallets[wallet] = WalletSeed(
                proxy_wallet=wallet,
                user_name=str(row.get("userName") or "").strip(),
                x_username=str(row.get("xUsername") or "").strip(),
                verified_badge=str(row.get("verifiedBadge") or "").strip().lower() == "true",
                best_pnl=safe_float(row.get("bestPnl")),
                best_vol=safe_float(row.get("bestVol")),
                profile_views=int(safe_float(row.get("profileViews"))),
                leaderboard_hits=int(safe_float(row.get("leaderboardHits"))),
                best_rank_seen=int(safe_float(row.get("bestRankSeen"), 10**9)),
                modes=(
                    set(str(row.get("modes") or "").split("|"))
                    if row.get("modes")
                    else set()
                ),
            )
    return wallets


def xlsx_cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t", "")
    if cell_type == "inlineStr":
        return "".join(cell.itertext())
    value = cell.find("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v")
    if value is None or value.text is None:
        return ""
    if cell_type == "s":
        index = int(safe_float(value.text, -1))
        return shared_strings[index] if 0 <= index < len(shared_strings) else ""
    return value.text


def load_xlsx_shared_strings(xlsx: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in xlsx.namelist():
        return []
    root = ET.fromstring(xlsx.read("xl/sharedStrings.xml"))
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    return ["".join(item.itertext()) for item in root.findall(f"{namespace}si")]


def load_xlsx_rows(path: Path) -> list[dict[str, str]]:
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with zipfile.ZipFile(path, "r") as xlsx:
        shared_strings = load_xlsx_shared_strings(xlsx)
        root = ET.fromstring(xlsx.read("xl/worksheets/sheet1.xml"))
    sheet_rows = root.findall(f".//{namespace}row")
    if not sheet_rows:
        return []
    header = [xlsx_cell_text(cell, shared_strings) for cell in sheet_rows[0].findall(f"{namespace}c")]
    rows: list[dict[str, str]] = []
    for sheet_row in sheet_rows[1:]:
        cells = [xlsx_cell_text(cell, shared_strings) for cell in sheet_row.findall(f"{namespace}c")]
        row = {field: cells[index] if index < len(cells) else "" for index, field in enumerate(header)}
        if row.get("proxyWallet"):
            rows.append(row)
    return rows


def load_wallets_from_score_xlsx(path: Path) -> dict[str, WalletSeed]:
    wallets: dict[str, WalletSeed] = {}
    for row in load_xlsx_rows(path):
        wallet = str(row.get("proxyWallet") or "").lower()
        if not wallet:
            continue
        wallets[wallet] = WalletSeed(
            proxy_wallet=wallet,
            user_name=row.get("userName", ""),
            x_username=row.get("xUsername", ""),
            verified_badge=str(row.get("verifiedBadge", "")).lower() in ("1", "true"),
            best_pnl=safe_float(row.get("realizedPnlAfterCosts") or row.get("realizedPnlClosed")),
            best_vol=safe_float(row.get("totalBoughtAfterCosts") or row.get("totalBoughtClosed")),
            profile_views=int(safe_float(row.get("profileViews"))),
            leaderboard_hits=int(safe_float(row.get("leaderboardHits"), 1)),
            best_rank_seen=int(safe_float(row.get("rank"), 10**9)),
            modes=set(str(row.get("modes", "")).split("|")) if row.get("modes") else {"oneShareNetPnlAfterCosts"},
        )
    return wallets


def normalize_condition_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    if re.fullmatch(r"0x[a-f0-9]{64}", text):
        return text
    return ""


def closed_position_unique_key(pos: dict[str, Any]) -> str:
    """Stable identity for one closed-position row (one outcome asset)."""
    asset = str(pos.get("asset") or "").strip().lower()
    if asset:
        return f"asset:{asset}"

    condition_id = normalize_condition_id(pos.get("conditionId"))
    outcome_index = str(pos.get("outcomeIndex") or "").strip()
    outcome = str(pos.get("outcome") or "").strip().lower()
    if condition_id:
        return f"condition:{condition_id}|index:{outcome_index}|outcome:{outcome}"

    return "json:" + json.dumps(pos, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def dedupe_closed_positions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = closed_position_unique_key(row)
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    result.sort(
        key=lambda row: (
            safe_float(row.get("timestamp")),
            normalize_condition_id(row.get("conditionId")),
            str(row.get("asset") or ""),
        )
    )
    return result


class CompleteFetchCache:
    """SQLite resume cache. Writes are local; an optional old DB is read-only fallback."""

    def __init__(self, path: Path, fallback_path: Path | None = None):
        self.path = path
        self.conn = sqlite3.connect(path, timeout=60.0)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self._ensure_schema(self.conn)

        self.fallback_conn: sqlite3.Connection | None = None
        if fallback_path is not None:
            try:
                fallback_resolved = fallback_path.resolve()
                if fallback_path.exists() and fallback_resolved != path.resolve():
                    uri = fallback_resolved.as_uri() + "?mode=ro"
                    self.fallback_conn = sqlite3.connect(uri, uri=True, timeout=60.0)
                    self.fallback_conn.execute("PRAGMA busy_timeout=60000")
            except Exception as exc:
                print(f"[cache:fallback-warning] cannot open {fallback_path}: {exc}", flush=True)

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_state (
                wallet TEXT PRIMARY KEY,
                snapshot_end INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_markets (
                wallet TEXT NOT NULL,
                market TEXT NOT NULL,
                PRIMARY KEY (wallet, market)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS closed_market_rows (
                wallet TEXT NOT NULL,
                market TEXT NOT NULL,
                rows_json TEXT NOT NULL,
                fetched_at INTEGER NOT NULL,
                PRIMARY KEY (wallet, market)
            )
            """
        )
        conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()
        if self.fallback_conn is not None:
            self.fallback_conn.close()

    @staticmethod
    def _snapshot_from(conn: sqlite3.Connection | None, wallet: str) -> int:
        if conn is None:
            return 0
        try:
            row = conn.execute(
                "SELECT snapshot_end FROM activity_state WHERE wallet=?", (wallet,)
            ).fetchone()
            return int(row[0]) if row else 0
        except sqlite3.Error:
            return 0

    def get_activity_snapshot_end(self, wallet: str) -> int:
        return max(
            self._snapshot_from(self.conn, wallet),
            self._snapshot_from(self.fallback_conn, wallet),
        )

    @staticmethod
    def _activity_markets_from(
        conn: sqlite3.Connection | None, wallet: str
    ) -> set[str]:
        if conn is None:
            return set()
        try:
            return {
                str(row[0])
                for row in conn.execute(
                    "SELECT market FROM activity_markets WHERE wallet=?", (wallet,)
                )
            }
        except sqlite3.Error:
            return set()

    def get_activity_markets(self, wallet: str) -> set[str]:
        return self._activity_markets_from(
            self.conn, wallet
        ) | self._activity_markets_from(self.fallback_conn, wallet)

    def merge_activity_markets(
        self, wallet: str, markets: set[str], snapshot_end: int
    ) -> None:
        if markets:
            self.conn.executemany(
                "INSERT OR IGNORE INTO activity_markets(wallet, market) VALUES (?, ?)",
                ((wallet, market) for market in markets),
            )
        self.conn.execute(
            """
            INSERT INTO activity_state(wallet, snapshot_end) VALUES (?, ?)
            ON CONFLICT(wallet) DO UPDATE SET snapshot_end=excluded.snapshot_end
            """,
            (wallet, int(snapshot_end)),
        )
        self.conn.commit()

    @staticmethod
    def _closed_rows_from(
        conn: sqlite3.Connection | None,
        wallet: str,
        markets: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict[str, Any]]] = {}
        if conn is None:
            return result
        for index in range(0, len(markets), 800):
            chunk = markets[index : index + 800]
            if not chunk:
                continue
            placeholders = ",".join("?" for _ in chunk)
            query = (
                "SELECT market, rows_json FROM closed_market_rows "
                f"WHERE wallet=? AND market IN ({placeholders})"
            )
            try:
                cursor = conn.execute(query, (wallet, *chunk))
            except sqlite3.Error:
                continue
            for market, rows_json in cursor:
                try:
                    rows = json.loads(rows_json)
                except json.JSONDecodeError:
                    continue
                if isinstance(rows, list):
                    result[str(market)] = rows
        return result

    def get_closed_rows(
        self, wallet: str, markets: list[str]
    ) -> dict[str, list[dict[str, Any]]]:
        result = self._closed_rows_from(self.conn, wallet, markets)
        missing = [market for market in markets if market not in result]
        if missing and self.fallback_conn is not None:
            result.update(self._closed_rows_from(self.fallback_conn, wallet, missing))
        return result

    def upsert_closed_rows(
        self,
        wallet: str,
        rows_by_market: dict[str, list[dict[str, Any]]],
        fetched_at: int | None = None,
    ) -> None:
        if not rows_by_market:
            return
        timestamp = int(time.time()) if fetched_at is None else int(fetched_at)
        self.conn.executemany(
            """
            INSERT INTO closed_market_rows(wallet, market, rows_json, fetched_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(wallet, market) DO UPDATE SET
                rows_json=excluded.rows_json,
                fetched_at=excluded.fetched_at
            """,
            (
                (
                    wallet,
                    market,
                    json.dumps(rows, ensure_ascii=False, separators=(",", ":")),
                    timestamp,
                )
                for market, rows in rows_by_market.items()
            ),
        )
        self.conn.commit()


def fetch_official_traded_count(client: PolymarketClient, wallet: str) -> int:
    data = client.get_json(
        "/traded",
        {"user": wallet},
        delay_override=0.0,
        rate_limiter=ACTIVITY_API_LIMITER,
    )
    if not isinstance(data, dict) or "traded" not in data:
        raise RuntimeError(f"Unexpected /traded response: {data!r}")
    return int(safe_float(data.get("traded")))


def activity_request(
    client: PolymarketClient,
    wallet: str,
    start_ts: int,
    end_ts: int,
    offset: int,
) -> list[dict[str, Any]]:
    rows = client.get_json(
        "/activity",
        {
            "user": wallet,
            "type": "TRADE",
            "start": max(int(start_ts), 1),
            "end": max(int(end_ts), 1),
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
            "limit": ACTIVITY_PAGE_LIMIT,
            "offset": offset,
        },
        delay_override=0.0,
        rate_limiter=ACTIVITY_API_LIMITER,
    )
    if not isinstance(rows, list):
        raise RuntimeError(
            f"Unexpected /activity response for {start_ts}..{end_ts} offset={offset}: "
            f"{type(rows).__name__}"
        )
    return rows


def markets_from_activity_rows(rows: list[dict[str, Any]]) -> set[str]:
    # Combo trades use a separate positions/activity API and must not be mixed
    # with standard /closed-positions coverage.
    result: set[str] = set()
    for row in rows:
        is_combo = str(row.get("isCombo") or "").strip().lower() in {
            "1", "true", "yes"
        }
        if is_combo:
            continue
        market = normalize_condition_id(row.get("conditionId"))
        if market:
            result.add(market)
    return result


def fetch_activity_markets_range(
    client: PolymarketClient,
    wallet: str,
    start_ts: int,
    end_ts: int,
) -> set[str]:
    """Complete, stable activity scan using adaptive timestamp windows."""
    if end_ts <= start_ts:
        return set()

    markets: set[str] = set()
    stack: list[tuple[int, int]] = [(max(start_ts, 1), end_ts)]
    completed_windows = 0

    while stack:
        window_start, window_end = stack.pop()
        first_rows = activity_request(
            client, wallet, window_start, window_end, offset=0
        )
        markets.update(markets_from_activity_rows(first_rows))
        if len(first_rows) < ACTIVITY_PAGE_LIMIT:
            completed_windows += 1
            continue

        last_rows = activity_request(
            client,
            wallet,
            window_start,
            window_end,
            offset=ACTIVITY_MAX_OFFSET,
        )
        if len(last_rows) == ACTIVITY_PAGE_LIMIT:
            midpoint = window_start + (window_end - window_start) // 2
            if midpoint <= window_start or midpoint >= window_end:
                raise RuntimeError(
                    "One-second Activity window exceeds offset=5000; "
                    "cannot prove complete market discovery."
                )
            # One-second overlap prevents boundary loss; set dedupe removes repeats.
            stack.append((max(midpoint - 1, window_start), window_end))
            stack.append((window_start, min(midpoint + 1, window_end)))
            continue

        markets.update(markets_from_activity_rows(last_rows))
        middle_offsets = list(
            range(ACTIVITY_PAGE_LIMIT, ACTIVITY_MAX_OFFSET, ACTIVITY_PAGE_LIMIT)
        )
        with ThreadPoolExecutor(max_workers=ACTIVITY_FETCH_WORKERS) as executor:
            future_map = {
                executor.submit(
                    activity_request,
                    client,
                    wallet,
                    window_start,
                    window_end,
                    offset,
                ): offset
                for offset in middle_offsets
            }
            for future in as_completed(future_map):
                rows = future.result()
                markets.update(markets_from_activity_rows(rows))

        completed_windows += 1
        if completed_windows % 20 == 0:
            print(
                f"    [activity-markets] {wallet} windows={completed_windows} "
                f"unique_markets={len(markets)}",
                flush=True,
            )

    return markets


def get_complete_activity_markets(
    client: PolymarketClient,
    wallet: str,
    cache: CompleteFetchCache,
) -> tuple[set[str], int, set[str]]:
    cached_markets = cache.get_activity_markets(wallet)
    previous_end = cache.get_activity_snapshot_end(wallet)
    snapshot_end = int(time.time())
    start_ts = max(previous_end - 1, 1) if previous_end else 1

    print(
        f"    [activity-markets] {wallet} cached={len(cached_markets)} "
        f"scan={start_ts}..{snapshot_end}",
        flush=True,
    )
    new_markets = fetch_activity_markets_range(
        client, wallet, start_ts, snapshot_end
    )
    all_markets = cached_markets | new_markets
    cache.merge_activity_markets(wallet, new_markets, snapshot_end)
    print(
        f"    [activity-markets:done] {wallet} total={len(all_markets)} "
        f"new={len(new_markets - cached_markets)}",
        flush=True,
    )
    return all_markets, snapshot_end, new_markets


def fetch_current_positions_complete(
    client: PolymarketClient,
    wallet: str,
) -> tuple[list[dict[str, Any]], bool]:
    rows_all: list[dict[str, Any]] = []
    seen: set[str] = set()
    ended_short = False
    for offset in range(
        0, CURRENT_POSITION_MAX_OFFSET + 1, CURRENT_POSITION_PAGE_LIMIT
    ):
        rows = client.get_json(
            "/positions",
            {
                "user": wallet,
                "sizeThreshold": 0,
                "limit": CURRENT_POSITION_PAGE_LIMIT,
                "offset": offset,
                "sortBy": "TITLE",
                "sortDirection": "ASC",
            },
            delay_override=0.0,
            rate_limiter=CLOSED_API_LIMITER,
        )
        if not isinstance(rows, list):
            raise RuntimeError(
                f"Unexpected /positions response at offset={offset}: "
                f"{type(rows).__name__}"
            )
        for row in rows:
            key = closed_position_unique_key(row)
            if key not in seen:
                seen.add(key)
                rows_all.append(row)
        if len(rows) < CURRENT_POSITION_PAGE_LIMIT:
            ended_short = True
            break
    return rows_all, ended_short


def fetch_closed_positions_direct_asc(
    client: PolymarketClient,
    wallet: str,
    max_positions: int,
) -> tuple[list[dict[str, Any]], bool, str]:
    positions: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    no_new_streak = 0
    requested_cap = max(int(max_positions), 0)
    cap_is_user_limited = requested_cap < CLOSED_POSITIONS_MAX_API_OFFSET + 50
    effective_cap = min(
        requested_cap,
        CLOSED_POSITIONS_MAX_API_OFFSET + 50,
    )

    for offset in range(0, effective_cap, 50):
        rows = client.get_json(
            "/closed-positions",
            {
                "user": wallet,
                "limit": 50,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "ASC",
            },
            delay_override=0.0,
            rate_limiter=CLOSED_API_LIMITER,
        )
        if not isinstance(rows, list):
            raise RuntimeError(
                f"Unexpected /closed-positions response at offset={offset}: "
                f"{type(rows).__name__}"
            )

        new_count = 0
        for row in rows:
            key = closed_position_unique_key(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            positions.append(row)
            new_count += 1

        if offset == 0 or offset % 2500 == 0 or new_count != len(rows):
            print(
                f"    [positions-direct] {wallet} offset={offset} rows={len(rows)} "
                f"new={new_count} unique={len(positions)}",
                flush=True,
            )

        if len(rows) < 50:
            return dedupe_closed_positions(positions), True, "direct-asc-short-page"

        if new_count == 0:
            no_new_streak += 1
        else:
            no_new_streak = 0
        if no_new_streak >= 20:
            return dedupe_closed_positions(positions), False, "direct-asc-repeated-pages"

        if len(positions) >= requested_cap:
            return (
                dedupe_closed_positions(positions[:requested_cap]),
                False,
                "user-position-cap" if cap_is_user_limited else "api-offset-cap",
            )

    return dedupe_closed_positions(positions), False, "api-offset-cap"


def chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def fetch_closed_market_batch(
    client: PolymarketClient,
    wallet: str,
    markets: list[str],
) -> dict[str, list[dict[str, Any]]]:
    requested = set(markets)
    rows_by_market: dict[str, list[dict[str, Any]]] = {
        market: [] for market in markets
    }
    seen_keys: set[str] = set()

    for offset in range(0, CLOSED_POSITIONS_MAX_API_OFFSET + 1, 50):
        rows = client.get_json(
            "/closed-positions",
            {
                "user": wallet,
                "market": ",".join(markets),
                "limit": 50,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "ASC",
            },
            delay_override=0.0,
            rate_limiter=CLOSED_API_LIMITER,
        )
        if not isinstance(rows, list):
            raise RuntimeError(
                f"Unexpected market-filtered /closed-positions response: "
                f"{type(rows).__name__}"
            )

        for row in rows:
            market = normalize_condition_id(row.get("conditionId"))
            if market not in requested:
                raise RuntimeError(
                    f"API returned market outside requested batch: {market}"
                )
            key = closed_position_unique_key(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            rows_by_market[market].append(row)

        if len(rows) < 50:
            return rows_by_market

    raise RuntimeError(
        "A market-filtered batch reached offset=100000 without a short page; "
        "batch completeness cannot be proven."
    )


def fetch_closed_markets_parallel(
    client: PolymarketClient,
    wallet: str,
    markets: set[str],
    cache: CompleteFetchCache,
    *,
    force_refresh: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    market_list = sorted(markets)
    cached = {} if force_refresh else cache.get_closed_rows(wallet, market_list)
    missing = [market for market in market_list if market not in cached]
    result = dict(cached)

    batches = chunked(missing, CLOSED_MARKET_BATCH_SIZE)
    if not batches:
        return result

    print(
        f"    [market-batches] {wallet} markets={len(market_list)} "
        f"cached={len(cached)} fetch={len(missing)} batches={len(batches)} "
        f"workers={CLOSED_FETCH_WORKERS}",
        flush=True,
    )

    completed = 0
    with ThreadPoolExecutor(max_workers=CLOSED_FETCH_WORKERS) as executor:
        future_map = {
            executor.submit(fetch_closed_market_batch, client, wallet, batch): batch
            for batch in batches
        }
        for future in as_completed(future_map):
            rows_by_market = future.result()
            result.update(rows_by_market)
            cache.upsert_closed_rows(wallet, rows_by_market)
            completed += 1
            if completed == 1 or completed % 100 == 0 or completed == len(batches):
                fetched_rows = sum(len(rows) for rows in result.values())
                print(
                    f"    [market-batches:progress] {wallet} "
                    f"{completed}/{len(batches)} cached_markets={len(result)} "
                    f"closed_rows={fetched_rows}",
                    flush=True,
                )

    return result


def flatten_closed_market_rows(
    rows_by_market: dict[str, list[dict[str, Any]]]
) -> list[dict[str, Any]]:
    return dedupe_closed_positions(
        [row for rows in rows_by_market.values() for row in rows]
    )


def fetch_closed_positions_market_complete(
    client: PolymarketClient,
    wallet: str,
    max_positions: int,
    cache: CompleteFetchCache,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    initial_current, initial_current_complete = fetch_current_positions_complete(
        client, wallet
    )
    if not initial_current_complete:
        raise RuntimeError(
            "Current positions reached offset=10000; coverage cannot be proven."
        )
    initial_current_markets = {
        market
        for row in initial_current
        if (market := normalize_condition_id(row.get("conditionId")))
    }

    activity_markets, snapshot_end, _ = get_complete_activity_markets(
        client, wallet, cache
    )
    if not activity_markets:
        return [], {
            "complete": True,
            "fetchMethod": "activity-market-batches",
            "activityMarkets": 0,
            "closedMarkets": 0,
            "currentMarkets": len(initial_current_markets),
            "snapshotEnd": snapshot_end,
        }

    rows_by_market = fetch_closed_markets_parallel(
        client, wallet, activity_markets, cache
    )

    # Catch markets created while the long scan was running.
    catchup_end = int(time.time())
    catchup_markets = fetch_activity_markets_range(
        client, wallet, max(snapshot_end - 1, 1), catchup_end
    )
    if catchup_markets:
        activity_markets |= catchup_markets
        cache.merge_activity_markets(wallet, catchup_markets, catchup_end)
        rows_by_market.update(
            fetch_closed_markets_parallel(
                client, wallet, catchup_markets, cache, force_refresh=True
            )
        )

    current_rows, current_complete = fetch_current_positions_complete(client, wallet)
    if not current_complete:
        raise RuntimeError(
            "Current positions reached offset=10000 during final verification."
        )
    current_markets = {
        market
        for row in current_rows
        if (market := normalize_condition_id(row.get("conditionId")))
    }

    # Refresh markets that were/currently are open, because they can close while
    # the multi-minute scan is running.
    volatile_markets = initial_current_markets | current_markets | catchup_markets
    if volatile_markets:
        rows_by_market.update(
            fetch_closed_markets_parallel(
                client, wallet, volatile_markets, cache, force_refresh=True
            )
        )

    missing_markets: set[str] = set()
    for repair_pass in range(1, COVERAGE_REPAIR_PASSES + 1):
        closed_markets = {
            market for market, rows in rows_by_market.items() if rows
        }
        missing_markets = activity_markets - closed_markets - current_markets
        if not missing_markets:
            break
        print(
            f"    [coverage-repair] {wallet} pass={repair_pass} "
            f"missing_markets={len(missing_markets)}",
            flush=True,
        )
        rows_by_market.update(
            fetch_closed_markets_parallel(
                client, wallet, missing_markets, cache, force_refresh=True
            )
        )
        current_rows, current_complete = fetch_current_positions_complete(
            client, wallet
        )
        if not current_complete:
            raise RuntimeError(
                "Current positions became unpageable during coverage repair."
            )
        current_markets = {
            market
            for row in current_rows
            if (market := normalize_condition_id(row.get("conditionId")))
        }

    closed_positions = flatten_closed_market_rows(rows_by_market)
    closed_markets = {
        market for market, rows in rows_by_market.items() if rows
    }
    activity_only_markets = activity_markets - closed_markets - current_markets
    activity_only_sample = sorted(activity_only_markets)[:20]
    if activity_only_markets:
        print(
            f"    [coverage-warning] {wallet} activity_only_markets="
            f"{len(activity_only_markets)}; all market batches completed, "
            "so scoring continues",
            flush=True,
        )

    requested_cap = max(int(max_positions), 0)
    cap_applied = requested_cap < len(closed_positions)
    if cap_applied:
        closed_positions = closed_positions[:requested_cap]

    metadata = {
        "complete": not cap_applied,
        "fetchMethod": "activity-market-batches",
        "activityMarkets": len(activity_markets),
        "closedMarkets": len(closed_markets),
        "currentMarkets": len(current_markets),
        "closedPositions": len(closed_positions),
        "snapshotEnd": catchup_end,
        "missingMarkets": len(activity_only_markets),
        "activityOnlyMarkets": len(activity_only_markets),
        "activityOnlyMarketSample": activity_only_sample,
        "coverageWarning": bool(activity_only_markets),
        "capApplied": cap_applied,
    }
    print(
        f"    [positions-complete] {wallet} closed_positions={len(closed_positions)} "
        f"activity_markets={len(activity_markets)} closed_markets={len(closed_markets)} "
        f"current_markets={len(current_markets)} "
        f"activity_only_markets={len(activity_only_markets)}",
        flush=True,
    )
    return closed_positions, metadata


def fetch_closed_positions(
    client: PolymarketClient,
    wallet: str,
    limit: int = 50,
    max_positions: int = 100000,
    progress_every: int = 500,
    page_cache: dict[str, dict[int, list[dict[str, Any]]]] | None = None,
    page_cache_file=None,
    complete_cache: CompleteFetchCache | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Hybrid fetch: quick direct path for small wallets, complete market batches for large ones."""
    del limit, progress_every, page_cache, page_cache_file

    if complete_cache is None:
        raise RuntimeError("CompleteFetchCache is required for accurate mode-2 fetching.")

    if not COMPLETE_CLOSED_POSITION_FETCH:
        positions, complete, reason = fetch_closed_positions_direct_asc(
            client, wallet, max_positions
        )
        return positions, {
            "complete": complete,
            "fetchMethod": reason,
            "closedPositions": len(positions),
        }

    traded_count = fetch_official_traded_count(client, wallet)
    print(
        f"    [traded] {wallet} official_markets={traded_count}",
        flush=True,
    )

    requested_cap = max(int(max_positions), 0)
    user_requested_small_cap = requested_cap <= CLOSED_POSITIONS_MAX_API_OFFSET
    if traded_count <= DIRECT_FAST_PATH_MAX_TRADED_MARKETS or user_requested_small_cap:
        positions, complete, reason = fetch_closed_positions_direct_asc(
            client, wallet, max_positions
        )
        if complete:
            metadata = {
                "complete": True,
                "fetchMethod": reason,
                "officialTradedMarkets": traded_count,
                "closedPositions": len(positions),
            }
            print(
                f"    [positions:done] {wallet} total={len(positions)} "
                f"method={reason}",
                flush=True,
            )
            return positions, metadata
        print(
            f"    [positions:fallback] {wallet} direct method incomplete ({reason}); "
            "switching to activity + market batches",
            flush=True,
        )

    positions, metadata = fetch_closed_positions_market_complete(
        client, wallet, max_positions, complete_cache
    )
    metadata["officialTradedMarkets"] = traded_count
    if not metadata.get("complete"):
        raise RuntimeError(
            "MAX_POSITIONS_PER_WALLET truncated the wallet; complete scoring refused."
        )
    return positions, metadata


def wilson_lower_bound(wins: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    phat = wins / total
    denom = 1 + z * z / total
    centre = phat + z * z / (2 * total)
    margin = z * math.sqrt((phat * (1 - phat) + z * z / (4 * total)) / total)
    return (centre - margin) / denom


def parse_timestamp(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        return timestamp / 1000.0 if timestamp > 10_000_000_000 else timestamp
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d+(\.\d+)?", text):
        timestamp = float(text)
        return timestamp / 1000.0 if timestamp > 10_000_000_000 else timestamp
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def first_timestamp(pos: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        timestamp = parse_timestamp(pos.get(key))
        if timestamp is not None:
            return timestamp
    return None


def trade_timestamp(pos: dict[str, Any]) -> float | None:
    return first_timestamp(
        pos,
        [
            "timestamp",
            "closeTimestamp",
            "closedAt",
            "resolvedAt",
            "redeemedAt",
            "updatedAt",
            "openTimestamp",
            "openedAt",
            "createdAt",
            "created",
        ],
    )


def trade_day_key(pos: dict[str, Any]) -> str:
    timestamp = trade_timestamp(pos)
    if timestamp is None:
        return "unknown"
    return datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d")


def current_position_value(pos: dict[str, Any]) -> float:
    return safe_float(
        pos.get("currentValue")
        or pos.get("curValue")
        or pos.get("value")
        or pos.get("cashPnl")
        or pos.get("realizedPnl")
    )


def first_float(pos: dict[str, Any], keys: list[str], default: float = 0.0) -> float:
    for key in keys:
        if key in pos and pos.get(key) not in (None, ""):
            return safe_float(pos.get(key), default)
    return default


def position_category(pos: dict[str, Any]) -> str:
    for key in ("category", "eventCategory", "marketCategory", "tag", "topic"):
        value = str(pos.get(key) or "").strip()
        if value:
            return value.upper()
    return ""


def fee_rate_for_position(pos: dict[str, Any]) -> float:
    category = position_category(pos)
    return POLYMARKET_FEE_RATES.get(category, DEFAULT_FEE_RATE)


def estimate_shares(pos: dict[str, Any], entry_price: float) -> float:
    shares = first_float(pos, ["shares", "size", "quantity", "qty", "totalShares", "amount"])
    if shares > 0:
        return shares
    bought = safe_float(pos.get("totalBought"))
    if bought > 0 and entry_price > 0:
        return bought / entry_price
    return 0.0


def copy_entry_price(wallet_entry_price: float) -> float:
    if not USE_ASSUMED_SPREAD:
        return wallet_entry_price
    return min(wallet_entry_price + ASSUMED_SPREAD, 1.0)


def copy_exit_price(wallet_exit_price: float) -> float:
    if not USE_ASSUMED_SPREAD:
        return wallet_exit_price
    return max(wallet_exit_price - ASSUMED_SPREAD, 0.0)


def polymarket_fee(shares: float, fee_rate: float, price: float) -> float:
    if not USE_POLYMARKET_FEES:
        return 0.0
    return shares * fee_rate * price * (1.0 - price)


def adjusted_position_costs(pos: dict[str, Any], shares_override: float | None = None) -> dict[str, float]:
    wallet_entry_price = min(max(safe_float(pos.get("avgPrice")), 0.0), 1.0)
    entry_price = copy_entry_price(wallet_entry_price)
    shares = shares_override if shares_override is not None else estimate_shares(pos, wallet_entry_price)
    fee_rate = fee_rate_for_position(pos)
    entry_fee = polymarket_fee(shares, fee_rate, entry_price)
    exit_fee = 0.0

    wallet_exit_raw = first_float(
        pos,
        ["exitPrice", "avgExitPrice", "avgSellPrice", "sellPrice", "closedPrice", "redeemPrice"],
        default=-1.0,
    )
    if wallet_exit_raw >= 0.0:
        exit_price = copy_exit_price(min(max(wallet_exit_raw, 0.0), 1.0))
        exit_fee = polymarket_fee(shares, fee_rate, exit_price)
        gross_pnl = shares * (exit_price - entry_price)
    else:
        payout = 1.0 if safe_float(pos.get("realizedPnl")) > 0 else 0.0
        gross_pnl = shares * (payout - entry_price)

    net_pnl = gross_pnl - entry_fee - exit_fee
    return {
        "walletEntryPrice": wallet_entry_price,
        "copyEntryPrice": entry_price,
        "assumedSpread": ASSUMED_SPREAD if USE_ASSUMED_SPREAD else 0.0,
        "feeRate": fee_rate,
        "shares": shares,
        "entryFee": entry_fee,
        "exitFee": exit_fee,
        "grossPnl": gross_pnl,
        "netPnlAfterFeesAndSpread": net_pnl,
    }


def score_positions(positions: list[dict[str, Any]], smoothing: float = 1.0) -> dict[str, Any]:
    wins = 0
    losses = 0
    breakeven = 0
    sum_win_edge = 0.0
    sum_loss_risk = 0.0
    sum_win_edge_sq = 0.0
    sum_loss_risk_sq = 0.0
    realized_pnl = 0.0
    realized_pnl_after_costs = 0.0
    total_bought = 0.0
    total_bought_after_costs = 0.0
    one_share_net_pnl_after_costs = 0.0
    one_share_total_cost_after_costs = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    gross_profit_after_costs = 0.0
    gross_loss_after_costs = 0.0
    total_entry_fees = 0.0
    total_exit_fees = 0.0
    total_assumed_spread_cost = 0.0
    sum_wallet_entry_price = 0.0
    sum_copy_entry_price = 0.0
    sum_fee_rate = 0.0
    costed_positions = 0
    sum_resolved_entry_price = 0.0
    current_consecutive_wins = 0
    current_consecutive_losses = 0
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    pnl_series: list[float] = []
    equity = 0.0
    peak_equity = 0.0
    max_drawdown = 0.0
    recent_balance_values: list[float] = []
    recent_activity_count = 0
    short_hold_count = 0
    hold_duration_count = 0
    trades_by_day: dict[str, int] = {}
    first_trade_ts: float | None = None
    last_trade_ts: float | None = None
    now_ts = time.time()
    recent_cutoff = now_ts - RECENT_ACTIVITY_DAYS * 24 * 60 * 60
    short_hold_seconds = SHORT_HOLD_MAX_HOURS * 60 * 60

    for pos in positions:
        position_trade_ts = trade_timestamp(pos)
        if position_trade_ts is not None:
            first_trade_ts = (
                position_trade_ts if first_trade_ts is None else min(first_trade_ts, position_trade_ts)
            )
            last_trade_ts = (
                position_trade_ts if last_trade_ts is None else max(last_trade_ts, position_trade_ts)
            )
        day_key = trade_day_key(pos)
        trades_by_day[day_key] = trades_by_day.get(day_key, 0) + 1
        pnl = safe_float(pos.get("realizedPnl"))
        costs = adjusted_position_costs(pos)
        one_share_costs = adjusted_position_costs(pos, shares_override=1.0)
        net_pnl = costs["netPnlAfterFeesAndSpread"]
        bought = safe_float(pos.get("totalBought"))
        realized_pnl += pnl
        realized_pnl_after_costs += net_pnl
        one_share_net_pnl_after_costs += one_share_costs["netPnlAfterFeesAndSpread"]
        one_share_total_cost_after_costs += one_share_costs["copyEntryPrice"] + one_share_costs["entryFee"]
        total_entry_fees += costs["entryFee"]
        total_exit_fees += costs["exitFee"]
        total_assumed_spread_cost += costs["shares"] * max(costs["copyEntryPrice"] - costs["walletEntryPrice"], 0.0)
        sum_wallet_entry_price += costs["walletEntryPrice"]
        sum_copy_entry_price += costs["copyEntryPrice"]
        sum_fee_rate += costs["feeRate"]
        costed_positions += 1
        total_bought += bought
        total_bought_after_costs += costs["shares"] * costs["copyEntryPrice"] + costs["entryFee"]
        pnl_series.append(net_pnl)
        equity += net_pnl
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)
        recent_balance_values.append(current_position_value(pos))

        open_ts = first_timestamp(pos, ["openTimestamp", "openedAt", "createdAt", "timestamp", "created"])
        close_ts = first_timestamp(
            pos,
            ["closeTimestamp", "closedAt", "resolvedAt", "redeemedAt", "updatedAt", "timestamp"],
        )
        if (open_ts is not None and open_ts >= recent_cutoff) or (
            close_ts is not None and close_ts >= recent_cutoff
        ):
            recent_activity_count += 1
        if open_ts is not None and close_ts is not None and close_ts >= open_ts:
            hold_duration_count += 1
            if close_ts - open_ts < short_hold_seconds:
                short_hold_count += 1

        if pnl > 0:
            wins += 1
            sum_resolved_entry_price += costs["copyEntryPrice"]
            current_consecutive_wins += 1
            current_consecutive_losses = 0
            max_consecutive_wins = max(max_consecutive_wins, current_consecutive_wins)
            edge = max(0.0, 1.0 - costs["copyEntryPrice"])
            sum_win_edge += edge
            sum_win_edge_sq += edge * edge
        elif pnl < 0:
            losses += 1
            sum_resolved_entry_price += costs["copyEntryPrice"]
            current_consecutive_losses += 1
            current_consecutive_wins = 0
            max_consecutive_losses = max(max_consecutive_losses, current_consecutive_losses)
            risk = max(0.0, costs["copyEntryPrice"])
            sum_loss_risk += risk
            sum_loss_risk_sq += risk * risk
        else:
            breakeven += 1
            current_consecutive_wins = 0
            current_consecutive_losses = 0
        if pnl > 0:
            gross_profit += pnl
        elif pnl < 0:
            gross_loss += abs(pnl)
        if net_pnl > 0:
            gross_profit_after_costs += net_pnl
        elif net_pnl < 0:
            gross_loss_after_costs += abs(net_pnl)

    resolved = wins + losses
    net_edge = sum_win_edge - sum_loss_risk
    edge_rally_denominator = sum_loss_risk_sq if losses > 0 and sum_loss_risk_sq > 0 else smoothing
    edge_rally_raw = sum_win_edge_sq / edge_rally_denominator
    rally_times_net_edge = edge_rally_raw * net_edge
    rally_times_one_share_net_pnl = edge_rally_raw * one_share_net_pnl_after_costs
    edge_rally = rally_times_net_edge
    net_edge_score = rally_times_net_edge
    win_rate = wins / resolved if resolved else 0.0
    avg_resolved_entry_price = sum_resolved_entry_price / resolved if resolved else 0.0
    adjusted_win_rate = wilson_lower_bound(wins, resolved) - avg_resolved_entry_price
    roi_closed = realized_pnl / total_bought if total_bought > 0 else 0.0
    roi_after_costs = (
        realized_pnl_after_costs / total_bought_after_costs if total_bought_after_costs > 0 else 0.0
    )
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0
    profit_factor_after_costs = (
        gross_profit_after_costs / gross_loss_after_costs if gross_loss_after_costs > 0 else 0.0
    )
    recovery_factor = realized_pnl_after_costs / max_drawdown if max_drawdown > 0 else 0.0
    net_edge_to_max_drawdown = net_edge / max_drawdown if max_drawdown > 0 else 0.0
    short_hold_ratio = short_hold_count / hold_duration_count if hold_duration_count else 0.0
    all_recent_balances_negative = bool(recent_balance_values) and all(
        value < 0 for value in recent_balance_values
    )
    expected_payoff = realized_pnl_after_costs / resolved if resolved else 0.0
    trading_days = len(trades_by_day)
    average_trades_per_day = len(positions) / trading_days if trading_days else 0.0
    max_trades_in_one_day = max(trades_by_day.values()) if trades_by_day else 0
    days_since_last_trade = (now_ts - last_trade_ts) / (24 * 60 * 60) if last_trade_ts is not None else 0.0
    if first_trade_ts is not None and last_trade_ts is not None:
        first_trade_day = datetime.utcfromtimestamp(first_trade_ts).date()
        last_trade_day = datetime.utcfromtimestamp(last_trade_ts).date()
        calendar_trade_span_days = max((last_trade_day - first_trade_day).days + 1, 1)
    else:
        calendar_trade_span_days = 0
    trades_per_calendar_day_first_to_last = (
        len(positions) / calendar_trade_span_days if calendar_trade_span_days else 0.0
    )
    profit_per_trade_after_costs = realized_pnl_after_costs / len(positions) if positions else 0.0
    profit_per_trade_times_win_rate_after_costs = profit_per_trade_after_costs * win_rate
    profit_per_trade_times_net_edge_after_costs = profit_per_trade_after_costs * net_edge
    profit_per_trade_times_one_share_net_pnl_after_costs = (
        profit_per_trade_after_costs * one_share_net_pnl_after_costs
    )
    rally_times_net_edge_times_profit_per_trade_after_costs = (
        rally_times_net_edge * profit_per_trade_after_costs
    )
    one_share_average_daily_cost_after_costs = (
        one_share_total_cost_after_costs / trading_days if trading_days else 0.0
    )
    if len(pnl_series) > 1:
        mean_pnl = sum(pnl_series) / len(pnl_series)
        variance = sum((pnl - mean_pnl) ** 2 for pnl in pnl_series) / (len(pnl_series) - 1)
        std_pnl = math.sqrt(variance)
        sharpe_ratio = mean_pnl / std_pnl * math.sqrt(len(pnl_series)) if std_pnl > 0 else 0.0
    else:
        sharpe_ratio = 0.0

    return {
        "positions": len(positions),
        "resolvedPositions": resolved,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "winRate": win_rate,
        "adjustedWinRate": adjusted_win_rate,
        "sumWinEdge": sum_win_edge,
        "sumLossRisk": sum_loss_risk,
        "sumWinEdgeSq": sum_win_edge_sq,
        "sumLossRiskSq": sum_loss_risk_sq,
        "edgeRallyDenominator": edge_rally_denominator,
        "netEdge": net_edge,
        "netEdgeScore": net_edge_score,
        "edgeRallyRaw": edge_rally_raw,
        "edgeRally": edge_rally,
        "rallyTimesNetEdge": rally_times_net_edge,
        "rallyTimesOneShareNetPnlAfterCosts": rally_times_one_share_net_pnl,
        "realizedPnlClosed": realized_pnl_after_costs,
        "realizedPnlClosedRaw": realized_pnl,
        "realizedPnlAfterCosts": realized_pnl_after_costs,
        "totalBoughtClosed": total_bought_after_costs,
        "totalBoughtClosedRaw": total_bought,
        "totalBoughtAfterCosts": total_bought_after_costs,
        "oneShareNetPnlAfterCosts": one_share_net_pnl_after_costs,
        "oneShareTotalCostAfterCosts": one_share_total_cost_after_costs,
        "oneShareAverageDailyCostAfterCosts": one_share_average_daily_cost_after_costs,
        "roiClosed": roi_after_costs,
        "roiRaw": roi_closed,
        "roiAfterCosts": roi_after_costs,
        "maxDrawdown": max_drawdown,
        "maxDrawdownAfterCosts": max_drawdown,
        "profitFactor": profit_factor_after_costs,
        "profitFactorRaw": profit_factor,
        "profitFactorAfterCosts": profit_factor_after_costs,
        "recoveryFactor": recovery_factor,
        "netEdgeToMaxDrawdown": net_edge_to_max_drawdown,
        "sharpeRatio": sharpe_ratio,
        "expectedPayoff": expected_payoff,
        "expectedPayoffAfterCosts": expected_payoff,
        "profitPerTradeAfterCosts": profit_per_trade_after_costs,
        "profitPerTradeTimesWinRateAfterCosts": profit_per_trade_times_win_rate_after_costs,
        "profitPerTradeTimesNetEdgeAfterCosts": profit_per_trade_times_net_edge_after_costs,
        "profitPerTradeTimesOneShareNetPnlAfterCosts": (
            profit_per_trade_times_one_share_net_pnl_after_costs
        ),
        "rallyTimesNetEdgeTimesProfitPerTradeAfterCosts": (
            rally_times_net_edge_times_profit_per_trade_after_costs
        ),
        "maxConsecutiveWins": max_consecutive_wins,
        "maxConsecutiveLosses": max_consecutive_losses,
        "grossProfit": gross_profit_after_costs,
        "grossLoss": gross_loss_after_costs,
        "grossProfitRaw": gross_profit,
        "grossLossRaw": gross_loss,
        "grossProfitAfterCosts": gross_profit_after_costs,
        "grossLossAfterCosts": gross_loss_after_costs,
        "walletEntryPrice": sum_wallet_entry_price / costed_positions if costed_positions else 0.0,
        "copyEntryPrice": sum_copy_entry_price / costed_positions if costed_positions else 0.0,
        "feeRate": sum_fee_rate / costed_positions if costed_positions else DEFAULT_FEE_RATE,
        "entryFee": total_entry_fees,
        "exitFee": total_exit_fees,
        "assumedSpread": ASSUMED_SPREAD if USE_ASSUMED_SPREAD else 0.0,
        "assumedSpreadCost": total_assumed_spread_cost,
        "recentActivityCount": recent_activity_count,
        "tradingDays": trading_days,
        "averageTradesPerDay": average_trades_per_day,
        "maxTradesInOneDay": max_trades_in_one_day,
        "daysSinceLastTrade": days_since_last_trade,
        "tradesPerCalendarDayFirstToLast": trades_per_calendar_day_first_to_last,
        "shortHoldCount": short_hold_count,
        "holdDurationCount": hold_duration_count,
        "shortHoldRatio": short_hold_ratio,
        "allRecentBalancesNegative": all_recent_balances_negative,
    }


def rank_wallets(
    client: PolymarketClient,
    wallets: dict[str, WalletSeed],
    out_dir: Path,
    min_positions: int,
    min_losses: int,
    min_pnl: float,
    smoothing: float,
    max_wallets: int | None,
    max_positions_per_wallet: int,
    preserve_wallet_order: bool = False,
    shard_count: int = 1,
    shard_index: int = 0,
    fallback_out_dir: Path | None = None,
    skip_final_xlsx: bool = False,
) -> None:
    remove_obsolete_trade_dedup_files(out_dir)
    raw_path = out_dir / RAW_CLOSED_POSITIONS_LOG_FILE_NAME
    secondary_raw_path = out_dir / SECONDARY_RAW_CLOSED_POSITIONS_LOG_FILE_NAME
    fail_path = out_dir / "closed_positions_failed.csv"
    score_path = out_dir / "edge_scores.xlsx"
    not_saved_reasons_path = out_dir / NOT_SAVED_REASONS_FILE_NAME
    not_saved_reason_stats_path = out_dir / NOT_SAVED_REASON_STATS_FILE_NAME
    progress_path = out_dir / "edge_scores_progress.csv"
    score_journal_path = out_dir / SCORE_JOURNAL_FILE_NAME
    memory_path = out_dir / TEST_MEMORY_FILE_NAME
    page_cache_path = out_dir / CLOSED_POSITION_PAGE_CACHE_FILE_NAME
    secondary_page_cache_path = out_dir / SECONDARY_CLOSED_POSITION_PAGE_CACHE_FILE_NAME
    universe_path = out_dir / "wallet_universe.csv"
    complete_fetch_db_path = out_dir / COMPLETE_FETCH_CACHE_DB_FILE_NAME

    ranked_wallets = (
        list(wallets.values())
        if preserve_wallet_order
        else sorted(wallets.values(), key=lambda item: item.best_pnl, reverse=True)
    )
    if max_wallets:
        ranked_wallets = ranked_wallets[:max_wallets]

    shard_count = max(int(shard_count), 1)
    shard_index = int(shard_index)
    if not 0 <= shard_index < shard_count:
        raise ValueError(f"Invalid shard index {shard_index} for shard count {shard_count}")
    if shard_count > 1:
        global_count = len(ranked_wallets)
        ranked_wallets = [
            seed
            for global_index, seed in enumerate(ranked_wallets)
            if global_index % shard_count == shard_index
        ]
        print(
            f"[shard] index={shard_index}/{shard_count} "
            f"wallets={len(ranked_wallets)} global_wallets={global_count}",
            flush=True,
        )
    shard_wallet_set = {seed.proxy_wallet for seed in ranked_wallets}

    fallback_out_dir = fallback_out_dir.resolve() if fallback_out_dir else None
    if fallback_out_dir is not None and fallback_out_dir == out_dir.resolve():
        fallback_out_dir = None

    score_fieldnames = get_score_fieldnames()
    score_by_wallet: dict[str, dict[str, Any]] = {}
    score_sources: list[Path] = []
    if fallback_out_dir is not None:
        score_sources.append(fallback_out_dir / "edge_scores_progress.csv")
        score_sources.append(fallback_out_dir / SCORE_JOURNAL_FILE_NAME)
    score_sources.append(progress_path)
    score_sources.append(score_journal_path)
    for source in score_sources:
        for row in load_progress_scores(source):
            wallet = str(row.get("proxyWallet") or "").lower()
            if wallet and wallet in shard_wallet_set:
                score_by_wallet[wallet] = row

    not_saved_reasons_by_wallet: dict[str, dict[str, Any]] = {}
    not_saved_reason_stats: dict[str, dict[str, Any]] = {}
    filtered_wallets_to_purge = load_filtered_wallets_from_memory(memory_path)
    tested_wallets = load_test_memory(memory_path)
    if fallback_out_dir is not None:
        fallback_memory = fallback_out_dir / TEST_MEMORY_FILE_NAME
        filtered_wallets_to_purge |= load_filtered_wallets_from_memory(fallback_memory)
        tested_wallets |= load_test_memory(fallback_memory)
    filtered_wallets_to_purge &= shard_wallet_set
    tested_wallets &= shard_wallet_set
    if tested_wallets:
        print(f"[resume] loaded test memory for {len(tested_wallets)} shard wallets", flush=True)

    cached_positions = load_closed_positions_cache(raw_path)
    if fallback_out_dir is not None:
        fallback_cached_positions = load_closed_positions_cache(
            fallback_out_dir / RAW_CLOSED_POSITIONS_LOG_FILE_NAME
        )
        merge_missing_closed_positions_cache(cached_positions, fallback_cached_positions)
    cached_positions = {
        wallet: positions
        for wallet, positions in cached_positions.items()
        if wallet in shard_wallet_set
    }
    if cached_positions:
        print(f"[resume] loaded closed-position cache for {len(cached_positions)} wallets", flush=True)
    if USE_SECONDARY_OFFLINE_POSITION_BACKUPS:
        secondary_cached_positions = load_closed_positions_cache(secondary_raw_path)
        added_wallets = merge_missing_closed_positions_cache(cached_positions, secondary_cached_positions)
        if secondary_cached_positions:
            print(
                f"[resume] loaded secondary closed-position cache for {len(secondary_cached_positions)} "
                f"wallets; added {added_wallets} missing wallets",
                flush=True,
            )
    page_cache = load_closed_position_page_cache(page_cache_path)
    if fallback_out_dir is not None:
        fallback_page_cache = load_closed_position_page_cache(
            fallback_out_dir / CLOSED_POSITION_PAGE_CACHE_FILE_NAME
        )
        merge_missing_closed_position_page_cache(page_cache, fallback_page_cache)
    page_cache = {
        wallet: offsets
        for wallet, offsets in page_cache.items()
        if wallet in shard_wallet_set
    }
    if page_cache:
        print(f"[resume] loaded page cache for {len(page_cache)} shard wallets", flush=True)
    if USE_SECONDARY_OFFLINE_POSITION_BACKUPS:
        secondary_page_cache = load_closed_position_page_cache(secondary_page_cache_path)
        added_pages = merge_missing_closed_position_page_cache(page_cache, secondary_page_cache)
        if secondary_page_cache:
            print(
                f"[resume] loaded secondary page cache for {len(secondary_page_cache)} wallets; "
                f"added {added_pages} missing pages",
                flush=True,
            )

    fallback_complete_db = (
        fallback_out_dir / COMPLETE_FETCH_CACHE_DB_FILE_NAME
        if fallback_out_dir is not None
        else None
    )
    complete_fetch_cache = CompleteFetchCache(
        complete_fetch_db_path,
        fallback_path=fallback_complete_db,
    )
    print(
        f"[resume] complete fetch cache: {complete_fetch_db_path} "
        f"fallback={fallback_complete_db or 'none'}",
        flush=True,
    )

    fail_file, fail_writer = open_csv_append(fail_path, ["proxyWallet", "error"])
    memory_file, memory_writer = open_csv_append(
        memory_path,
        ["proxyWallet", "userName", "status", "reason", "testedAt"],
    )
    score_journal_file, score_journal_writer = open_csv_append(
        score_journal_path,
        score_fieldnames,
    )
    consecutive_fetch_failures = 0
    fetch_failed_wallets: set[str] = set()
    with raw_path.open("a", encoding="utf-8") as raw_file, fail_file, memory_file, score_journal_file, page_cache_path.open(
        "a", encoding="utf-8"
    ) as page_cache_file:

        for index, seed in enumerate(ranked_wallets, start=1):
            if seed.proxy_wallet in tested_wallets:
                print(f"[skip] {index}/{len(ranked_wallets)} {seed.user_name} {seed.proxy_wallet}", flush=True)
                if seed.proxy_wallet not in score_by_wallet:
                    write_not_saved_reason(
                        not_saved_reasons_by_wallet,
                        not_saved_reason_stats,
                        not_saved_reasons_path,
                        not_saved_reason_stats_path,
                        seed,
                        status="skipped",
                        reason="already in wallet_test_memory; delete memory file to retest and recover exact reason",
                    )
                continue

            print(f"[closed] {index}/{len(ranked_wallets)} {seed.user_name} {seed.proxy_wallet}", flush=True)
            if seed.proxy_wallet in cached_positions:
                positions = cached_positions[seed.proxy_wallet]
            else:
                try:
                    positions, fetch_metadata = fetch_closed_positions(
                        client,
                        seed.proxy_wallet,
                        max_positions=max_positions_per_wallet,
                        page_cache=page_cache,
                        page_cache_file=page_cache_file,
                        complete_cache=complete_fetch_cache,
                    )
                    raw_file.write(
                        json.dumps(
                            {
                                "proxyWallet": seed.proxy_wallet,
                                "positions": positions,
                                "complete": bool(fetch_metadata.get("complete")),
                                "fetchVersion": COMPLETE_FETCH_VERSION,
                                "fetchedAt": int(time.time()),
                                "fetchMetadata": fetch_metadata,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    raw_file.flush()
                    cached_positions[seed.proxy_wallet] = positions
                except Exception as exc:
                    fetch_failed_wallets.add(seed.proxy_wallet)
                    consecutive_fetch_failures += 1
                    fail_writer.writerow({"proxyWallet": seed.proxy_wallet, "error": repr(exc)})
                    fail_file.flush()
                    write_not_saved_reason(
                        not_saved_reasons_by_wallet,
                        not_saved_reason_stats,
                        not_saved_reasons_path,
                        not_saved_reason_stats_path,
                        seed,
                        status="fetch_failed",
                        reason=repr(exc),
                    )
                    write_live_score_outputs(score_by_wallet.values(), score_path, out_dir, score_fieldnames)
                    if (
                        client.proxy_url
                        and PROXY_FAILOVER_ENABLED
                        and consecutive_fetch_failures >= PROXY_WORKER_MAX_CONSECUTIVE_FETCH_FAILURES
                    ):
                        write_sorted_scores_csv(
                            score_by_wallet.values(), progress_path, score_fieldnames
                        )
                        complete_fetch_cache.close()
                        raise WorkerProxyFailure(
                            f"proxy worker had {consecutive_fetch_failures} consecutive fetch failures; "
                            f"last_wallet={seed.proxy_wallet}; last_error={exc!r}"
                        )
                    continue

            consecutive_fetch_failures = 0
            score = score_positions(positions, smoothing=smoothing)
            if score["resolvedPositions"] < min_positions:
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=f"resolvedPositions {score['resolvedPositions']} < {min_positions}",
                )
                tested_wallets.add(seed.proxy_wallet)
                purge_filtered_wallet_now(
                    seed.proxy_wallet,
                    filtered_wallets_to_purge,
                    cached_positions,
                    page_cache,
                    raw_path,
                    page_cache_path,
                    universe_path,
                )
                write_not_saved_reason(
                    not_saved_reasons_by_wallet,
                    not_saved_reason_stats,
                    not_saved_reasons_path,
                    not_saved_reason_stats_path,
                    seed,
                    status="filtered",
                    reason=f"resolvedPositions {score['resolvedPositions']} < {min_positions}",
                    score=score,
                )
                write_live_score_outputs(score_by_wallet.values(), score_path, out_dir, score_fieldnames)
                continue
            if score["losses"] < min_losses:
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=f"losses {score['losses']} < {min_losses}",
                )
                tested_wallets.add(seed.proxy_wallet)
                purge_filtered_wallet_now(
                    seed.proxy_wallet,
                    filtered_wallets_to_purge,
                    cached_positions,
                    page_cache,
                    raw_path,
                    page_cache_path,
                    universe_path,
                )
                write_not_saved_reason(
                    not_saved_reasons_by_wallet,
                    not_saved_reason_stats,
                    not_saved_reasons_path,
                    not_saved_reason_stats_path,
                    seed,
                    status="filtered",
                    reason=f"losses {score['losses']} < {min_losses}",
                    score=score,
                )
                write_live_score_outputs(score_by_wallet.values(), score_path, out_dir, score_fieldnames)
                continue
            if score["realizedPnlAfterCosts"] < min_pnl:
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=f"realizedPnlAfterCosts {score['realizedPnlAfterCosts']} < {min_pnl}",
                )
                tested_wallets.add(seed.proxy_wallet)
                purge_filtered_wallet_now(
                    seed.proxy_wallet,
                    filtered_wallets_to_purge,
                    cached_positions,
                    page_cache,
                    raw_path,
                    page_cache_path,
                    universe_path,
                )
                write_not_saved_reason(
                    not_saved_reasons_by_wallet,
                    not_saved_reason_stats,
                    not_saved_reasons_path,
                    not_saved_reason_stats_path,
                    seed,
                    status="filtered",
                    reason=f"realizedPnlAfterCosts {score['realizedPnlAfterCosts']} < {min_pnl}",
                    score=score,
                )
                write_live_score_outputs(score_by_wallet.values(), score_path, out_dir, score_fieldnames)
                continue
            filter_reason = mode_2_filter_reason(score)
            if filter_reason:
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=filter_reason,
                )
                tested_wallets.add(seed.proxy_wallet)
                purge_filtered_wallet_now(
                    seed.proxy_wallet,
                    filtered_wallets_to_purge,
                    cached_positions,
                    page_cache,
                    raw_path,
                    page_cache_path,
                    universe_path,
                )
                write_not_saved_reason(
                    not_saved_reasons_by_wallet,
                    not_saved_reason_stats,
                    not_saved_reasons_path,
                    not_saved_reason_stats_path,
                    seed,
                    status="filtered",
                    reason=filter_reason,
                    score=score,
                )
                write_live_score_outputs(score_by_wallet.values(), score_path, out_dir, score_fieldnames)
                continue

            roi_proxy = seed.best_pnl / seed.best_vol if seed.best_vol > 0 else 0.0
            score_row = {
                "proxyWallet": seed.proxy_wallet,
                "userName": seed.user_name,
                "xUsername": seed.x_username,
                "verifiedBadge": seed.verified_badge,
                "bestPnlLeaderboard": seed.best_pnl,
                "bestVolLeaderboard": seed.best_vol,
                "profileViews": seed.profile_views,
                "roiProxyLeaderboard": roi_proxy,
                "leaderboardHits": seed.leaderboard_hits,
                "bestRankSeen": seed.best_rank_seen,
                "modes": "|".join(sorted(seed.modes)),
                **score,
            }
            score_by_wallet[seed.proxy_wallet] = score_row
            score_journal_writer.writerow(
                {key: score_row.get(key, "") for key in score_fieldnames}
            )
            score_journal_file.flush()
            if UPDATE_PROGRESS_CSV_AFTER_EACH_WALLET:
                write_sorted_scores_csv(score_by_wallet.values(), progress_path, score_fieldnames)
            write_test_memory_row(
                memory_writer,
                memory_file,
                seed,
                status="scored",
                reason="ok",
            )
            tested_wallets.add(seed.proxy_wallet)
            write_live_score_outputs(score_by_wallet.values(), score_path, out_dir, score_fieldnames)

    if not UPDATE_PROGRESS_CSV_AFTER_EACH_WALLET:
        write_sorted_scores_csv(score_by_wallet.values(), progress_path, score_fieldnames)

    if client.proxy_url and PROXY_FAILOVER_ENABLED and fetch_failed_wallets:
        complete_fetch_cache.close()
        raise WorkerRetryRequired(
            f"{len(fetch_failed_wallets)} wallet(s) had fetch failures in this pass and need failover retry"
        )

    # در اجرای چند VLESS، XLSXهای سنگین فقط یک‌بار بعد از merge ساخته می‌شوند.
    if not skip_final_xlsx:
        write_not_saved_reason_outputs(
            not_saved_reasons_by_wallet,
            not_saved_reason_stats,
            not_saved_reasons_path,
            not_saved_reason_stats_path,
        )
        write_all_score_outputs(score_by_wallet.values(), score_path, out_dir, score_fieldnames)
    else:
        print(
            "[shard output] final XLSX generation skipped; manager will build merged XLSX files",
            flush=True,
        )
    if PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS:
        rewrite_closed_positions_cache(raw_path, cached_positions)
        rewrite_closed_position_page_cache(page_cache_path, page_cache)
    # در اجرای shard، فایل universe باید ثابت بماند؛ تغییر تعداد/ترتیب ردیف‌ها باعث
    # عوض‌شدن modulo و جابه‌جایی والت‌ها در اجرای Failover می‌شود.
    if (
        PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE
        and shard_count == 1
        and filtered_wallets_to_purge
    ):
        rewrite_wallet_universe_without_wallets(universe_path, filtered_wallets_to_purge)
    complete_fetch_cache.close()


def mode_2_filter_reason(score: dict[str, Any]) -> str:
    if FILTER_ALL_RECENT_BALANCES_NEGATIVE and score["allRecentBalancesNegative"]:
        return "all recent balances are negative"
    if FILTER_NEGATIVE_NET_EDGE and score["netEdge"] < 0:
        return f"netEdge {score['netEdge']} < 0"
    if (
        FILTER_NON_POSITIVE_ONE_SHARE_NET_PNL_AFTER_COSTS
        and score["oneShareNetPnlAfterCosts"] <= 0
    ):
        return f"oneShareNetPnlAfterCosts {score['oneShareNetPnlAfterCosts']} <= 0"
    if FILTER_MIN_RECOVERY_FACTOR and score["recoveryFactor"] < MIN_RECOVERY_FACTOR:
        return f"recoveryFactor {score['recoveryFactor']} < {MIN_RECOVERY_FACTOR}"
    if FILTER_NO_RECENT_7D_OPEN_OR_CLOSE and score["recentActivityCount"] <= 0:
        return f"no open/close activity in last {RECENT_ACTIVITY_DAYS} days"
    if (
        FILTER_SHORT_HOLD_RATIO
        and score["holdDurationCount"] > 0
        and score["shortHoldRatio"] > MAX_SHORT_HOLD_RATIO
    ):
        return (
            f"shortHoldRatio {score['shortHoldRatio']} > {MAX_SHORT_HOLD_RATIO} "
            f"for holds under {SHORT_HOLD_MAX_HOURS}h"
        )
    return ""


def load_closed_positions_cache(raw_path: Path) -> dict[str, list[dict[str, Any]]]:
    cached: dict[str, list[dict[str, Any]]] = {}
    ignored_legacy = 0
    if not raw_path.exists():
        return cached
    with raw_path.open("r", encoding="utf-8") as file:
        for line in file:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            wallet = str(row.get("proxyWallet") or "").lower()
            positions = row.get("positions")
            complete = bool(row.get("complete"))
            fetch_version = str(row.get("fetchVersion") or "")
            trusted = complete and fetch_version == COMPLETE_FETCH_VERSION
            if not trusted and TRUST_LEGACY_RAW_CLOSED_POSITION_CACHE:
                trusted = wallet and isinstance(positions, list)
            if wallet and isinstance(positions, list) and trusted:
                cached[wallet] = dedupe_closed_positions(positions)
            elif wallet and isinstance(positions, list):
                ignored_legacy += 1
    if ignored_legacy:
        print(
            f"[cache] ignored {ignored_legacy} legacy/incomplete raw wallet rows; "
            "they will be fetched again accurately",
            flush=True,
        )
    return cached


def load_closed_position_page_cache(
    page_cache_path: Path,
) -> dict[str, dict[int, list[dict[str, Any]]]]:
    cached: dict[str, dict[int, list[dict[str, Any]]]] = {}
    if not page_cache_path.exists():
        return cached
    with page_cache_path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            wallet = str(row.get("proxyWallet") or "").lower()
            offset = int(safe_float(row.get("offset"), -1))
            rows = row.get("rows")
            if (
                wallet
                and 0 <= offset <= CLOSED_POSITIONS_MAX_API_OFFSET
                and isinstance(rows, list)
            ):
                cached.setdefault(wallet, {})[offset] = rows
    return cached


def merge_missing_closed_positions_cache(
    primary: dict[str, list[dict[str, Any]]],
    secondary: dict[str, list[dict[str, Any]]],
) -> int:
    added = 0
    for wallet, positions in secondary.items():
        if wallet not in primary:
            primary[wallet] = positions
            added += 1
    return added


def merge_missing_closed_position_page_cache(
    primary: dict[str, dict[int, list[dict[str, Any]]]],
    secondary: dict[str, dict[int, list[dict[str, Any]]]],
) -> int:
    added = 0
    for wallet, offsets in secondary.items():
        primary_offsets = primary.setdefault(wallet, {})
        for offset, rows in offsets.items():
            if offset not in primary_offsets:
                primary_offsets[offset] = rows
                added += 1
    return added


def rewrite_closed_positions_cache(raw_path: Path, cached: dict[str, list[dict[str, Any]]]) -> None:
    with raw_path.open("w", encoding="utf-8") as file:
        for wallet, positions in cached.items():
            file.write(json.dumps({"proxyWallet": wallet, "positions": positions, "complete": True, "fetchVersion": COMPLETE_FETCH_VERSION, "fetchedAt": int(time.time())}, ensure_ascii=False) + "\n")


def rewrite_closed_position_page_cache(
    page_cache_path: Path,
    cached: dict[str, dict[int, list[dict[str, Any]]]],
) -> None:
    with page_cache_path.open("w", encoding="utf-8") as file:
        for wallet, offsets in cached.items():
            for offset, rows in sorted(offsets.items()):
                file.write(
                    json.dumps(
                        {"proxyWallet": wallet, "offset": offset, "rows": rows},
                        ensure_ascii=False,
                    )
                    + "\n"
                )


def rewrite_wallet_universe_without_wallets(universe_path: Path, wallets_to_remove: set[str]) -> None:
    if not universe_path.exists():
        return
    with universe_path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        fieldnames = list(reader.fieldnames or [])
        rows = [
            row
            for row in reader
            if str(row.get("proxyWallet") or "").lower() not in wallets_to_remove
        ]
    with universe_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def purge_filtered_wallet_now(
    wallet: str,
    filtered_wallets_to_purge: set[str],
    cached_positions: dict[str, list[dict[str, Any]]],
    page_cache: dict[str, dict[int, list[dict[str, Any]]]],
    raw_path: Path,
    page_cache_path: Path,
    universe_path: Path,
) -> None:
    # فقط علامت‌گذاری/حذف از حافظه؛ بازنویسی فایل‌های بزرگ در پایان rank_wallets انجام می‌شود.
    # نسخه قبلی بعد از هر والت کل JSONL/CSV را بازنویسی می‌کرد و روی هزاران والت O(n²) می‌شد.
    filtered_wallets_to_purge.add(wallet)
    if PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS:
        cached_positions.pop(wallet, None)
        page_cache.pop(wallet, None)


def load_test_memory(memory_path: Path) -> set[str]:
    tested: set[str] = set()
    if not memory_path.exists():
        return tested
    with memory_path.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            wallet = str(row.get("proxyWallet") or "").lower()
            status = str(row.get("status") or "").lower()
            if wallet and status in {"scored", "filtered"}:
                tested.add(wallet)
    return tested


def load_filtered_wallets_from_memory(memory_path: Path) -> set[str]:
    filtered: set[str] = set()
    if not memory_path.exists():
        return filtered
    with memory_path.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            wallet = str(row.get("proxyWallet") or "").lower()
            status = str(row.get("status") or "").lower()
            if wallet and status == "filtered":
                filtered.add(wallet)
    return filtered


def write_test_memory_row(
    writer: csv.DictWriter,
    file,
    seed: WalletSeed,
    status: str,
    reason: str,
) -> None:
    writer.writerow(
        {
            "proxyWallet": seed.proxy_wallet,
            "userName": seed.user_name,
            "status": status,
            "reason": reason,
            "testedAt": int(time.time()),
        }
    )
    file.flush()


def get_not_saved_reason_fieldnames() -> list[str]:
    return [
        "proxyWallet",
        "userName",
        "xUsername",
        "status",
        "reason",
        "positions",
        "resolvedPositions",
        "wins",
        "losses",
        "realizedPnlClosed",
        "realizedPnlClosedRaw",
        "realizedPnlAfterCosts",
        "oneShareNetPnlAfterCosts",
        "oneShareTotalCostAfterCosts",
        "oneShareAverageDailyCostAfterCosts",
        "profitPerTradeAfterCosts",
        "profitPerTradeTimesWinRateAfterCosts",
        "profitPerTradeTimesNetEdgeAfterCosts",
        "profitPerTradeTimesOneShareNetPnlAfterCosts",
        "rallyTimesNetEdgeTimesProfitPerTradeAfterCosts",
        "netEdge",
        "recoveryFactor",
        "recentActivityCount",
        "tradingDays",
        "averageTradesPerDay",
        "maxTradesInOneDay",
        "daysSinceLastTrade",
        "tradesPerCalendarDayFirstToLast",
        "shortHoldRatio",
        "allRecentBalancesNegative",
        "testedAt",
    ]


def get_not_saved_reason_stats_fieldnames() -> list[str]:
    return [
        "rank",
        "reasonGroup",
        "count",
        "latestReason",
        "latestWallet",
        "latestUserName",
        "latestAt",
    ]


def normalize_not_saved_reason(reason: str) -> str:
    if re.match(r"recoveryFactor .+ < .+", reason):
        return "recoveryFactor < minimum"
    if re.match(r"netEdge .+ < 0", reason):
        return "netEdge < 0"
    if re.match(r"oneShareNetPnlAfterCosts .+ <= 0", reason):
        return "oneShareNetPnlAfterCosts <= 0"
    if re.match(r"resolvedPositions .+ < .+", reason):
        return "resolvedPositions < minimum"
    if re.match(r"losses .+ < .+", reason):
        return "losses < minimum"
    if re.match(r"realizedPnlAfterCosts .+ < .+", reason):
        return "realizedPnlAfterCosts < minimum"
    if reason.startswith("no open/close activity in last"):
        return "no recent open/close activity"
    if reason.startswith("shortHoldRatio "):
        return "shortHoldRatio > maximum"
    if reason == "all recent balances are negative":
        return "all recent balances are negative"
    if reason.startswith("already in wallet_test_memory"):
        return "skipped from wallet_test_memory"
    return reason


def write_not_saved_reason(
    rows_by_wallet: dict[str, dict[str, Any]],
    stats_by_reason: dict[str, dict[str, Any]],
    path: Path,
    stats_path: Path,
    seed: WalletSeed,
    status: str,
    reason: str,
    score: dict[str, Any] | None = None,
) -> None:
    score = score or {}
    rows_by_wallet[seed.proxy_wallet] = {
        "proxyWallet": seed.proxy_wallet,
        "userName": seed.user_name,
        "xUsername": seed.x_username,
        "status": status,
        "reason": reason,
        "positions": score.get("positions", ""),
        "resolvedPositions": score.get("resolvedPositions", ""),
        "wins": score.get("wins", ""),
        "losses": score.get("losses", ""),
        "realizedPnlClosed": score.get("realizedPnlClosed", ""),
        "realizedPnlClosedRaw": score.get("realizedPnlClosedRaw", ""),
        "realizedPnlAfterCosts": score.get("realizedPnlAfterCosts", ""),
        "oneShareNetPnlAfterCosts": score.get("oneShareNetPnlAfterCosts", ""),
        "oneShareTotalCostAfterCosts": score.get("oneShareTotalCostAfterCosts", ""),
        "oneShareAverageDailyCostAfterCosts": score.get("oneShareAverageDailyCostAfterCosts", ""),
        "profitPerTradeAfterCosts": score.get("profitPerTradeAfterCosts", ""),
        "profitPerTradeTimesWinRateAfterCosts": score.get("profitPerTradeTimesWinRateAfterCosts", ""),
        "profitPerTradeTimesNetEdgeAfterCosts": score.get("profitPerTradeTimesNetEdgeAfterCosts", ""),
        "profitPerTradeTimesOneShareNetPnlAfterCosts": score.get(
            "profitPerTradeTimesOneShareNetPnlAfterCosts", ""
        ),
        "rallyTimesNetEdgeTimesProfitPerTradeAfterCosts": score.get(
            "rallyTimesNetEdgeTimesProfitPerTradeAfterCosts", ""
        ),
        "netEdge": score.get("netEdge", ""),
        "recoveryFactor": score.get("recoveryFactor", ""),
        "recentActivityCount": score.get("recentActivityCount", ""),
        "tradingDays": score.get("tradingDays", ""),
        "averageTradesPerDay": score.get("averageTradesPerDay", ""),
        "maxTradesInOneDay": score.get("maxTradesInOneDay", ""),
        "daysSinceLastTrade": score.get("daysSinceLastTrade", ""),
        "tradesPerCalendarDayFirstToLast": score.get("tradesPerCalendarDayFirstToLast", ""),
        "shortHoldRatio": score.get("shortHoldRatio", ""),
        "allRecentBalancesNegative": score.get("allRecentBalancesNegative", ""),
        "testedAt": int(time.time()),
    }
    reason_group = normalize_not_saved_reason(reason)
    if reason_group not in stats_by_reason:
        stats_by_reason[reason_group] = {
            "reasonGroup": reason_group,
            "count": 0,
            "latestReason": "",
            "latestWallet": "",
            "latestUserName": "",
            "latestAt": "",
        }
    stats_by_reason[reason_group]["count"] = int(stats_by_reason[reason_group]["count"]) + 1
    stats_by_reason[reason_group]["latestReason"] = reason
    stats_by_reason[reason_group]["latestWallet"] = seed.proxy_wallet
    stats_by_reason[reason_group]["latestUserName"] = seed.user_name
    stats_by_reason[reason_group]["latestAt"] = int(time.time())
    # ساخت XLSX برای تک‌تک والت‌ها بسیار سنگین است. فقط checkpoint دوره‌ای می‌زنیم.
    if (
        NOT_SAVED_XLSX_CHECKPOINT_EVERY > 0
        and len(rows_by_wallet) % NOT_SAVED_XLSX_CHECKPOINT_EVERY == 0
    ):
        write_not_saved_reason_outputs(rows_by_wallet, stats_by_reason, path, stats_path)


def write_not_saved_reason_outputs(
    rows_by_wallet: dict[str, dict[str, Any]],
    stats_by_reason: dict[str, dict[str, Any]],
    path: Path,
    stats_path: Path,
) -> None:
    write_table_xlsx(
        rows_by_wallet.values(),
        path,
        get_not_saved_reason_fieldnames(),
        sheet_name="not_saved_reasons",
    )
    write_table_xlsx(
        sorted(stats_by_reason.values(), key=lambda row: int(row["count"]), reverse=True),
        stats_path,
        get_not_saved_reason_stats_fieldnames(),
        sheet_name="reason_stats",
    )


def sorted_score_rows(rows: Any) -> list[dict[str, Any]]:
    score_rows = [dict(row) for row in rows if row and row.get("proxyWallet")]
    score_rows.sort(
        key=lambda row: (
            safe_float(row.get("edgeRally"), safe_float(row.get("netEdgeScore"), safe_float(row.get("netEdge")))),
            safe_float(row.get("adjustedWinRate")),
            safe_float(row.get("expectedPayoff")),
        ),
        reverse=True,
    )
    return score_rows


def write_sorted_scores_csv(rows: Any, path: Path, fieldnames: list[str]) -> None:
    if path.exists() and not OVERWRITE_OUTPUT_FILES:
        return
    score_rows = sorted_score_rows(rows)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for rank, row in enumerate(score_rows, start=1):
            row = dict(row)
            row["rank"] = rank
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def sorted_rows_by_factor(rows: Any, factor: str, descending: bool = True) -> list[dict[str, Any]]:
    score_rows = [dict(row) for row in rows if row and row.get("proxyWallet")]
    score_rows.sort(key=lambda row: safe_float(row.get(factor)), reverse=descending)
    return score_rows


def write_scores_csv(rows: Any, path: Path, fieldnames: list[str], sort_factor: str, descending: bool) -> None:
    if path.exists() and not OVERWRITE_OUTPUT_FILES:
        return
    score_rows = sorted_rows_by_factor(rows, sort_factor, descending)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for rank, row in enumerate(score_rows, start=1):
            row = dict(row)
            row["rank"] = rank
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_factor_result_files(rows: Any, out_dir: Path, fieldnames: list[str]) -> None:
    factors = [
        ("netEdge", True),
        ("netEdgeScore", True),
        ("rallyTimesNetEdge", True),
        ("rallyTimesOneShareNetPnlAfterCosts", True),
        ("adjustedWinRate", True),
        ("winRate", True),
        ("realizedPnlAfterCosts", True),
        ("oneShareNetPnlAfterCosts", True),
        ("oneShareTotalCostAfterCosts", False),
        ("oneShareAverageDailyCostAfterCosts", False),
        ("profitPerTradeAfterCosts", True),
        ("profitPerTradeTimesWinRateAfterCosts", True),
        ("profitPerTradeTimesNetEdgeAfterCosts", True),
        ("profitPerTradeTimesOneShareNetPnlAfterCosts", True),
        ("rallyTimesNetEdgeTimesProfitPerTradeAfterCosts", True),
        ("roiAfterCosts", True),
        ("maxDrawdown", False),
        ("profitFactorAfterCosts", True),
        ("recoveryFactor", True),
        ("netEdgeToMaxDrawdown", True),
        ("sharpeRatio", True),
        ("expectedPayoffAfterCosts", True),
        ("averageTradesPerDay", True),
        ("maxTradesInOneDay", True),
        ("daysSinceLastTrade", False),
        ("tradesPerCalendarDayFirstToLast", True),
        ("shortHoldRatio", False),
        ("profileViews", True),
    ]
    for factor, descending in factors:
        write_scores_xlsx(
            rows,
            out_dir / f"edge_scores_by_{factor}.xlsx",
            fieldnames,
            sort_factor=factor,
            descending=descending,
            sheet_name=f"by_{factor}",
        )


def write_all_score_outputs(rows: Any, score_path: Path, out_dir: Path, fieldnames: list[str]) -> None:
    write_scores_xlsx(rows, score_path, fieldnames)
    write_factor_result_files(rows, out_dir, fieldnames)


def write_live_score_outputs(rows: Any, score_path: Path, out_dir: Path, fieldnames: list[str]) -> None:
    if UPDATE_ALL_RESULT_FILES_AFTER_EACH_WALLET:
        write_all_score_outputs(rows, score_path, out_dir, fieldnames)


def excel_column_name(index: int) -> str:
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def excel_cell_value(value: Any) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_scores_xlsx(
    rows: Any,
    path: Path,
    fieldnames: list[str],
    sort_factor: str | None = None,
    descending: bool = True,
    sheet_name: str = "edge_scores",
) -> None:
    score_rows = (
        sorted_rows_by_factor(rows, sort_factor, descending) if sort_factor else sorted_score_rows(rows)
    )
    write_table_xlsx(score_rows, path, fieldnames, sheet_name=sheet_name)


def write_table_xlsx(rows: Any, path: Path, fieldnames: list[str], sheet_name: str) -> None:
    if path.exists() and not OVERWRITE_OUTPUT_FILES:
        return
    all_rows = [fieldnames]
    for rank, row in enumerate(rows, start=1):
        row = dict(row)
        if "rank" in fieldnames:
            row["rank"] = rank
        all_rows.append([row.get(key, "") for key in fieldnames])
    column_widths = []
    for col_index, field in enumerate(fieldnames):
        max_len = max(len(str(row[col_index])) for row in all_rows)
        column_widths.append(min(max(max_len + 2, 12), 80))
    cols_xml = "<cols>" + "".join(
        f'<col min="{idx}" max="{idx}" width="{width}" customWidth="1"/>'
        for idx, width in enumerate(column_widths, start=1)
    ) + "</cols>"
    sheet_rows = []
    for row_index, row in enumerate(all_rows, start=1):
        cells = []
        for col_index, value in enumerate(row, start=1):
            ref = f"{excel_column_name(col_index)}{row_index}"
            if isinstance(value, bool):
                cells.append(f'<c r="{ref}" t="b"><v>{1 if value else 0}</v></c>')
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                cells.append(f'<c r="{ref}"><v>{value}</v></c>')
            else:
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{excel_cell_value(value)}</t></is></c>')
        sheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    worksheet = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"{cols_xml}<sheetData>{''.join(sheet_rows)}</sheetData></worksheet>"
    )
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as xlsx:
        xlsx.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
        xlsx.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        safe_sheet_name = excel_cell_value(sheet_name[:31])
        xlsx.writestr("xl/workbook.xml", f'<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="{safe_sheet_name}" sheetId="1" r:id="rId1"/></sheets></workbook>')
        xlsx.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
        xlsx.writestr("xl/worksheets/sheet1.xml", worksheet)


def get_score_fieldnames() -> list[str]:
    return [
        "rank",
        "proxyWallet",
        "userName",
        "xUsername",
        "verifiedBadge",
        "netEdge",
        "netEdgeScore",
        "adjustedWinRate",
        "winRate",
        "positions",
        "resolvedPositions",
        "wins",
        "losses",
        "breakeven",
        "averageTradesPerDay",
        "maxTradesInOneDay",
        "daysSinceLastTrade",
        "tradesPerCalendarDayFirstToLast",
        "sumWinEdge",
        "sumLossRisk",
        "sumWinEdgeSq",
        "sumLossRiskSq",
        "edgeRallyDenominator",
        "realizedPnlClosed",
        "realizedPnlClosedRaw",
        "realizedPnlAfterCosts",
        "totalBoughtClosed",
        "totalBoughtClosedRaw",
        "totalBoughtAfterCosts",
        "oneShareNetPnlAfterCosts",
        "oneShareTotalCostAfterCosts",
        "oneShareAverageDailyCostAfterCosts",
        "profitPerTradeAfterCosts",
        "profitPerTradeTimesWinRateAfterCosts",
        "profitPerTradeTimesNetEdgeAfterCosts",
        "profitPerTradeTimesOneShareNetPnlAfterCosts",
        "rallyTimesNetEdgeTimesProfitPerTradeAfterCosts",
        "roiClosed",
        "roiRaw",
        "roiAfterCosts",
        "maxDrawdown",
        "maxDrawdownAfterCosts",
        "profitFactor",
        "profitFactorRaw",
        "profitFactorAfterCosts",
        "recoveryFactor",
        "netEdgeToMaxDrawdown",
        "sharpeRatio",
        "expectedPayoff",
        "expectedPayoffAfterCosts",
        "maxConsecutiveWins",
        "maxConsecutiveLosses",
        "grossProfit",
        "grossLoss",
        "grossProfitRaw",
        "grossLossRaw",
        "grossProfitAfterCosts",
        "grossLossAfterCosts",
        "walletEntryPrice",
        "copyEntryPrice",
        "assumedSpread",
        "assumedSpreadCost",
        "feeRate",
        "entryFee",
        "exitFee",
        "recentActivityCount",
        "tradingDays",
        "shortHoldCount",
        "holdDurationCount",
        "shortHoldRatio",
        "allRecentBalancesNegative",
        "edgeRally",
        "edgeRallyRaw",
        "rallyTimesNetEdge",
        "rallyTimesOneShareNetPnlAfterCosts",
        "bestPnlLeaderboard",
        "bestVolLeaderboard",
        "profileViews",
        "roiProxyLeaderboard",
        "leaderboardHits",
        "bestRankSeen",
        "modes",
    ]


def load_progress_scores(progress_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not progress_path.exists():
        return rows
    with progress_path.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row.get("proxyWallet"):
                rows.append(dict(row))
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket Edge Rally wallet ranker")
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["1", "2"],
        help="1 = extract leaderboard wallets, 2 = score wallets",
    )
    parser.add_argument("--out-dir", default=None, help="Output folder")
    parser.add_argument("--delay", type=float, default=None, help="Delay between API calls")
    parser.add_argument("--timeout", type=float, default=None, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=None, help="Retry count")
    parser.add_argument("--max-offset", type=int, default=None, help="Leaderboard max offset")
    parser.add_argument("--leaderboard-only", action="store_true", help="Only collect universe")
    parser.add_argument("--score-only", action="store_true", help="Use existing wallet_universe.csv")
    parser.add_argument("--max-wallets", type=int, default=None, help="Debug limit for scoring")
    parser.add_argument("--min-positions", type=int, default=None, help="Minimum resolved positions")
    parser.add_argument("--min-losses", type=int, default=None, help="Minimum losing positions")
    parser.add_argument("--min-pnl", type=float, default=None, help="Minimum closed realized PnL")
    parser.add_argument("--smoothing", type=float, default=None, help="Denominator smoothing")
    parser.add_argument(
        "--max-positions-per-wallet",
        type=int,
        default=None,
        help="Maximum closed positions to fetch per wallet",
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--proxy", default=None, help="HTTP proxy for this worker")
    parser.add_argument("--shard-count", type=int, default=1, help="Total wallet shards")
    parser.add_argument("--shard-index", type=int, default=0, help="This wallet shard index")
    parser.add_argument(
        "--wallet-universe-file",
        default=None,
        help="Input wallet_universe.csv path; independent from output folder",
    )
    parser.add_argument(
        "--fallback-out-dir",
        default=None,
        help="Read-only old output folder used for resume/cache fallback",
    )
    parser.add_argument("--xray", default=None, help="Path to xray.exe")
    parser.add_argument(
        "--vpn-list-file",
        default=None,
        help="VPN link text file; relative paths are resolved next to this Python file",
    )
    parser.add_argument("--vless-root", default=None, help="Root folder for multi-proxy shard outputs")
    parser.add_argument("--no-vless", action="store_true", help="Run one direct worker without proxy nodes")
    parser.add_argument("--merge-only", action="store_true", help="Only merge existing shard outputs")
    parser.add_argument("--skip-ip-check", action="store_true", help="Do not verify proxy-node outbound IPs")
    parser.add_argument("--skip-final-xlsx", action="store_true", help=argparse.SUPPRESS)
    return parser


def choose_mode(args: argparse.Namespace) -> int:
    if RUN_MODE in (1, 2):
        return RUN_MODE

    raise SystemExit("Invalid RUN_MODE. Open the code and set RUN_MODE = 1 or RUN_MODE = 2.")


def setting(value: Any, default: Any) -> Any:
    return default if value is None else value


def print_active_settings(
    mode: int,
    out_dir: Path,
    delay: float,
    timeout: float,
    retries: int,
    max_offset: int,
    max_wallets: int | None,
    min_positions: int,
    min_losses: int,
    min_pnl: float,
    max_positions_per_wallet: int,
) -> None:
    print("")
    print(f"[mode] {mode}")
    print(f"[out] {out_dir}")
    print(f"[network] delay={delay}s timeout={timeout}s retries={retries}")
    if mode == 1:
        print(
            f"[extract settings] max_offset={max_offset} "
            f"limit={LEADERBOARD_LIMIT} modes={len(CATEGORIES) * len(TIME_PERIODS) * len(ORDER_BY)}"
        )
    else:
        max_wallets_text = "ALL" if max_wallets is None else str(max_wallets)
        print(
            f"[score settings] max_wallets={max_wallets_text} "
            f"max_positions_per_wallet={max_positions_per_wallet} "
            f"min_positions={min_positions} "
            f"min_losses={min_losses} "
            f"min_pnl={min_pnl}"
        )
        print(
            "[mode 2 filters] "
            f"all_recent_balances_negative={FILTER_ALL_RECENT_BALANCES_NEGATIVE} "
            f"negative_net_edge={FILTER_NEGATIVE_NET_EDGE} "
            f"non_positive_one_share_net_pnl_after_costs="
            f"{FILTER_NON_POSITIVE_ONE_SHARE_NET_PNL_AFTER_COSTS} "
            f"min_recovery_factor={FILTER_MIN_RECOVERY_FACTOR}:{MIN_RECOVERY_FACTOR} "
            f"recent_activity_days={FILTER_NO_RECENT_7D_OPEN_OR_CLOSE}:{RECENT_ACTIVITY_DAYS} "
            f"short_hold={FILTER_SHORT_HOLD_RATIO}:{MAX_SHORT_HOLD_RATIO}/{SHORT_HOLD_MAX_HOURS}h "
            f"purge_position_jsonl={PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS} "
            f"purge_wallet_universe={PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE} "
            f"live_all_result_files={UPDATE_ALL_RESULT_FILES_AFTER_EACH_WALLET} "
            f"live_progress_csv={UPDATE_PROGRESS_CSV_AFTER_EACH_WALLET} "
            f"overwrite_outputs={OVERWRITE_OUTPUT_FILES}"
        )
        print(
            "[mode 2 input] "
            f"use_one_share_ranking_input={USE_ONE_SHARE_RANKING_INPUT} "
            f"file={ONE_SHARE_RANKING_INPUT_FILE_NAME}"
        )
        print(
            "[offline fallback] "
            f"use_secondary_backups={USE_SECONDARY_OFFLINE_POSITION_BACKUPS} "
            f"raw={SECONDARY_RAW_CLOSED_POSITIONS_LOG_FILE_NAME} "
            f"pages={SECONDARY_CLOSED_POSITION_PAGE_CACHE_FILE_NAME}"
        )
        print(f"[memory] {TEST_MEMORY_FILE_NAME} controls resume; delete it to restart scoring")
        print(f"[not saved reasons] {NOT_SAVED_REASONS_FILE_NAME} shows why wallets did not enter score files")
        print(f"[not saved reason stats] {NOT_SAVED_REASON_STATS_FILE_NAME} counts repeated removal reasons")
        print(
            f"[raw data] keep {RAW_CLOSED_POSITIONS_LOG_FILE_NAME} and {CLOSED_POSITION_PAGE_CACHE_FILE_NAME}"
        )
        print(
            "[complete fetch] "
            f"enabled={COMPLETE_CLOSED_POSITION_FETCH} "
            f"direct_market_threshold={DIRECT_FAST_PATH_MAX_TRADED_MARKETS} "
            f"market_batch={CLOSED_MARKET_BATCH_SIZE} "
            f"closed_workers={CLOSED_FETCH_WORKERS} "
            f"activity_workers={ACTIVITY_FETCH_WORKERS} "
            f"cache_db={COMPLETE_FETCH_CACHE_DB_FILE_NAME}"
        )
        print("[live output] edge_scores_progress.csv updates during scoring")
        print("[final output] edge_scores.xlsx and edge_scores_by_<factor>.xlsx are sorted after scoring finishes")
    print("")


def _clean_text(value: Any) -> str:
    return urllib.parse.unquote(str(value or "")).strip()


def _query_first(query: dict[str, list[str]], *names: str, default: str = "") -> str:
    for name in names:
        values = query.get(name)
        if values:
            return _clean_text(values[0])
    return default


def _decode_base64_text(value: str) -> str:
    compact = "".join(str(value or "").strip().split())
    if not compact:
        raise ValueError("empty Base64 value")
    padded = compact + "=" * (-len(compact) % 4)
    errors: list[Exception] = []
    for decoder in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            return decoder(padded.encode("ascii")).decode("utf-8")
        except Exception as exc:
            errors.append(exc)
    raise ValueError(f"invalid Base64 value: {errors[-1] if errors else 'decode failed'}")


def read_proxy_links_from_text(raw: str) -> list[str]:
    """Read vless/vmess/trojan/ss links or a Base64 subscription blob."""
    supported_prefixes = ("vless://", "vmess://", "trojan://", "ss://")
    raw = str(raw or "").strip()
    if not raw:
        return []

    lines = [line.strip() for line in raw.splitlines()]
    links = [
        line
        for line in lines
        if line and not line.startswith("#") and line.lower().startswith(supported_prefixes)
    ]
    if links:
        return list(dict.fromkeys(links))

    # A common subscription is one Base64 blob whose decoded body is newline links.
    compact = "".join(line for line in lines if line and not line.startswith("#"))
    if compact:
        try:
            decoded = _decode_base64_text(compact)
            links = [
                line.strip()
                for line in decoded.splitlines()
                if line.strip().lower().startswith(supported_prefixes)
            ]
        except Exception:
            links = []
    return list(dict.fromkeys(links))


def read_vless_links_from_text(raw: str) -> list[str]:
    """Backward-compatible name; now accepts all four supported protocols."""
    return read_proxy_links_from_text(raw)


def resolve_vpn_links_file(file_name: str | None = None) -> Path:
    """Return the VPN-list path, relative to this Python file unless absolute."""
    requested = str(file_name or VPN_LINKS_FILE_NAME).strip()
    path = Path(requested).expanduser()
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    return path.resolve()


def vpn_links_file_template() -> str:
    return (
        "# هر لینک VPN را در یک خط قرار بده.\n"
        "# پروتکل‌های قابل استفاده: vless://  vmess://  trojan://  ss://\n"
        "# خطوط خالی و خطوطی که با # شروع شوند نادیده گرفته می‌شوند.\n"
        "# می‌توانی یک subscription معمولی Base64 را هم کامل در همین فایل پیست کنی.\n"
        "#\n"
        "# vless://...\n"
        "# vmess://...\n"
        "# trojan://...\n"
        "# ss://...\n"
    )


def load_proxy_links_from_file(file_name: str | None = None) -> tuple[list[str], Path]:
    """Load and deduplicate supported proxy links from the external text file."""
    path = resolve_vpn_links_file(file_name)
    if not path.exists():
        if AUTO_CREATE_VPN_LINKS_FILE:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(vpn_links_file_template(), encoding="utf-8")
        return [], path

    try:
        raw = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        raw = path.read_text(encoding="utf-8", errors="replace")

    return read_proxy_links_from_text(raw), path


def _query_from_values(values: dict[str, Any]) -> dict[str, list[str]]:
    query: dict[str, list[str]] = {}
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, list):
            query[str(key)] = [str(item) for item in value]
        else:
            query[str(key)] = [str(value)]
    return query


def _int_value(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _bool_value(value: Any, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _base_xray_config(local_port: int, outbound: dict[str, Any]) -> dict[str, Any]:
    outbound = dict(outbound)
    outbound.setdefault("tag", "proxy-out")
    outbound.setdefault("mux", {"enabled": False})
    return {
        "log": {"loglevel": "warning"},
        "inbounds": [
            {
                "tag": "local-http",
                "listen": "127.0.0.1",
                "port": int(local_port),
                "protocol": "http",
                "settings": {"timeout": 0},
            }
        ],
        "outbounds": [
            outbound,
            {"tag": "direct", "protocol": "freedom"},
            {"tag": "block", "protocol": "blackhole"},
        ],
    }


def _build_stream_settings(
    query: dict[str, list[str]],
    host: str,
    *,
    default_network: str = "tcp",
    default_security: str = "none",
) -> dict[str, Any]:
    network = _query_first(query, "type", "net", "network", default=default_network).lower()
    if network == "h2":
        network = "http"
    elif network == "splithttp":
        network = "xhttp"
    elif network == "http-upgrade":
        network = "httpupgrade"

    security = _query_first(
        query, "security", "tls", default=default_security
    ).lower()
    if security in {"1", "true"}:
        security = "tls"
    elif security in {"0", "false", ""}:
        security = "none"

    stream: dict[str, Any] = {"network": network, "security": security}

    sni = _query_first(query, "sni", "serverName", "servername", default=host)
    fp = _query_first(query, "fp", "fingerprint", default="chrome")
    alpn_raw = _query_first(query, "alpn")
    alpn = [item.strip() for item in alpn_raw.split(",") if item.strip()]
    insecure = _bool_value(_query_first(query, "allowInsecure", "insecure"), False)

    if security == "tls":
        tls_settings: dict[str, Any] = {
            "serverName": sni,
            "allowInsecure": insecure,
        }
        if fp:
            tls_settings["fingerprint"] = fp
        if alpn:
            tls_settings["alpn"] = alpn
        stream["tlsSettings"] = tls_settings
    elif security == "reality":
        reality_settings: dict[str, Any] = {
            "serverName": sni,
            "fingerprint": fp or "chrome",
            "publicKey": _query_first(query, "pbk", "publicKey", "publickey"),
            "shortId": _query_first(query, "sid", "shortId", "shortid"),
            "spiderX": _query_first(query, "spx", "spiderX", "spiderx", default="/"),
        }
        if not reality_settings["publicKey"]:
            raise ValueError("Reality link has no pbk/publicKey")
        stream["realitySettings"] = reality_settings
    elif security not in {"none", ""}:
        raise ValueError(f"unsupported transport security={security!r}")

    path_value = _query_first(query, "path", default="/") or "/"
    host_header = _query_first(query, "host")
    header_type = _query_first(query, "headerType", "headertype", "header", default="none")

    if network == "ws":
        settings: dict[str, Any] = {"path": path_value}
        if host_header:
            settings["headers"] = {"Host": host_header}
        early_data = _query_first(query, "ed", "maxEarlyData", "maxearlydata")
        if early_data:
            settings["maxEarlyData"] = _int_value(early_data, 0)
        early_header = _query_first(query, "eh", "earlyDataHeaderName", "earlydataheadername")
        if early_header:
            settings["earlyDataHeaderName"] = early_header
        stream["wsSettings"] = settings
    elif network == "grpc":
        service_name = _query_first(
            query, "serviceName", "service", "servicename", "path"
        ).lstrip("/")
        grpc_settings: dict[str, Any] = {"serviceName": service_name}
        authority = _query_first(query, "authority", default=host_header)
        if authority:
            grpc_settings["authority"] = authority
        if _query_first(query, "mode").lower() == "multi":
            grpc_settings["multiMode"] = True
        stream["grpcSettings"] = grpc_settings
    elif network == "httpupgrade":
        settings = {"path": path_value}
        if host_header:
            settings["host"] = host_header
        stream["httpupgradeSettings"] = settings
    elif network == "http":
        settings = {"path": path_value}
        if host_header:
            settings["host"] = [item.strip() for item in host_header.split(",") if item.strip()]
        stream["httpSettings"] = settings
    elif network == "xhttp":
        settings = {"path": path_value}
        if host_header:
            settings["host"] = host_header
        mode = _query_first(query, "mode")
        if mode:
            settings["mode"] = mode
        extra = _query_first(query, "extra")
        if extra:
            try:
                settings["extra"] = json.loads(extra)
            except json.JSONDecodeError:
                pass
        stream["xhttpSettings"] = settings
    elif network in {"tcp", "raw"}:
        key = "rawSettings" if network == "raw" else "tcpSettings"
        settings: dict[str, Any] = {"header": {"type": header_type or "none"}}
        if header_type == "http":
            request: dict[str, Any] = {"path": [path_value]}
            if host_header:
                request["headers"] = {"Host": [host_header]}
            settings["header"]["request"] = request
        stream[key] = settings
    elif network in {"kcp", "mkcp"}:
        stream["network"] = "kcp"
        kcp_settings: dict[str, Any] = {
            "header": {"type": header_type or "none"}
        }
        seed = _query_first(query, "seed", "path")
        if seed and seed != "/":
            kcp_settings["seed"] = seed
        stream["kcpSettings"] = kcp_settings
    elif network == "quic":
        quic_security = _query_first(query, "quicSecurity", "quicsecurity", default="none")
        quic_key = _query_first(query, "key")
        stream["quicSettings"] = {
            "security": quic_security,
            "key": quic_key,
            "header": {"type": header_type or "none"},
        }
    else:
        raise ValueError(f"unsupported transport type={network!r}")

    return stream


def parse_vless_link(link: str, local_port: int) -> tuple[dict[str, Any], str]:
    parsed = urllib.parse.urlsplit(link.strip())
    if parsed.scheme.lower() != "vless":
        raise ValueError("link does not start with vless://")
    user_id = _clean_text(parsed.username)
    host = parsed.hostname or ""
    port = parsed.port
    if not user_id or not host or not port:
        raise ValueError("VLESS link must contain UUID, host and port")

    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    node_name = _clean_text(parsed.fragment) or f"{host}:{port}"
    user: dict[str, Any] = {
        "id": user_id,
        "encryption": _query_first(query, "encryption", default="none") or "none",
    }
    flow = _query_first(query, "flow")
    if flow:
        user["flow"] = flow
    packet_encoding = _query_first(query, "packetEncoding", "packetencoding")
    if packet_encoding:
        user["packetEncoding"] = packet_encoding

    outbound = {
        "tag": "proxy-out",
        "protocol": "vless",
        "settings": {
            "vnext": [
                {
                    "address": host,
                    "port": int(port),
                    "users": [user],
                }
            ]
        },
        "streamSettings": _build_stream_settings(query, host),
        "mux": {"enabled": False},
    }
    return _base_xray_config(local_port, outbound), node_name


def parse_vmess_link(link: str, local_port: int) -> tuple[dict[str, Any], str]:
    raw = link.strip()
    if not raw.lower().startswith("vmess://"):
        raise ValueError("link does not start with vmess://")

    payload = raw[len("vmess://"):].split("#", 1)[0].strip()
    data: dict[str, Any] | None = None
    try:
        decoded = _decode_base64_text(payload)
        candidate = json.loads(decoded)
        if isinstance(candidate, dict):
            data = candidate
    except Exception:
        data = None

    if data is None:
        # Less common URL-style VMess: vmess://uuid@host:port?...#name
        parsed = urllib.parse.urlsplit(raw)
        user_id = _clean_text(parsed.username)
        host = parsed.hostname or ""
        port = parsed.port
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if not user_id or not host or not port:
            raise ValueError("unsupported VMess link; expected Base64 JSON or URL-style UUID@host:port")
        node_name = _clean_text(parsed.fragment) or f"{host}:{port}"
        user: dict[str, Any] = {
            "id": user_id,
            "alterId": _int_value(_query_first(query, "aid", "alterId"), 0),
            "security": _query_first(query, "scy", "cipher", default="auto") or "auto",
        }
        outbound = {
            "tag": "proxy-out",
            "protocol": "vmess",
            "settings": {"vnext": [{"address": host, "port": int(port), "users": [user]}]},
            "streamSettings": _build_stream_settings(query, host),
            "mux": {"enabled": False},
        }
        return _base_xray_config(local_port, outbound), node_name

    host = str(data.get("add") or data.get("address") or "").strip()
    port = _int_value(data.get("port"), 0)
    user_id = str(data.get("id") or "").strip()
    if not host or not port or not user_id:
        raise ValueError("VMess JSON must contain add/address, port and id")

    network = str(data.get("net") or "tcp").lower()
    transport_security = str(data.get("tls") or "none").lower()
    stream_values = {
        "type": network,
        "security": transport_security,
        "host": data.get("host") or "",
        "path": data.get("path") or "/",
        "headerType": data.get("type") or "none",
        "serviceName": data.get("path") or "",
        "authority": data.get("host") or "",
        "sni": data.get("sni") or data.get("serverName") or host,
        "alpn": data.get("alpn") or "",
        "fp": data.get("fp") or data.get("fingerprint") or "chrome",
        "allowInsecure": data.get("allowInsecure") or data.get("insecure") or "",
        "seed": data.get("path") or "",
        "quicSecurity": data.get("host") or "none",
        "key": data.get("path") or "",
    }
    query = _query_from_values(stream_values)
    user = {
        "id": user_id,
        "alterId": _int_value(data.get("aid") or data.get("alterId"), 0),
        "security": str(data.get("scy") or data.get("cipher") or "auto"),
    }
    packet_encoding = str(data.get("packetEncoding") or "").strip()
    if packet_encoding:
        user["packetEncoding"] = packet_encoding

    outbound = {
        "tag": "proxy-out",
        "protocol": "vmess",
        "settings": {"vnext": [{"address": host, "port": port, "users": [user]}]},
        "streamSettings": _build_stream_settings(
            query,
            host,
            default_network=network,
            default_security=transport_security,
        ),
        "mux": {"enabled": _bool_value(data.get("mux"), False)},
    }
    node_name = str(data.get("ps") or data.get("name") or f"{host}:{port}").strip()
    return _base_xray_config(local_port, outbound), node_name


def parse_trojan_link(link: str, local_port: int) -> tuple[dict[str, Any], str]:
    parsed = urllib.parse.urlsplit(link.strip())
    if parsed.scheme.lower() != "trojan":
        raise ValueError("link does not start with trojan://")
    password = _clean_text(parsed.username)
    host = parsed.hostname or ""
    port = parsed.port
    if not password or not host or not port:
        raise ValueError("Trojan link must contain password, host and port")

    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if not _query_first(query, "security", "tls"):
        query["security"] = ["tls"]
    server: dict[str, Any] = {
        "address": host,
        "port": int(port),
        "password": password,
    }
    email = _query_first(query, "email")
    if email:
        server["email"] = email
    flow = _query_first(query, "flow")
    if flow:
        server["flow"] = flow

    outbound = {
        "tag": "proxy-out",
        "protocol": "trojan",
        "settings": {"servers": [server]},
        "streamSettings": _build_stream_settings(
            query, host, default_security="tls"
        ),
        "mux": {"enabled": _bool_value(_query_first(query, "mux"), False)},
    }
    node_name = _clean_text(parsed.fragment) or f"{host}:{port}"
    return _base_xray_config(local_port, outbound), node_name


def _decode_ss_userinfo(value: str) -> tuple[str, str]:
    value = urllib.parse.unquote(str(value or "")).strip()
    if not value:
        raise ValueError("empty Shadowsocks user info")
    decoded = value
    if ":" not in decoded:
        decoded = _decode_base64_text(value)
    if ":" not in decoded:
        raise ValueError("Shadowsocks credentials must be method:password")
    method, password = decoded.split(":", 1)
    if not method or not password:
        raise ValueError("Shadowsocks method/password is empty")
    return method, password


def parse_ss_link(link: str, local_port: int) -> tuple[dict[str, Any], str]:
    raw = link.strip()
    if not raw.lower().startswith("ss://"):
        raise ValueError("link does not start with ss://")

    parsed = urllib.parse.urlsplit(raw)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    plugin = _query_first(query, "plugin")
    if plugin:
        raise ValueError(
            f"Shadowsocks SIP003 plugin is not supported by Xray core in this launcher: {plugin}"
        )

    host = parsed.hostname or ""
    port = parsed.port
    method = ""
    password = ""

    if host and port:
        if parsed.password is not None:
            method = _clean_text(parsed.username)
            password = _clean_text(parsed.password)
        else:
            method, password = _decode_ss_userinfo(parsed.username or "")
    else:
        # Legacy form: ss://BASE64(method:password@host:port)#name
        body = raw[len("ss://"):]
        body = body.split("#", 1)[0].split("?", 1)[0]
        decoded = _decode_base64_text(body)
        legacy = urllib.parse.urlsplit("ss://" + decoded)
        host = legacy.hostname or ""
        port = legacy.port
        if legacy.password is not None:
            method = _clean_text(legacy.username)
            password = _clean_text(legacy.password)
        else:
            method, password = _decode_ss_userinfo(legacy.username or "")

    if not host or not port or not method or not password:
        raise ValueError("invalid Shadowsocks link")

    server: dict[str, Any] = {
        "address": host,
        "port": int(port),
        "method": method,
        "password": password,
    }
    outbound = {
        "tag": "proxy-out",
        "protocol": "shadowsocks",
        "settings": {"servers": [server]},
        "mux": {"enabled": False},
    }
    node_name = _clean_text(parsed.fragment) or f"{host}:{port}"
    return _base_xray_config(local_port, outbound), node_name


def parse_proxy_link(link: str, local_port: int) -> tuple[dict[str, Any], str, str]:
    scheme = urllib.parse.urlsplit(link.strip()).scheme.lower()
    parsers = {
        "vless": parse_vless_link,
        "vmess": parse_vmess_link,
        "trojan": parse_trojan_link,
        "ss": parse_ss_link,
    }
    parser = parsers.get(scheme)
    if parser is None:
        raise ValueError(f"unsupported proxy protocol={scheme!r}")
    config, node_name = parser(link, local_port)
    return config, node_name, scheme

def find_xray_executable(value: str | None) -> Path | None:
    candidates: list[Path] = []
    if value:
        candidates.append(Path(value))
    if XRAY_EXECUTABLE:
        candidates.append(Path(XRAY_EXECUTABLE))
    script_dir = Path(__file__).resolve().parent
    local_app_data = Path(os.environ.get("LOCALAPPDATA", "")) if os.environ.get("LOCALAPPDATA") else None
    program_files = Path(os.environ.get("PROGRAMFILES", "")) if os.environ.get("PROGRAMFILES") else None
    candidates.extend(
        [
            script_dir / "xray.exe",
            script_dir / "xray",
            script_dir / "bin" / "xray" / "xray.exe",
            script_dir / "v2rayN-With-Core" / "bin" / "xray" / "xray.exe",
        ]
    )
    if local_app_data is not None:
        candidates.extend(
            [
                local_app_data / "v2rayN" / "bin" / "xray" / "xray.exe",
                local_app_data / "Programs" / "v2rayN" / "bin" / "xray" / "xray.exe",
            ]
        )
    if program_files is not None:
        candidates.append(program_files / "v2rayN" / "bin" / "xray" / "xray.exe")
    which = shutil.which("xray.exe") or shutil.which("xray")
    if which:
        candidates.append(Path(which))
    for candidate in candidates:
        try:
            if candidate.exists() and candidate.is_file():
                return candidate.resolve()
        except OSError:
            continue
    return None


def port_is_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=0.4):
            return True
    except OSError:
        return False


def next_free_local_port(start: int) -> int:
    port = max(int(start), 1024)
    while port < 65535:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                port += 1
    raise RuntimeError("No free local TCP port found")


def wait_for_local_port(proc: subprocess.Popen, port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        if port_is_open(port):
            return True
        time.sleep(0.2)
    return False


def proxy_text_request(proxy_url: str, url: str, timeout: float = 12.0) -> str:
    if requests is not None:
        session = requests.Session()
        session.trust_env = False
        session.proxies.update({"http": proxy_url, "https": proxy_url})
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text.strip()
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with opener.open(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace").strip()


class XrayStreamHandle:
    def __init__(self, thread: threading.Thread, proc: subprocess.Popen) -> None:
        self.thread = thread
        self.proc = proc

    def close(self) -> None:
        try:
            if self.proc.stdout is not None:
                self.proc.stdout.close()
        except Exception:
            pass
        self.thread.join(timeout=2)


def start_xray_node(
    xray_path: Path,
    config_path: Path,
    port: int,
    log_path: Path,
    logger: RunLogRouter | None = None,
    source: str = "XRAY",
) -> tuple[subprocess.Popen, Any]:
    log_file = None if logger is not None else log_path.open("a", encoding="utf-8")
    commands = [
        [str(xray_path), "run", "-c", str(config_path)],
        [str(xray_path), "-config", str(config_path)],
    ]
    last_error = ""
    for command in commands:
        proc = subprocess.Popen(
            command,
            stdout=(subprocess.PIPE if logger is not None else log_file),
            stderr=subprocess.STDOUT,
            text=(logger is not None),
            encoding=("utf-8" if logger is not None else None),
            errors=("replace" if logger is not None else None),
            bufsize=(1 if logger is not None else -1),
            cwd=str(config_path.parent),
            creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0),
        )
        stream_handle = None
        if logger is not None:
            stream_thread = threading.Thread(
                target=stream_process_output,
                args=(proc, source, logger),
                daemon=True,
            )
            stream_thread.start()
            stream_handle = XrayStreamHandle(stream_thread, proc)
        if wait_for_local_port(proc, port, VLESS_START_TIMEOUT_SECONDS):
            return proc, (stream_handle if stream_handle is not None else log_file)
        last_error = f"exit={proc.poll()} command={command}"
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
        if stream_handle is not None:
            stream_handle.close()
        time.sleep(0.5)
    if log_file is not None:
        log_file.close()
    raise RuntimeError(f"Xray did not open local port {port}; {last_error}; log={log_path}")


def stop_process(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass


def stream_process_output(
    proc: subprocess.Popen,
    prefix: str,
    logger: RunLogRouter,
) -> None:
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\r\n")
        if line:
            logger.log(line, source=prefix)


def merge_csv_by_wallet(
    sources: list[Path],
    destination: Path,
    fieldnames: list[str],
    wallet_field: str = "proxyWallet",
) -> int:
    rows_by_wallet: dict[str, dict[str, Any]] = {}
    for source in sources:
        if not source.exists():
            continue
        with source.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                wallet = str(row.get(wallet_field) or "").strip().lower()
                if wallet:
                    rows_by_wallet[wallet] = dict(row)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_by_wallet.values():
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    return len(rows_by_wallet)


def merge_raw_jsonl(sources: list[Path], destination: Path) -> int:
    seen: set[str] = set()
    count = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as out:
        # New shard data first; old fallback is usually last in the sources list.
        for source in sources:
            if not source.exists():
                continue
            with source.open("r", encoding="utf-8", errors="ignore") as file:
                for line in file:
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    wallet = str(row.get("proxyWallet") or "").lower()
                    if not wallet or wallet in seen:
                        continue
                    seen.add(wallet)
                    out.write(json.dumps(row, ensure_ascii=False) + "\n")
                    count += 1
    return count


def merge_vless_outputs(root: Path, fallback_out_dir: Path | None = None) -> Path:
    root = root.resolve()
    part_dirs = sorted(path for path in root.glob("part_*") if path.is_dir())
    merged_dir = root / "merged"
    ensure_dir(merged_dir)

    source_dirs: list[Path] = []
    if fallback_out_dir is not None and fallback_out_dir.exists():
        source_dirs.append(fallback_out_dir.resolve())
    source_dirs.extend(part_dirs)

    score_rows_by_wallet: dict[str, dict[str, Any]] = {}
    for directory in source_dirs:
        for score_source in (
            directory / "edge_scores_progress.csv",
            directory / SCORE_JOURNAL_FILE_NAME,
        ):
            for row in load_progress_scores(score_source):
                wallet = str(row.get("proxyWallet") or "").lower()
                if wallet:
                    score_rows_by_wallet[wallet] = row

    fieldnames = get_score_fieldnames()
    write_sorted_scores_csv(
        score_rows_by_wallet.values(),
        merged_dir / "edge_scores_progress.csv",
        fieldnames,
    )
    write_all_score_outputs(
        score_rows_by_wallet.values(),
        merged_dir / "edge_scores.xlsx",
        merged_dir,
        fieldnames,
    )

    memory_sources = [directory / TEST_MEMORY_FILE_NAME for directory in source_dirs]
    merge_csv_by_wallet(
        memory_sources,
        merged_dir / TEST_MEMORY_FILE_NAME,
        ["proxyWallet", "userName", "status", "reason", "testedAt"],
    )

    completed_wallets: set[str] = set()
    for directory in source_dirs:
        completed_wallets |= load_test_memory(directory / TEST_MEMORY_FILE_NAME)

    failed_rows: dict[tuple[str, str], dict[str, str]] = {}
    for directory in source_dirs:
        path = directory / "closed_positions_failed.csv"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            for row in csv.DictReader(file):
                key = (
                    str(row.get("proxyWallet") or "").lower(),
                    str(row.get("error") or ""),
                )
                if key[0] and key[0] not in completed_wallets:
                    failed_rows[key] = row
    with (merged_dir / "closed_positions_failed.csv").open(
        "w", encoding="utf-8", newline=""
    ) as file:
        writer = csv.DictWriter(file, fieldnames=["proxyWallet", "error"])
        writer.writeheader()
        writer.writerows(failed_rows.values())

    universe_source = next(
        (directory / "wallet_universe.csv" for directory in source_dirs if (directory / "wallet_universe.csv").exists()),
        None,
    )
    if universe_source is not None:
        shutil.copy2(universe_source, merged_dir / "wallet_universe.csv")

    if VLESS_MERGE_RAW_JSONL:
        raw_sources = [
            directory / RAW_CLOSED_POSITIONS_LOG_FILE_NAME
            for directory in reversed(source_dirs)
        ]
        merge_raw_jsonl(raw_sources, merged_dir / RAW_CLOSED_POSITIONS_LOG_FILE_NAME)

    summary = {
        "mergedAt": int(time.time()),
        "parts": [str(path) for path in part_dirs],
        "fallback": str(fallback_out_dir) if fallback_out_dir else None,
        "scoredWallets": len(score_rows_by_wallet),
        "mergedDirectory": str(merged_dir),
        "rawJsonlMerged": bool(VLESS_MERGE_RAW_JSONL),
    }
    (merged_dir / "merge_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[merge] scored_wallets={len(score_rows_by_wallet)} output={merged_dir}", flush=True)
    return merged_dir


def run_vless_manager(args: argparse.Namespace) -> int:
    links, vpn_file = load_proxy_links_from_file(args.vpn_list_file)
    if not links:
        print(
            f"هیچ لینک vless/vmess/trojan/ss فعالی داخل فایل پیدا نشد: {vpn_file}\n"
            "لینک‌ها را هرکدام در یک خط داخل همین فایل قرار بده و برنامه را دوباره اجرا کن؛ "
            "یا USE_VLESS_MULTI=False بگذار.",
            file=sys.stderr,
        )
        return 2

    xray_path = find_xray_executable(args.xray)
    if xray_path is None:
        print(
            "xray.exe not found. Put xray.exe next to this Python file or use --xray PATH.",
            file=sys.stderr,
        )
        return 2

    fallback_out_dir = Path(args.fallback_out_dir or VLESS_FALLBACK_OUT_DIR).resolve()
    universe_file = Path(
        args.wallet_universe_file or (fallback_out_dir / "wallet_universe.csv")
    ).resolve()
    if not universe_file.exists():
        print(f"Missing wallet universe: {universe_file}", file=sys.stderr)
        return 2

    root = Path(args.vless_root or VLESS_OUTPUT_ROOT).resolve()
    runtime_dir = root / "_vless_runtime"
    ensure_dir(runtime_dir)
    ensure_dir(root)

    console_stdout = sys.stdout
    console_stderr = sys.stderr
    logger = RunLogRouter(root / ALL_LOG_FILE_NAME, root / ERROR_LOG_FILE_NAME)
    if CLEAN_CONSOLE_DASHBOARD:
        sys.stdout = RoutedLogStream(logger, "MANAGER")
        sys.stderr = RoutedLogStream(logger, "STDERR", force_error=True)
    logger.log("=" * 80, source="SYSTEM")
    logger.log("New multi-proxy run started", source="SYSTEM")
    console_stdout.write(
        f"Starting and testing {len(links)} proxy links... "
        f"Logs: {root / ALL_LOG_FILE_NAME} | Errors: {root / ERROR_LOG_FILE_NAME}\n"
    )
    console_stdout.flush()

    active_nodes: list[dict[str, Any]] = []
    xray_handles: list[tuple[subprocess.Popen, Any]] = []
    used_ips: set[str] = set()
    next_port = VLESS_LOCAL_HTTP_PORT_START
    worker_states: dict[int, dict[str, Any]] = {}
    output_threads: list[threading.Thread] = []
    pending_shards: deque[int] = deque()
    pending_set: set[int] = set()
    completed_shards: set[int] = set()
    permanently_failed: dict[int, str] = {}
    assignment_history: list[dict[str, Any]] = []
    shard_attempts: dict[int, int] = {}
    shard_last_node: dict[int, int | None] = {}
    shard_count = 0
    last_console_status = 0.0
    last_error_notice = time.monotonic()
    last_console_width = 0
    all_nodes_down_notice_logged = False

    # دقیقاً همان ترتیب/Limit مود 2 برای محاسبه درصد کل استفاده می‌شود.
    progress_wallet_rows = sorted(
        load_wallet_universe(universe_file).values(),
        key=lambda item: item.best_pnl,
        reverse=True,
    )
    progress_max_wallets = setting(args.max_wallets, MAX_WALLETS_TO_SCORE)
    if progress_max_wallets:
        progress_wallet_rows = progress_wallet_rows[: int(progress_max_wallets)]
    progress_wallet_set = {item.proxy_wallet for item in progress_wallet_rows}
    total_wallet_count = len(progress_wallet_set)
    fallback_completed_wallets = (
        load_test_memory(fallback_out_dir / TEST_MEMORY_FILE_NAME) & progress_wallet_set
    )

    script_path = Path(__file__).resolve()
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    def public_node(node: dict[str, Any]) -> dict[str, Any]:
        return {
            "source_index": node["source_index"],
            "name": node["name"],
            "protocol": node["protocol"],
            "proxy": node["proxy"],
            "port": node["port"],
            "ip": node["ip"],
            "hash": node["hash"],
            "healthy": bool(node.get("healthy")),
            "health_failures": int(node.get("health_failures", 0)),
            "busy_shard": node.get("busy_shard"),
            "last_health_error": node.get("last_health_error", ""),
            "next_dead_recheck_at": node.get("next_dead_recheck_at", 0),
            "recovery_count": int(node.get("recovery_count", 0)),
        }

    def completed_wallet_count() -> int:
        completed = set(fallback_completed_wallets)
        for part_dir in root.glob("part_*"):
            if part_dir.is_dir():
                completed |= load_test_memory(part_dir / TEST_MEMORY_FILE_NAME)
        return len(completed & progress_wallet_set)

    def console_line(text: str, *, newline: bool = False) -> None:
        nonlocal last_console_width
        if not CLEAN_CONSOLE_DASHBOARD:
            return
        padded = text.ljust(max(last_console_width, len(text)))
        console_stdout.write("\r" + padded)
        if newline:
            console_stdout.write("\n")
            last_console_width = 0
        else:
            last_console_width = max(last_console_width, len(text))
        console_stdout.flush()

    def show_dashboard(force: bool = False) -> None:
        nonlocal last_console_status
        now_mono = time.monotonic()
        if not force and now_mono - last_console_status < CONSOLE_STATUS_INTERVAL_SECONDS:
            return
        done = completed_wallet_count()
        percent = (done / total_wallet_count * 100.0) if total_wallet_count else 100.0
        healthy = sum(1 for node in active_nodes if node.get("healthy"))
        total_nodes = len(active_nodes)
        running = len(worker_states)
        pending = len(pending_shards)
        text = (
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"VPN Active: {healthy}/{total_nodes} | "
            f"Wallets: {done}/{total_wallet_count} | "
            f"Done: {percent:.2f}% | Running: {running} | Pending: {pending}"
        )
        console_line(text)
        last_console_status = now_mono

    def show_error_notice_if_due(force: bool = False) -> None:
        nonlocal last_error_notice
        now_mono = time.monotonic()
        if not force and now_mono - last_error_notice < CONSOLE_ERROR_NOTICE_INTERVAL_SECONDS:
            return
        new_errors = logger.consume_new_errors()
        last_error_notice = now_mono
        if new_errors:
            console_line("", newline=True)
            console_stdout.write(
                f"[{datetime.now().strftime('%H:%M:%S')}] ERROR NOTICE: "
                f"{new_errors} new error log entr{'y' if new_errors == 1 else 'ies'} "
                f"in the last minute -> read {root / ERROR_LOG_FILE_NAME}\n"
            )
            console_stdout.flush()
            show_dashboard(force=True)

    def close_node_xray(node: dict[str, Any]) -> None:
        proc = node.get("xray_proc")
        stop_process(proc)
        handle = node.get("xray_log_handle")
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        node["xray_proc"] = None
        node["xray_log_handle"] = None

    def mark_node_dead(node_index: int, error: str) -> None:
        node = active_nodes[node_index]
        if not node.get("healthy") and node.get("xray_proc") is None:
            node["last_health_error"] = error
            return
        node["healthy"] = False
        node["last_health_error"] = error
        node["next_dead_recheck_at"] = (
            time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
        )
        busy_shard = node.get("busy_shard")
        print(
            f"[proxy:dead] node={node_index} ip={node.get('ip')} "
            f"busy_shard={busy_shard} error={error}; duty will move to healthy nodes",
            flush=True,
        )
        if busy_shard is not None and busy_shard in worker_states:
            worker_states[busy_shard]["forced_stop"] = True
            stop_process(worker_states[busy_shard]["proc"])
        close_node_xray(node)

    def recover_dead_node(node_index: int) -> tuple[int, bool, str, Any, Any, str]:
        node = active_nodes[node_index]
        proc = None
        handle = None
        try:
            if PROXY_DEAD_RESTART_BEFORE_CHECK:
                close_node_xray(node)
                proc, handle = start_xray_node(
                    xray_path,
                    Path(node["config_path"]),
                    int(node["port"]),
                    Path(node["xray_log"]),
                    logger=logger,
                    source=f"XRAY-{node_index}",
                )
            else:
                proc = node.get("xray_proc")
                handle = node.get("xray_log_handle")
            if proc is None or proc.poll() is not None:
                raise RuntimeError("xray process did not start")
            outbound_ip = proxy_text_request(
                node["proxy"],
                VLESS_IP_CHECK_URL,
                timeout=PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
            ).strip()
            if not outbound_ip:
                raise RuntimeError("empty outbound IP")
            return node_index, True, outbound_ip, proc, handle, ""
        except Exception as exc:
            if proc is not None:
                stop_process(proc)
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
            return node_index, False, "", None, None, repr(exc)

    def save_status() -> None:
        payload = {
            "updatedAt": int(time.time()),
            "nodes": [public_node(node) for node in active_nodes],
            "completedShards": sorted(completed_shards),
            "pendingShards": list(pending_shards),
            "runningShards": {
                str(shard): {
                    "node": state["node_index"],
                    "pid": state["proc"].pid,
                    "attempt": state["attempt"],
                }
                for shard, state in worker_states.items()
            },
            "permanentlyFailed": permanently_failed,
            "assignmentHistory": assignment_history,
        }
        (root / "proxy_failover_status.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def worker_command(shard_index: int, node: dict[str, Any]) -> tuple[list[str], Path, Path]:
        part_dir = root / f"part_{shard_index:03d}"
        ensure_dir(part_dir)
        local_universe = part_dir / "wallet_universe.csv"
        # در شروع هر اجرای Manager، universe اصلی دوباره روی هر part کپی می‌شود تا
        # اگر نسخه قدیمی آن را purge کرده بود، ترتیب modulo خراب نشود. در retryهای
        # همان اجرا دیگر بازنویسی نمی‌شود و ورودی shard ثابت می‌ماند.
        if shard_attempts.get(shard_index, 0) == 0 or not local_universe.exists():
            shutil.copy2(universe_file, local_universe)

        command = [
            sys.executable,
            str(script_path),
            "--worker",
            "--proxy",
            node["proxy"],
            "--shard-count",
            str(shard_count),
            "--shard-index",
            str(shard_index),
            "--out-dir",
            str(part_dir),
            "--wallet-universe-file",
            str(local_universe),
            "--fallback-out-dir",
            str(fallback_out_dir),
            "--skip-final-xlsx",
        ]
        for option, value in [
            ("--delay", args.delay),
            ("--timeout", args.timeout),
            ("--retries", args.retries),
            ("--max-offset", args.max_offset),
            ("--max-wallets", args.max_wallets),
            ("--min-positions", args.min_positions),
            ("--min-losses", args.min_losses),
            ("--min-pnl", args.min_pnl),
            ("--smoothing", args.smoothing),
            ("--max-positions-per-wallet", args.max_positions_per_wallet),
        ]:
            if value is not None:
                command.extend([option, str(value)])
        return command, part_dir, local_universe

    def launch_shard(shard_index: int, node_index: int, reason: str) -> None:
        node = active_nodes[node_index]
        if not node.get("healthy"):
            raise RuntimeError(f"cannot launch shard on unhealthy node {node_index}")
        if node.get("busy_shard") is not None:
            raise RuntimeError(f"node {node_index} is already busy")
        attempt = shard_attempts.get(shard_index, 0)
        command, part_dir, _ = worker_command(shard_index, node)
        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
        )
        prefix = f"P{shard_index}" if attempt == 0 else f"P{shard_index}R{attempt}"
        thread = threading.Thread(
            target=stream_process_output,
            args=(proc, prefix, logger),
            daemon=True,
        )
        thread.start()
        output_threads.append(thread)
        worker_states[shard_index] = {
            "proc": proc,
            "thread": thread,
            "node_index": node_index,
            "attempt": attempt,
            "started_at": time.time(),
            "forced_stop": False,
        }
        node["busy_shard"] = shard_index
        shard_last_node[shard_index] = node_index
        event = {
            "time": int(time.time()),
            "shard": shard_index,
            "node": node_index,
            "ip": node["ip"],
            "attempt": attempt,
            "reason": reason,
            "pid": proc.pid,
        }
        assignment_history.append(event)
        print(
            f"[worker:start] shard={shard_index}/{shard_count} attempt={attempt} "
            f"node={node_index} ip={node['ip']} pid={proc.pid} reason={reason} output={part_dir}",
            flush=True,
        )

    def queue_shard(shard_index: int, reason: str) -> None:
        if (
            shard_index in completed_shards
            or shard_index in permanently_failed
            or shard_index in worker_states
            or shard_index in pending_set
        ):
            return
        pending_shards.append(shard_index)
        pending_set.add(shard_index)
        print(f"[failover:queue] shard={shard_index} reason={reason}", flush=True)

    def idle_healthy_nodes() -> list[int]:
        return [
            index
            for index, node in enumerate(active_nodes)
            if node.get("healthy") and node.get("busy_shard") is None
        ]

    def choose_node(shard_index: int, candidates: list[int]) -> int:
        last_node = shard_last_node.get(shard_index)
        alternatives = [idx for idx in candidates if idx != last_node]
        return alternatives[0] if alternatives else candidates[0]

    def check_node(node_index: int) -> tuple[int, bool, str, str]:
        node = active_nodes[node_index]
        proc = node.get("xray_proc")
        if proc is None or proc.poll() is not None:
            return node_index, False, "", "xray process exited"
        if not port_is_open(node["port"]):
            return node_index, False, "", "local proxy port is closed"
        try:
            outbound_ip = proxy_text_request(
                node["proxy"],
                VLESS_IP_CHECK_URL,
                timeout=PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
            ).strip()
            if not outbound_ip:
                return node_index, False, "", "empty outbound IP"
            return node_index, True, outbound_ip, ""
        except Exception as exc:
            return node_index, False, "", repr(exc)

    try:
        for source_index, link in enumerate(links):
            port = next_free_local_port(next_port)
            next_port = port + 1
            link_hash = hashlib.sha256(link.encode("utf-8")).hexdigest()[:12]
            config_path = runtime_dir / f"node_{source_index:03d}_{link_hash}.json"
            xray_log = runtime_dir / f"node_{source_index:03d}_{link_hash}_xray.log"
            proxy_url = f"http://127.0.0.1:{port}"
            node_name = f"node-{source_index}"
            protocol = "unknown"
            proc = None
            log_handle = None
            config_ready = False
            try:
                config, node_name, protocol = parse_proxy_link(link, port)
                config_path.write_text(
                    json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                config_ready = True
                proc, log_handle = start_xray_node(
                    xray_path,
                    config_path,
                    port,
                    xray_log,
                    logger=logger,
                    source=f"XRAY-{source_index}",
                )
                xray_handles.append((proc, log_handle))
                outbound_ip = "unchecked"
                if VLESS_CHECK_OUTBOUND_IP and not args.skip_ip_check:
                    outbound_ip = proxy_text_request(proxy_url, VLESS_IP_CHECK_URL).strip()
                    if not outbound_ip:
                        raise RuntimeError("empty outbound IP response")
                    if VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS and outbound_ip in used_ips:
                        raise RuntimeError(
                            f"duplicate outbound IP {outbound_ip}; node starts in standby and will be rechecked"
                        )
                used_ips.add(outbound_ip)
                active_nodes.append(
                    {
                        "source_index": source_index,
                        "name": node_name,
                        "protocol": protocol,
                        "proxy": proxy_url,
                        "port": port,
                        "ip": outbound_ip,
                        "hash": link_hash,
                        "xray_proc": proc,
                        "xray_log_handle": log_handle,
                        "config_path": str(config_path),
                        "xray_log": str(xray_log),
                        "healthy": True,
                        "health_failures": 0,
                        "last_health_error": "",
                        "busy_shard": None,
                        "next_dead_recheck_at": 0.0,
                        "recovery_count": 0,
                    }
                )
                print(
                    f"[proxy:ok] node={source_index} protocol={protocol} name={node_name!r} "
                    f"local={proxy_url} outbound_ip={outbound_ip}",
                    flush=True,
                )
            except Exception as exc:
                error_text = repr(exc)
                print(f"[proxy:startup-failed] node={source_index} error={error_text}", flush=True)
                if proc is not None:
                    stop_process(proc)
                if log_handle is not None:
                    try:
                        log_handle.close()
                    except Exception:
                        pass
                if config_ready:
                    active_nodes.append(
                        {
                            "source_index": source_index,
                            "name": node_name,
                            "protocol": protocol,
                            "proxy": proxy_url,
                            "port": port,
                            "ip": "unavailable",
                            "hash": link_hash,
                            "xray_proc": None,
                            "xray_log_handle": None,
                            "config_path": str(config_path),
                            "xray_log": str(xray_log),
                            "healthy": False,
                            "health_failures": PROXY_HEALTH_FAILURE_THRESHOLD,
                            "last_health_error": error_text,
                            "busy_shard": None,
                            "next_dead_recheck_at": (
                                time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
                            ),
                            "recovery_count": 0,
                        }
                    )

            if CLEAN_CONSOLE_DASHBOARD:
                console_line(
                    f"Testing VPNs: {source_index + 1}/{len(links)} | "
                    f"Active unique IPs: "
                    f"{sum(1 for node in active_nodes if node.get('healthy'))}"
                )

        if CLEAN_CONSOLE_DASHBOARD:
            console_line("", newline=True)

        initial_healthy_indexes = [
            index for index, node in enumerate(active_nodes) if node.get("healthy")
        ]
        if not initial_healthy_indexes:
            print(
                "No usable proxy node is healthy at startup. "
                "The manager needs at least one initial IP to create shards.",
                file=sys.stderr,
            )
            return 2

        shard_count = len(initial_healthy_indexes)
        print(
            f"[proxy] active_unique_ips={shard_count} total_parsed_nodes={len(active_nodes)} "
            "wallets will be round-robin sharded",
            flush=True,
        )
        print(
            f"[failover] enabled={PROXY_FAILOVER_ENABLED} health_interval="
            f"{PROXY_HEALTH_CHECK_INTERVAL_SECONDS}s failure_threshold="
            f"{PROXY_HEALTH_FAILURE_THRESHOLD} max_attempts="
            f"{PROXY_FAILOVER_MAX_ATTEMPTS_PER_SHARD}",
            flush=True,
        )

        for shard_index, node_index in enumerate(initial_healthy_indexes):
            shard_attempts[shard_index] = 0
            shard_last_node[shard_index] = None
            launch_shard(shard_index, node_index, reason="initial")

        show_dashboard(force=True)
        next_health_check = time.monotonic() + PROXY_HEALTH_CHECK_INTERVAL_SECONDS
        while len(completed_shards) + len(permanently_failed) < shard_count:
            made_progress = False

            # Workerهای تمام‌شده را جمع کن و shard ناقص را برای اجرای دوباره صف کن.
            for shard_index, state in list(worker_states.items()):
                proc = state["proc"]
                return_code = proc.poll()
                if return_code is None:
                    continue
                made_progress = True
                state["thread"].join(timeout=2)
                node_index = state["node_index"]
                node = active_nodes[node_index]
                if node.get("busy_shard") == shard_index:
                    node["busy_shard"] = None
                del worker_states[shard_index]

                if return_code == 0:
                    completed_shards.add(shard_index)
                    print(
                        f"[worker:done] shard={shard_index} node={node_index} return_code=0",
                        flush=True,
                    )
                else:
                    if return_code == 75 and node.get("healthy"):
                        mark_node_dead(
                            node_index,
                            "worker reported consecutive proxy failures",
                        )
                    shard_attempts[shard_index] = shard_attempts.get(shard_index, 0) + 1
                    reason = (
                        "node health check stopped worker"
                        if state.get("forced_stop")
                        else f"worker return_code={return_code}"
                    )
                    if shard_attempts[shard_index] > PROXY_FAILOVER_MAX_ATTEMPTS_PER_SHARD:
                        permanently_failed[shard_index] = reason
                        print(
                            f"[failover:give-up] shard={shard_index} attempts="
                            f"{shard_attempts[shard_index]} reason={reason}",
                            flush=True,
                        )
                    else:
                        queue_shard(shard_index, reason)

            now = time.monotonic()
            if PROXY_FAILOVER_ENABLED and now >= next_health_check:
                healthy_indexes = [
                    index for index, node in enumerate(active_nodes) if node.get("healthy")
                ]
                if healthy_indexes:
                    with ThreadPoolExecutor(max_workers=min(len(healthy_indexes), 16)) as executor:
                        futures = [executor.submit(check_node, index) for index in healthy_indexes]
                        for future in as_completed(futures):
                            node_index, ok, outbound_ip, error = future.result()
                            node = active_nodes[node_index]
                            if ok:
                                # IP عوض‌شده فقط وقتی پذیرفته می‌شود که با نود سالم دیگری تکراری نباشد.
                                duplicate = any(
                                    other_index != node_index
                                    and other.get("healthy")
                                    and other.get("ip") == outbound_ip
                                    for other_index, other in enumerate(active_nodes)
                                )
                                if duplicate and VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS:
                                    ok = False
                                    error = f"outbound IP changed to duplicate {outbound_ip}"
                                else:
                                    if outbound_ip != node.get("ip"):
                                        print(
                                            f"[proxy:ip-change] node={node_index} "
                                            f"old={node.get('ip')} new={outbound_ip}",
                                            flush=True,
                                        )
                                        node["ip"] = outbound_ip
                                    node["health_failures"] = 0
                                    node["last_health_error"] = ""

                            if not ok:
                                node["health_failures"] = int(node.get("health_failures", 0)) + 1
                                node["last_health_error"] = error
                                print(
                                    f"[proxy:health-fail] node={node_index} "
                                    f"count={node['health_failures']}/{PROXY_HEALTH_FAILURE_THRESHOLD} "
                                    f"error={error}",
                                    flush=True,
                                )
                                if node["health_failures"] >= PROXY_HEALTH_FAILURE_THRESHOLD:
                                    mark_node_dead(node_index, error)
                next_health_check = time.monotonic() + PROXY_HEALTH_CHECK_INTERVAL_SECONDS
                save_status()

            # نودهای dead دوره‌ای Restart و دوباره تست می‌شوند؛ حذف دائمی نیستند.
            if PROXY_DEAD_RECHECK_ENABLED:
                due_dead_indexes = [
                    index
                    for index, node in enumerate(active_nodes)
                    if (
                        not node.get("healthy")
                        and now >= float(node.get("next_dead_recheck_at", 0.0))
                    )
                ]
                if due_dead_indexes:
                    print(
                        f"[proxy:dead-recheck] due_nodes={due_dead_indexes}",
                        flush=True,
                    )
                    with ThreadPoolExecutor(
                        max_workers=min(len(due_dead_indexes), 8)
                    ) as executor:
                        futures = [
                            executor.submit(recover_dead_node, index)
                            for index in due_dead_indexes
                        ]
                        for future in as_completed(futures):
                            (
                                node_index,
                                ok,
                                outbound_ip,
                                recovered_proc,
                                recovered_handle,
                                error,
                            ) = future.result()
                            node = active_nodes[node_index]
                            duplicate = bool(
                                ok
                                and VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS
                                and any(
                                    other_index != node_index
                                    and other.get("healthy")
                                    and other.get("ip") == outbound_ip
                                    for other_index, other in enumerate(active_nodes)
                                )
                            )
                            if duplicate:
                                ok = False
                                error = f"recovered with duplicate outbound IP {outbound_ip}"
                            if ok:
                                node["xray_proc"] = recovered_proc
                                node["xray_log_handle"] = recovered_handle
                                xray_handles.append((recovered_proc, recovered_handle))
                                old_ip = node.get("ip")
                                node["ip"] = outbound_ip
                                node["healthy"] = True
                                node["health_failures"] = 0
                                node["last_health_error"] = ""
                                node["next_dead_recheck_at"] = 0.0
                                node["recovery_count"] = int(node.get("recovery_count", 0)) + 1
                                print(
                                    f"[proxy:recovered] node={node_index} "
                                    f"old_ip={old_ip} new_ip={outbound_ip} "
                                    f"recoveries={node['recovery_count']}",
                                    flush=True,
                                )
                                all_nodes_down_notice_logged = False
                            else:
                                if recovered_proc is not None:
                                    stop_process(recovered_proc)
                                if recovered_handle is not None:
                                    try:
                                        recovered_handle.close()
                                    except Exception:
                                        pass
                                node["xray_proc"] = None
                                node["xray_log_handle"] = None
                                node["last_health_error"] = error
                                node["next_dead_recheck_at"] = (
                                    time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
                                )
                                print(
                                    f"[proxy:still-dead] node={node_index} "
                                    f"next_recheck={PROXY_DEAD_RECHECK_INTERVAL_SECONDS}s "
                                    f"error={error}",
                                    flush=True,
                                )
                    save_status()

            # فقط به نود سالم و بیکار کار بده؛ هر IP در هر لحظه حداکثر یک Worker دارد.
            while pending_shards:
                candidates = idle_healthy_nodes()
                if not candidates:
                    break
                shard_index = pending_shards.popleft()
                pending_set.discard(shard_index)
                node_index = choose_node(shard_index, candidates)
                if PROXY_FAILOVER_RETRY_DELAY_SECONDS:
                    time.sleep(PROXY_FAILOVER_RETRY_DELAY_SECONDS)
                launch_shard(shard_index, node_index, reason="failover")
                made_progress = True

            if pending_shards and not worker_states and not idle_healthy_nodes():
                reason = "no healthy proxy node remains; waiting for dead-node recheck"
                if PROXY_DEAD_RECHECK_ENABLED:
                    if not all_nodes_down_notice_logged:
                        print(f"[failover:wait] {reason}", flush=True)
                        all_nodes_down_notice_logged = True
                else:
                    while pending_shards:
                        shard_index = pending_shards.popleft()
                        pending_set.discard(shard_index)
                        permanently_failed[shard_index] = reason
                    print(f"[failover:stop] {reason}", flush=True)
                    break

            show_dashboard()
            show_error_notice_if_due()
            if made_progress:
                save_status()
            time.sleep(0.5)

        save_status()
        status = 0 if not permanently_failed else 1
        print(
            f"[failover:summary] completed={len(completed_shards)}/{shard_count} "
            f"failed={len(permanently_failed)}",
            flush=True,
        )

        if VLESS_AUTO_MERGE_OUTPUTS:
            merge_vless_outputs(root, fallback_out_dir=fallback_out_dir)
        return status

    except KeyboardInterrupt:
        print("[stop] Ctrl+C received; stopping workers and Xray nodes...", flush=True)
        for state in list(worker_states.values()):
            stop_process(state.get("proc"))
        console_line("", newline=True)
        console_stdout.write("Stopped by user. Progress is saved and will resume next run.\n")
        console_stdout.flush()
        return 130
    except Exception as exc:
        logger.log(
            "Unhandled manager exception:\n" + traceback.format_exc(),
            source="FATAL",
            force_error=True,
        )
        console_line("", newline=True)
        console_stdout.write(
            f"Fatal error: {exc!r}. Read {root / ERROR_LOG_FILE_NAME}\n"
        )
        console_stdout.flush()
        return 2
    finally:
        for state in list(worker_states.values()):
            stop_process(state.get("proc"))
        for thread in output_threads:
            thread.join(timeout=1)
        for node in active_nodes:
            close_node_xray(node)
        for proc, log_handle in reversed(xray_handles):
            stop_process(proc)
            try:
                log_handle.close()
            except Exception:
                pass
        try:
            show_error_notice_if_due(force=True)
            show_dashboard(force=True)
            console_line("", newline=True)
        except Exception:
            pass
        if CLEAN_CONSOLE_DASHBOARD:
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:
                pass
            sys.stdout = console_stdout
            sys.stderr = console_stderr
        logger.log("Run finished", source="SYSTEM")
        logger.close()

# =============================================================================
# صف جهانی پایدار: جایگزین پارت‌بندی ثابت
# =============================================================================
GLOBAL_QUEUE_ENABLED = True
GLOBAL_QUEUE_DB_FILE_NAME = "global_state.sqlite3"
GLOBAL_QUEUE_CACHE_DIR_NAME = "_queue_cache"
GLOBAL_QUEUE_RUNTIME_DIR_NAME = "_queue_runtime"
GLOBAL_QUEUE_BUCKET_COUNT = 512
GLOBAL_QUEUE_BATCH_SIZE = 8
GLOBAL_QUEUE_MAX_ATTEMPTS_PER_WALLET = 0  # صفر یعنی نامحدود؛ والت به‌خاطر قطعی موقت رها نمی‌شود.
GLOBAL_QUEUE_IMPORT_OLD_PARTS = True
GLOBAL_QUEUE_FINAL_MERGE_ON_EXIT = True
GLOBAL_QUEUE_MERGE_RAW_JSONL = False
ACTIVE_VPN_FILE_NAME = "active_vpns.txt"
ACTIVE_VPN_UPDATE_INTERVAL_SECONDS = 10.0


def _seed_to_json(seed: WalletSeed) -> str:
    return json.dumps(
        {
            "proxy_wallet": seed.proxy_wallet,
            "user_name": seed.user_name,
            "x_username": seed.x_username,
            "verified_badge": bool(seed.verified_badge),
            "best_pnl": seed.best_pnl,
            "best_vol": seed.best_vol,
            "profile_views": seed.profile_views,
            "leaderboard_hits": seed.leaderboard_hits,
            "best_rank_seen": seed.best_rank_seen,
            "modes": sorted(seed.modes),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _seed_from_json(value: str) -> WalletSeed:
    row = json.loads(value)
    return WalletSeed(
        proxy_wallet=str(row.get("proxy_wallet") or "").lower(),
        user_name=str(row.get("user_name") or ""),
        x_username=str(row.get("x_username") or ""),
        verified_badge=bool(row.get("verified_badge")),
        best_pnl=safe_float(row.get("best_pnl")),
        best_vol=safe_float(row.get("best_vol")),
        profile_views=int(safe_float(row.get("profile_views"))),
        leaderboard_hits=int(safe_float(row.get("leaderboard_hits"))),
        best_rank_seen=int(safe_float(row.get("best_rank_seen"), 10**9)),
        modes=set(row.get("modes") or []),
    )


def _wallet_bucket(wallet: str) -> int:
    digest = hashlib.sha256(wallet.lower().encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % max(int(GLOBAL_QUEUE_BUCKET_COUNT), 1)


def _atomic_write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(text, encoding="utf-8")
    os.replace(temp, path)


def _terminate_pid(pid: int) -> None:
    if pid <= 0 or pid == os.getpid():
        return
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=8,
                check=False,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
        else:
            os.kill(pid, 15)
    except Exception:
        pass


class GlobalQueueState:
    """SQLite WAL queue. Every state transition is committed before work continues."""

    def __init__(self, path: Path) -> None:
        ensure_dir(path.parent)
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, timeout=60.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._schema()

    def _schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS wallet_queue (
                wallet TEXT PRIMARY KEY,
                seed_json TEXT NOT NULL,
                priority INTEGER NOT NULL,
                bucket INTEGER NOT NULL,
                legacy_dir TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                assigned_node INTEGER,
                batch_id TEXT,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_wallet_queue_status_priority
                ON wallet_queue(status, priority);
            CREATE INDEX IF NOT EXISTS idx_wallet_queue_bucket_status
                ON wallet_queue(bucket, status, priority);
            CREATE TABLE IF NOT EXISTS runtime_processes (
                pid INTEGER PRIMARY KEY,
                kind TEXT NOT NULL,
                started_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS queue_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        try:
            self.conn.execute("ALTER TABLE wallet_queue ADD COLUMN active INTEGER NOT NULL DEFAULT 1")
        except sqlite3.OperationalError:
            pass
        self.conn.commit()

    def seed(self, seeds: list[WalletSeed]) -> None:
        now = int(time.time())
        with self.conn:
            self.conn.execute("UPDATE wallet_queue SET active=0")
            self.conn.executemany(
                """
                INSERT INTO wallet_queue(
                    wallet, seed_json, priority, bucket, status, updated_at, active
                ) VALUES (?, ?, ?, ?, 'pending', ?, 1)
                ON CONFLICT(wallet) DO UPDATE SET
                    seed_json=excluded.seed_json,
                    priority=excluded.priority,
                    bucket=excluded.bucket,
                    updated_at=excluded.updated_at,
                    active=1
                """,
                (
                    (
                        seed.proxy_wallet,
                        _seed_to_json(seed),
                        index,
                        _wallet_bucket(seed.proxy_wallet),
                        now,
                    )
                    for index, seed in enumerate(seeds)
                ),
            )
            allowed = {seed.proxy_wallet for seed in seeds}
            # max_wallets may have changed. Rows outside the current universe remain archived,
            # but do not contribute to progress or receive work.
            self.conn.execute(
                "INSERT OR REPLACE INTO queue_meta(key,value) VALUES('current_wallets_json', ?)",
                (json.dumps(sorted(allowed)),),
            )

    def current_wallets(self) -> set[str]:
        row = self.conn.execute(
            "SELECT value FROM queue_meta WHERE key='current_wallets_json'"
        ).fetchone()
        if not row:
            return set()
        try:
            return set(json.loads(str(row[0])))
        except Exception:
            return set()

    def kill_and_clear_stale_processes(self) -> None:
        rows = list(self.conn.execute("SELECT pid FROM runtime_processes"))
        for row in rows:
            _terminate_pid(int(row[0]))
        with self.conn:
            self.conn.execute("DELETE FROM runtime_processes")

    def register_pid(self, pid: int, kind: str) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO runtime_processes(pid,kind,started_at) VALUES(?,?,?)",
                (int(pid), kind, int(time.time())),
            )

    def unregister_pid(self, pid: int | None) -> None:
        if not pid:
            return
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM runtime_processes WHERE pid=?", (int(pid),))

    def recover_interrupted(self) -> int:
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE wallet_queue
                SET status='pending', assigned_node=NULL, batch_id=NULL,
                    last_error=CASE WHEN last_error='' THEN 'recovered after interrupted run' ELSE last_error END,
                    updated_at=?
                WHERE status='running' AND active=1
                """,
                (int(time.time()),),
            )
        return int(cursor.rowcount or 0)

    def mark_done(self, wallets: set[str]) -> int:
        if not wallets:
            return 0
        now = int(time.time())
        changed = 0
        with self.conn:
            for wallet in wallets:
                cursor = self.conn.execute(
                    """
                    UPDATE wallet_queue
                    SET status='done', assigned_node=NULL, batch_id=NULL,
                        completed_at=COALESCE(completed_at, ?), updated_at=?
                    WHERE wallet=? AND active=1 AND status!='done'
                    """,
                    (now, now, wallet),
                )
                changed += int(cursor.rowcount or 0)
        return changed

    def set_legacy_dir(self, wallets: set[str], directory: Path) -> int:
        if not wallets:
            return 0
        changed = 0
        with self.conn:
            for wallet in wallets:
                cursor = self.conn.execute(
                    """
                    UPDATE wallet_queue SET legacy_dir=?, updated_at=?
                    WHERE wallet=? AND active=1 AND legacy_dir=''
                    """,
                    (str(directory.resolve()), int(time.time()), wallet),
                )
                changed += int(cursor.rowcount or 0)
        return changed

    def claim_batch(
        self,
        node_index: int,
        busy_buckets: set[int],
    ) -> tuple[str, int, str, list[WalletSeed]] | None:
        current = self.current_wallets()
        if not current:
            return None
        excluded = sorted(int(value) for value in busy_buckets)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            query = "SELECT wallet,bucket,legacy_dir FROM wallet_queue WHERE status='pending' AND active=1"
            params: list[Any] = []
            if excluded:
                query += " AND bucket NOT IN (" + ",".join("?" for _ in excluded) + ")"
                params.extend(excluded)
            query += " ORDER BY priority LIMIT 1"
            first = self.conn.execute(query, params).fetchone()
            if first is None:
                self.conn.commit()
                return None
            bucket = int(first["bucket"])
            legacy_dir = str(first["legacy_dir"] or "")
            rows = list(
                self.conn.execute(
                    """
                    SELECT wallet,seed_json FROM wallet_queue
                    WHERE status='pending' AND active=1 AND bucket=? AND legacy_dir=?
                    ORDER BY priority LIMIT ?
                    """,
                    (bucket, legacy_dir, max(int(GLOBAL_QUEUE_BATCH_SIZE), 1)),
                )
            )
            if not rows:
                self.conn.commit()
                return None
            wallets = [str(row["wallet"]) for row in rows]
            batch_id = f"{int(time.time())}-{node_index}-{hashlib.sha1('|'.join(wallets).encode()).hexdigest()[:10]}"
            placeholders = ",".join("?" for _ in wallets)
            self.conn.execute(
                f"""
                UPDATE wallet_queue
                SET status='running', attempts=attempts+1, assigned_node=?,
                    batch_id=?, updated_at=?
                WHERE wallet IN ({placeholders}) AND active=1 AND status='pending'
                """,
                (node_index, batch_id, int(time.time()), *wallets),
            )
            self.conn.commit()
            return batch_id, bucket, legacy_dir, [_seed_from_json(str(row["seed_json"])) for row in rows]
        except Exception:
            self.conn.rollback()
            raise

    def finish_batch(
        self,
        wallets: set[str],
        completed: set[str],
        error: str,
    ) -> tuple[int, int]:
        now = int(time.time())
        done = 0
        requeued = 0
        with self.conn:
            for wallet in wallets:
                if wallet in completed:
                    cursor = self.conn.execute(
                        """
                        UPDATE wallet_queue
                        SET status='done', assigned_node=NULL, batch_id=NULL,
                            completed_at=COALESCE(completed_at, ?), updated_at=?
                        WHERE wallet=? AND active=1
                        """,
                        (now, now, wallet),
                    )
                    done += int(cursor.rowcount or 0)
                    continue
                row = self.conn.execute(
                    "SELECT attempts FROM wallet_queue WHERE wallet=?", (wallet,)
                ).fetchone()
                attempts = int(row[0]) if row else 0
                final = bool(
                    GLOBAL_QUEUE_MAX_ATTEMPTS_PER_WALLET > 0
                    and attempts >= GLOBAL_QUEUE_MAX_ATTEMPTS_PER_WALLET
                )
                self.conn.execute(
                    """
                    UPDATE wallet_queue
                    SET status=?, assigned_node=NULL, batch_id=NULL,
                        last_error=?, updated_at=?
                    WHERE wallet=?
                    """,
                    ("failed_final" if final else "pending", error[:4000], now, wallet),
                )
                if not final:
                    requeued += 1
        return done, requeued

    def sync_completed_from_dirs(self, directories: list[Path]) -> int:
        completed: set[str] = set()
        for directory in directories:
            completed |= load_test_memory(directory / TEST_MEMORY_FILE_NAME)
        return self.mark_done(completed)

    def counts(self) -> dict[str, int]:
        counts = {"done": 0, "pending": 0, "running": 0, "failed": 0, "total": 0}
        for row in self.conn.execute(
            "SELECT status, COUNT(*) FROM wallet_queue WHERE active=1 GROUP BY status"
        ):
            status = str(row[0])
            value = int(row[1])
            counts["total"] += value
            if status == "done":
                counts["done"] += value
            elif status == "running":
                counts["running"] += value
            elif status == "failed_final":
                counts["failed"] += value
            else:
                counts["pending"] += value
        return counts

    def all_finished(self) -> bool:
        counts = self.counts()
        return counts["total"] > 0 and counts["done"] + counts["failed"] >= counts["total"]

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


def _wallets_in_complete_cache(path: Path) -> set[str]:
    if not path.exists():
        return set()
    result: set[str] = set()
    try:
        uri = path.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10.0)
        try:
            for table in ("activity_state", "activity_markets", "closed_market_rows"):
                try:
                    result |= {
                        str(row[0]).lower()
                        for row in conn.execute(f"SELECT DISTINCT wallet FROM {table}")
                        if row and row[0]
                    }
                except sqlite3.Error:
                    pass
        finally:
            conn.close()
    except Exception:
        pass
    return result


def _discover_resume_sources(root: Path, fallback: Path | None) -> list[Path]:
    result: list[Path] = []
    if fallback is not None and fallback.exists():
        result.append(fallback.resolve())
    if root.exists():
        result.extend(sorted(path.resolve() for path in root.glob("part_*") if path.is_dir()))
        cache_root = root / GLOBAL_QUEUE_CACHE_DIR_NAME
        if cache_root.exists():
            result.extend(sorted(path.resolve() for path in cache_root.glob("bucket_*") if path.is_dir()))
    # preserve order, remove duplicates
    seen: set[str] = set()
    unique: list[Path] = []
    for path in result:
        key = str(path).lower()
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _import_old_state(queue: GlobalQueueState, root: Path, fallback: Path | None, logger: RunLogRouter) -> list[Path]:
    sources = _discover_resume_sources(root, fallback)
    completed: set[str] = set()
    for directory in sources:
        completed |= load_test_memory(directory / TEST_MEMORY_FILE_NAME)
        cache_wallets = _wallets_in_complete_cache(directory / COMPLETE_FETCH_CACHE_DB_FILE_NAME)
        if cache_wallets:
            queue.set_legacy_dir(cache_wallets, directory)
    changed = queue.mark_done(completed)
    logger.log(
        f"resume import sources={len(sources)} completed_seen={len(completed)} newly_marked={changed}",
        source="QUEUE",
    )
    return sources


def _write_active_vpn_file(root: Path, nodes: list[dict[str, Any]]) -> None:
    healthy = [node for node in nodes if node.get("healthy")]
    lines = [
        f"Updated: {_log_timestamp()}",
        f"Active VPNs: {len(healthy)}/{len(nodes)}",
        "",
    ]
    for index, node in enumerate(healthy, start=1):
        busy = node.get("busy_batch") or "idle"
        lines.append(
            f"{index}. node={node.get('source_index')} protocol={node.get('protocol')} "
            f"name={node.get('name')} ip={node.get('ip')} local_port={node.get('port')} "
            f"work={busy}"
        )
    if not healthy:
        lines.append("No active VPN. Dead/startup-failed nodes are still rechecked periodically.")
    _atomic_write_text(root / ACTIVE_VPN_FILE_NAME, "\n".join(lines) + "\n")


def _merge_global_outputs(root: Path, sources: list[Path], universe_file: Path) -> None:
    # Internal buckets and legacy parts are implementation details. User-facing files are written
    # directly into the single root folder.
    score_rows_by_wallet: dict[str, dict[str, Any]] = {}
    for directory in sources:
        for score_source in (
            directory / "edge_scores_progress.csv",
            directory / SCORE_JOURNAL_FILE_NAME,
        ):
            for row in load_progress_scores(score_source):
                wallet = str(row.get("proxyWallet") or "").lower()
                if wallet:
                    score_rows_by_wallet[wallet] = row

    fieldnames = get_score_fieldnames()
    write_sorted_scores_csv(score_rows_by_wallet.values(), root / "edge_scores_progress.csv", fieldnames)
    write_all_score_outputs(score_rows_by_wallet.values(), root / "edge_scores.xlsx", root, fieldnames)

    memory_sources = [directory / TEST_MEMORY_FILE_NAME for directory in sources]
    merge_csv_by_wallet(
        memory_sources,
        root / TEST_MEMORY_FILE_NAME,
        ["proxyWallet", "userName", "status", "reason", "testedAt"],
    )
    completed: set[str] = set()
    for directory in sources:
        completed |= load_test_memory(directory / TEST_MEMORY_FILE_NAME)
    failures: dict[tuple[str, str], dict[str, str]] = {}
    for directory in sources:
        path = directory / "closed_positions_failed.csv"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            for row in csv.DictReader(file):
                wallet = str(row.get("proxyWallet") or "").lower()
                error = str(row.get("error") or "")
                if wallet and wallet not in completed:
                    failures[(wallet, error)] = {"proxyWallet": wallet, "error": error}
    with (root / "closed_positions_failed.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["proxyWallet", "error"])
        writer.writeheader()
        writer.writerows(failures.values())
    if universe_file.exists():
        destination = root / "wallet_universe.csv"
        if universe_file.resolve() != destination.resolve():
            shutil.copy2(universe_file, destination)
    if GLOBAL_QUEUE_MERGE_RAW_JSONL:
        merge_raw_jsonl(
            [directory / RAW_CLOSED_POSITIONS_LOG_FILE_NAME for directory in reversed(sources)],
            root / RAW_CLOSED_POSITIONS_LOG_FILE_NAME,
        )
    summary = {
        "mergedAt": int(time.time()),
        "sourceDirectories": [str(path) for path in sources],
        "scoredWallets": len(score_rows_by_wallet),
        "globalQueueDatabase": str(root / GLOBAL_QUEUE_DB_FILE_NAME),
        "rawJsonlMerged": bool(GLOBAL_QUEUE_MERGE_RAW_JSONL),
    }
    (root / "merge_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def run_global_queue_manager(args: argparse.Namespace) -> int:
    links, vpn_file = load_proxy_links_from_file(args.vpn_list_file)
    if not links:
        print(
            f"No supported proxy link was found in: {vpn_file}\n"
            "Paste vless/vmess/trojan/ss links one per line, then run the program again. "
            "The global queue and all completed work will resume automatically.",
            file=sys.stderr,
        )
        return 2
    xray_path = find_xray_executable(args.xray)
    if xray_path is None:
        print("xray.exe was not found. Put it next to the Python file or set XRAY_EXECUTABLE.", file=sys.stderr)
        return 2

    fallback = Path(args.fallback_out_dir or VLESS_FALLBACK_OUT_DIR).resolve()
    universe_file = Path(
        args.wallet_universe_file or (fallback / "wallet_universe.csv")
    ).resolve()
    if not universe_file.exists():
        print(f"Missing wallet universe: {universe_file}", file=sys.stderr)
        return 2

    root = Path(args.vless_root or VLESS_OUTPUT_ROOT).resolve()
    runtime_dir = root / GLOBAL_QUEUE_RUNTIME_DIR_NAME
    cache_root = root / GLOBAL_QUEUE_CACHE_DIR_NAME
    ensure_dir(root)
    ensure_dir(runtime_dir)
    ensure_dir(cache_root)
    root_universe = root / "wallet_universe.csv"
    if universe_file.resolve() != root_universe.resolve():
        shutil.copy2(universe_file, root_universe)

    console_stdout = sys.stdout
    console_stderr = sys.stderr
    logger = RunLogRouter(root / ALL_LOG_FILE_NAME, root / ERROR_LOG_FILE_NAME)
    logger.log(
        f"[vpn-list] file={vpn_file} parsed_links={len(links)}",
        source="SYSTEM",
    )
    if CLEAN_CONSOLE_DASHBOARD:
        sys.stdout = RoutedLogStream(logger, "MANAGER")
        sys.stderr = RoutedLogStream(logger, "STDERR", force_error=True)

    queue = GlobalQueueState(root / GLOBAL_QUEUE_DB_FILE_NAME)
    queue.kill_and_clear_stale_processes()

    wallets = sorted(
        load_wallet_universe(universe_file).values(),
        key=lambda item: item.best_pnl,
        reverse=True,
    )
    max_wallets = setting(args.max_wallets, MAX_WALLETS_TO_SCORE)
    if max_wallets:
        wallets = wallets[: int(max_wallets)]
    queue.seed(wallets)
    recovered_count = queue.recover_interrupted()
    legacy_sources = _import_old_state(queue, root, fallback, logger) if GLOBAL_QUEUE_IMPORT_OLD_PARTS else []

    logger.log("=" * 80, source="SYSTEM")
    logger.log(
        f"global queue run started wallets={len(wallets)} interrupted_requeued={recovered_count}",
        source="SYSTEM",
    )
    console_stdout.write(
        f"Global queue ready. Logs: {root / ALL_LOG_FILE_NAME} | "
        f"Errors: {root / ERROR_LOG_FILE_NAME} | Active VPNs: {root / ACTIVE_VPN_FILE_NAME}\n"
    )
    console_stdout.flush()

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    script_path = Path(__file__).resolve()
    nodes: list[dict[str, Any]] = []
    worker_states: dict[int, dict[str, Any]] = {}
    output_threads: list[threading.Thread] = []
    last_dashboard = 0.0
    last_error_notice = time.monotonic()
    last_active_file = 0.0
    last_console_width = 0

    def console_line(text: str, newline: bool = False) -> None:
        nonlocal last_console_width
        if not CLEAN_CONSOLE_DASHBOARD:
            return
        padded = text.ljust(max(last_console_width, len(text)))
        console_stdout.write("\r" + padded)
        if newline:
            console_stdout.write("\n")
            last_console_width = 0
        else:
            last_console_width = max(last_console_width, len(text))
        console_stdout.flush()

    def update_active_file(force: bool = False) -> None:
        nonlocal last_active_file
        now = time.monotonic()
        if force or now - last_active_file >= ACTIVE_VPN_UPDATE_INTERVAL_SECONDS:
            _write_active_vpn_file(root, nodes)
            last_active_file = now

    def show_dashboard(force: bool = False) -> None:
        nonlocal last_dashboard
        now = time.monotonic()
        if not force and now - last_dashboard < CONSOLE_STATUS_INTERVAL_SECONDS:
            return
        counts = queue.counts()
        active = sum(1 for node in nodes if node.get("healthy"))
        percent = counts["done"] / counts["total"] * 100.0 if counts["total"] else 100.0
        console_line(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"VPN Active: {active}/{len(nodes)} | "
            f"Wallets: {counts['done']}/{counts['total']} | "
            f"Done: {percent:.2f}% | Running: {counts['running']} | Pending: {counts['pending']}"
        )
        last_dashboard = now
        update_active_file()

    def show_error_notice(force: bool = False) -> None:
        nonlocal last_error_notice
        now = time.monotonic()
        if not force and now - last_error_notice < CONSOLE_ERROR_NOTICE_INTERVAL_SECONDS:
            return
        count = logger.consume_new_errors()
        last_error_notice = now
        if count:
            console_line("", newline=True)
            console_stdout.write(
                f"[{datetime.now().strftime('%H:%M:%S')}] ERROR NOTICE: "
                f"{count} new error log entr{'y' if count == 1 else 'ies'} in the last minute "
                f"-> read {root / ERROR_LOG_FILE_NAME}\n"
            )
            console_stdout.flush()
            show_dashboard(force=True)

    def start_or_restart_node(node: dict[str, Any]) -> tuple[bool, str]:
        old_proc = node.get("xray_proc")
        if old_proc is not None:
            queue.unregister_pid(getattr(old_proc, "pid", None))
            stop_process(old_proc)
        old_handle = node.get("xray_handle")
        if old_handle is not None:
            try:
                old_handle.close()
            except Exception:
                pass
        try:
            proc, handle = start_xray_node(
                xray_path,
                node["config_path"],
                int(node["port"]),
                node["log_path"],
                logger=logger,
                source=f"XRAY{node['source_index']}",
            )
            queue.register_pid(proc.pid, "xray")
            proxy_url = str(node["proxy"])
            outbound_ip = (
                proxy_text_request(proxy_url, VLESS_IP_CHECK_URL, timeout=PROXY_HEALTH_CHECK_TIMEOUT_SECONDS)
                if VLESS_CHECK_OUTBOUND_IP and not args.skip_ip_check
                else f"unchecked-{node['source_index']}"
            )
            duplicate = any(
                other is not node
                and other.get("healthy")
                and other.get("ip") == outbound_ip
                for other in nodes
            )
            if duplicate and VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS:
                queue.unregister_pid(proc.pid)
                stop_process(proc)
                try:
                    handle.close()
                except Exception:
                    pass
                return False, f"duplicate outbound IP {outbound_ip}"
            node["xray_proc"] = proc
            node["xray_handle"] = handle
            node["ip"] = outbound_ip
            node["healthy"] = True
            node["health_failures"] = 0
            node["last_error"] = ""
            node["next_recheck"] = 0.0
            return True, ""
        except Exception as exc:
            node["xray_proc"] = None
            node["xray_handle"] = None
            node["healthy"] = False
            node["last_error"] = repr(exc)
            node["next_recheck"] = time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
            return False, repr(exc)

    def mark_node_dead(node_index: int, reason: str) -> None:
        node = nodes[node_index]
        if not node.get("healthy") and node.get("next_recheck", 0):
            return
        node["healthy"] = False
        node["last_error"] = reason
        node["health_failures"] = 0
        node["next_recheck"] = time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
        proc = node.get("xray_proc")
        if proc is not None:
            queue.unregister_pid(proc.pid)
            stop_process(proc)
        handle = node.get("xray_handle")
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        node["xray_proc"] = None
        node["xray_handle"] = None
        worker = worker_states.get(node_index)
        if worker is not None:
            worker["forced_stop"] = True
            stop_process(worker["proc"])
        logger.log(f"[proxy:dead] node={node_index} reason={reason}", source="PROXY", force_error=True)
        update_active_file(force=True)

    # Parse every valid link first. Even startup-failed nodes remain in this list and are retried.
    next_port = VLESS_LOCAL_HTTP_PORT_START
    for source_index, link in enumerate(links):
        try:
            port = next_free_local_port(next_port)
            next_port = port + 1
            config, name, protocol = parse_proxy_link(link, port)
            node_dir = runtime_dir / f"node_{source_index:03d}"
            ensure_dir(node_dir)
            config_path = node_dir / "config.json"
            config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
            node = {
                "source_index": source_index,
                "name": name,
                "protocol": protocol,
                "port": port,
                "proxy": f"http://127.0.0.1:{port}",
                "config_path": config_path,
                "log_path": node_dir / "xray.log",
                "healthy": False,
                "health_failures": 0,
                "next_recheck": 0.0,
                "busy_batch": None,
                "ip": "",
                "xray_proc": None,
                "xray_handle": None,
                "last_error": "",
            }
            nodes.append(node)
        except Exception as exc:
            logger.log(
                f"[proxy:parse-failed] link_index={source_index} error={exc!r}",
                source="PROXY",
                force_error=True,
            )

    if not nodes:
        if CLEAN_CONSOLE_DASHBOARD:
            sys.stdout = console_stdout
            sys.stderr = console_stderr
        logger.close()
        queue.close()
        print("No parseable proxy link remains.", file=sys.stderr)
        return 2

    # Initial checks run in parallel. Zero active nodes is allowed; manager waits for recovery.
    with ThreadPoolExecutor(max_workers=min(len(nodes), 16)) as executor:
        futures = {executor.submit(start_or_restart_node, node): index for index, node in enumerate(nodes)}
        for future in as_completed(futures):
            index = futures[future]
            ok, error = future.result()
            if ok:
                logger.log(
                    f"[proxy:active] node={index} protocol={nodes[index]['protocol']} ip={nodes[index]['ip']}",
                    source="PROXY",
                )
            else:
                logger.log(
                    f"[proxy:startup-failed] node={index} error={error}",
                    source="PROXY",
                    force_error=True,
                )
    seen_initial_ips: dict[str, int] = {}
    for index, node in enumerate(nodes):
        if not node.get("healthy"):
            continue
        ip = str(node.get("ip") or "")
        if VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS and ip in seen_initial_ips:
            mark_node_dead(index, f"duplicate outbound IP {ip}; first_node={seen_initial_ips[ip]}")
        else:
            seen_initial_ips[ip] = index
    update_active_file(force=True)

    def all_cache_dirs() -> list[Path]:
        dirs = sorted(path for path in cache_root.glob("bucket_*") if path.is_dir())
        return legacy_sources + dirs

    def sync_completed() -> None:
        queue.sync_completed_from_dirs(all_cache_dirs())

    def launch_batch(node_index: int) -> bool:
        node = nodes[node_index]
        busy_buckets = {int(state["bucket"]) for state in worker_states.values()}
        claim = queue.claim_batch(node_index, busy_buckets)
        if claim is None:
            return False
        batch_id, bucket, legacy_dir, seeds = claim
        bucket_dir = cache_root / f"bucket_{bucket:03d}"
        ensure_dir(bucket_dir)
        batch_file = runtime_dir / f"batch_{batch_id}.csv"
        write_wallet_universe_csv({seed.proxy_wallet: seed for seed in seeds}, batch_file)
        command = [
            sys.executable,
            str(script_path),
            "2",
            "--worker",
            "--score-only",
            "--out-dir",
            str(bucket_dir),
            "--wallet-universe-file",
            str(batch_file),
            "--proxy",
            str(node["proxy"]),
            "--skip-final-xlsx",
        ]
        if legacy_dir:
            command.extend(["--fallback-out-dir", legacy_dir])
        if args.timeout is not None:
            command.extend(["--timeout", str(args.timeout)])
        if args.retries is not None:
            command.extend(["--retries", str(args.retries)])
        if args.delay is not None:
            command.extend(["--delay", str(args.delay)])
        if args.min_positions is not None:
            command.extend(["--min-positions", str(args.min_positions)])
        if args.min_losses is not None:
            command.extend(["--min-losses", str(args.min_losses)])
        if args.min_pnl is not None:
            command.extend(["--min-pnl", str(args.min_pnl)])
        if args.max_positions_per_wallet is not None:
            command.extend(["--max-positions-per-wallet", str(args.max_positions_per_wallet)])

        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=env,
            cwd=str(script_path.parent),
            creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0),
        )
        queue.register_pid(proc.pid, "worker")
        thread = threading.Thread(
            target=stream_process_output,
            args=(proc, f"N{node_index}", logger),
            daemon=True,
        )
        thread.start()
        output_threads.append(thread)
        wallets_set = {seed.proxy_wallet for seed in seeds}
        worker_states[node_index] = {
            "proc": proc,
            "batch_id": batch_id,
            "bucket": bucket,
            "bucket_dir": bucket_dir,
            "batch_file": batch_file,
            "wallets": wallets_set,
            "forced_stop": False,
        }
        node["busy_batch"] = batch_id
        logger.log(
            f"[worker:start] node={node_index} batch={batch_id} bucket={bucket} wallets={len(seeds)}",
            source="QUEUE",
        )
        update_active_file(force=True)
        return True

    next_health_check = time.monotonic() + PROXY_HEALTH_CHECK_INTERVAL_SECONDS
    exit_code = 0
    try:
        while True:
            sync_completed()

            # Collect finished/aborted workers and requeue only wallets that have no durable memory row.
            for node_index, state in list(worker_states.items()):
                proc = state["proc"]
                return_code = proc.poll()
                if return_code is None:
                    continue
                queue.unregister_pid(proc.pid)
                completed = load_test_memory(state["bucket_dir"] / TEST_MEMORY_FILE_NAME) & state["wallets"]
                error = (
                    "worker completed"
                    if return_code == 0
                    else f"worker exit={return_code} forced_stop={state.get('forced_stop', False)}"
                )
                done_count, requeued = queue.finish_batch(state["wallets"], completed, error)
                logger.log(
                    f"[worker:finish] node={node_index} batch={state['batch_id']} "
                    f"exit={return_code} done={done_count} requeued={requeued}",
                    source="QUEUE",
                    force_error=(return_code != 0),
                )
                try:
                    state["batch_file"].unlink(missing_ok=True)
                except Exception:
                    pass
                nodes[node_index]["busy_batch"] = None
                worker_states.pop(node_index, None)
                if return_code == 75:
                    mark_node_dead(node_index, "worker reported repeated proxy failures")
                update_active_file(force=True)

            counts = queue.counts()
            if counts["total"] and counts["done"] + counts["failed"] >= counts["total"] and not worker_states:
                break

            now = time.monotonic()
            if now >= next_health_check:
                healthy_indexes = [i for i, node in enumerate(nodes) if node.get("healthy")]
                if healthy_indexes:
                    with ThreadPoolExecutor(max_workers=min(len(healthy_indexes), 16)) as executor:
                        futures = {}
                        for index in healthy_indexes:
                            node = nodes[index]
                            futures[executor.submit(
                                proxy_text_request,
                                str(node["proxy"]),
                                VLESS_IP_CHECK_URL,
                                PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
                            )] = index
                        for future in as_completed(futures):
                            index = futures[future]
                            node = nodes[index]
                            try:
                                outbound_ip = future.result()
                                duplicate = any(
                                    other_index != index
                                    and other.get("healthy")
                                    and other.get("ip") == outbound_ip
                                    for other_index, other in enumerate(nodes)
                                )
                                if duplicate and VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS:
                                    raise RuntimeError(f"duplicate outbound IP {outbound_ip}")
                                node["ip"] = outbound_ip
                                node["health_failures"] = 0
                                node["last_error"] = ""
                            except Exception as exc:
                                node["health_failures"] = int(node.get("health_failures", 0)) + 1
                                node["last_error"] = repr(exc)
                                logger.log(
                                    f"[proxy:health-fail] node={index} "
                                    f"count={node['health_failures']}/{PROXY_HEALTH_FAILURE_THRESHOLD} error={exc!r}",
                                    source="PROXY",
                                    force_error=True,
                                )
                                if node["health_failures"] >= PROXY_HEALTH_FAILURE_THRESHOLD:
                                    mark_node_dead(index, repr(exc))
                next_health_check = now + PROXY_HEALTH_CHECK_INTERVAL_SECONDS

            # Startup-failed and later-dead nodes are always retried, even if they never worked once.
            due = [
                index
                for index, node in enumerate(nodes)
                if not node.get("healthy") and now >= float(node.get("next_recheck", 0.0))
            ]
            if PROXY_DEAD_RECHECK_ENABLED and due:
                with ThreadPoolExecutor(max_workers=min(len(due), 8)) as executor:
                    futures = {executor.submit(start_or_restart_node, nodes[index]): index for index in due}
                    for future in as_completed(futures):
                        index = futures[future]
                        ok, error = future.result()
                        if ok:
                            logger.log(
                                f"[proxy:recovered] node={index} ip={nodes[index]['ip']}",
                                source="PROXY",
                            )
                        else:
                            logger.log(
                                f"[proxy:still-dead] node={index} next={PROXY_DEAD_RECHECK_INTERVAL_SECONDS}s error={error}",
                                source="PROXY",
                                force_error=True,
                            )
                update_active_file(force=True)

            # Every idle healthy VPN claims the next available batch from the one global queue.
            for node_index, node in enumerate(nodes):
                if not node.get("healthy") or node_index in worker_states:
                    continue
                launch_batch(node_index)

            show_dashboard()
            show_error_notice()
            time.sleep(0.5)

        sync_completed()
        counts = queue.counts()
        exit_code = 0 if counts["failed"] == 0 else 1
        logger.log(f"[queue:summary] {counts}", source="QUEUE", force_error=bool(counts["failed"]))
        return exit_code

    except KeyboardInterrupt:
        logger.log("KeyboardInterrupt; workers are returned to pending on next launch", source="SYSTEM")
        return 130
    except Exception:
        logger.log(traceback.format_exc(), source="FATAL", force_error=True)
        return 1
    finally:
        # Stop children. Any wallet without a durable scored/filtered memory row is recovered as
        # pending on the next launch by recover_interrupted().
        for node_index, state in list(worker_states.items()):
            proc = state.get("proc")
            queue.unregister_pid(getattr(proc, "pid", None))
            stop_process(proc)
        for node in nodes:
            proc = node.get("xray_proc")
            queue.unregister_pid(getattr(proc, "pid", None))
            stop_process(proc)
            handle = node.get("xray_handle")
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
        queue.recover_interrupted()
        sync_completed()
        update_active_file(force=True)
        show_dashboard(force=True)
        show_error_notice(force=True)
        if GLOBAL_QUEUE_FINAL_MERGE_ON_EXIT:
            try:
                _merge_global_outputs(root, all_cache_dirs(), universe_file)
            except Exception:
                logger.log(traceback.format_exc(), source="MERGE", force_error=True)
        queue.close()
        if CLEAN_CONSOLE_DASHBOARD:
            sys.stdout = console_stdout
            sys.stderr = console_stderr
        logger.log("Global queue run finished", source="SYSTEM")
        logger.close()

def run_ranker_worker(args: argparse.Namespace) -> int:
    global NOT_SAVED_XLSX_CHECKPOINT_EVERY
    if args.skip_final_xlsx:
        NOT_SAVED_XLSX_CHECKPOINT_EVERY = 0
    mode = choose_mode(args)

    out_dir = Path(setting(args.out_dir, OUT_DIR))
    ensure_dir(out_dir)

    delay = setting(args.delay, HTTP_DELAY)
    timeout = setting(args.timeout, HTTP_TIMEOUT)
    retries = setting(args.retries, HTTP_RETRIES)
    max_offset = setting(args.max_offset, MAX_LEADERBOARD_OFFSET)
    max_wallets = setting(args.max_wallets, MAX_WALLETS_TO_SCORE)
    min_positions = setting(args.min_positions, MIN_RESOLVED_POSITIONS)
    min_losses = setting(args.min_losses, MIN_LOSING_POSITIONS)
    min_pnl = setting(args.min_pnl, MIN_CLOSED_REALIZED_PNL)
    smoothing = setting(args.smoothing, SMOOTHING)
    max_positions_per_wallet = setting(args.max_positions_per_wallet, MAX_POSITIONS_PER_WALLET)

    print_active_settings(
        mode=mode,
        out_dir=out_dir,
        delay=delay,
        timeout=timeout,
        retries=retries,
        max_offset=max_offset,
        max_wallets=max_wallets,
        min_positions=min_positions,
        min_losses=min_losses,
        min_pnl=min_pnl,
        max_positions_per_wallet=max_positions_per_wallet,
    )
    print(
        f"[worker network] proxy={args.proxy or 'direct'} "
        f"shard={args.shard_index}/{args.shard_count} "
        f"fallback={args.fallback_out_dir or 'none'}",
        flush=True,
    )
    client = PolymarketClient(
        delay=delay,
        timeout=timeout,
        retries=retries,
        proxy_url=args.proxy,
    )

    universe_path = (
        Path(args.wallet_universe_file)
        if args.wallet_universe_file
        else out_dir / "wallet_universe.csv"
    )
    one_share_input_path = out_dir / ONE_SHARE_RANKING_INPUT_FILE_NAME
    preserve_wallet_order = False
    if mode == 2:
        if USE_ONE_SHARE_RANKING_INPUT:
            if not one_share_input_path.exists():
                print(
                    f"Missing {one_share_input_path}. Rename edge_scores_by_oneShareNetPnlAfterCosts.xlsx "
                    f"to {ONE_SHARE_RANKING_INPUT_FILE_NAME} or set USE_ONE_SHARE_RANKING_INPUT = False.",
                    file=sys.stderr,
                )
                return 2
            wallets = load_wallets_from_score_xlsx(one_share_input_path)
            preserve_wallet_order = True
        else:
            if not universe_path.exists():
                print(f"Missing {universe_path}. Run mode 1 first.", file=sys.stderr)
                return 2
            wallets = load_wallet_universe(universe_path)
    else:
        wallets = collect_leaderboard_universe(
            client=client,
            out_dir=out_dir,
            max_offset=max_offset,
            limit=LEADERBOARD_LIMIT,
        )

    print(f"[universe] unique wallets: {len(wallets)}", flush=True)
    if mode == 1:
        return 0

    try:
        rank_wallets(
            client=client,
            wallets=wallets,
            out_dir=out_dir,
            min_positions=min_positions,
            min_losses=min_losses,
            min_pnl=min_pnl,
            smoothing=smoothing,
            max_wallets=max_wallets,
            max_positions_per_wallet=max_positions_per_wallet,
            preserve_wallet_order=preserve_wallet_order,
            shard_count=args.shard_count,
            shard_index=args.shard_index,
            fallback_out_dir=(Path(args.fallback_out_dir) if args.fallback_out_dir else None),
            skip_final_xlsx=args.skip_final_xlsx,
        )
    except WorkerProxyFailure as exc:
        print(f"[worker:proxy-failed] {exc}", file=sys.stderr, flush=True)
        return 75
    except WorkerRetryRequired as exc:
        print(f"[worker:retry-required] {exc}", file=sys.stderr, flush=True)
        return 76
    if args.skip_final_xlsx:
        print(f"[done] shard CSV: {out_dir / 'edge_scores_progress.csv'}", flush=True)
    else:
        print(f"[done] results: {out_dir / 'edge_scores.xlsx'}", flush=True)
    return 0


def main() -> int:
    args = build_parser().parse_args()
    root = Path(args.vless_root or VLESS_OUTPUT_ROOT)
    fallback = Path(args.fallback_out_dir or VLESS_FALLBACK_OUT_DIR)

    if args.merge_only:
        merge_vless_outputs(root, fallback_out_dir=fallback)
        return 0

    if args.worker or args.no_vless or RUN_MODE == 1:
        return run_ranker_worker(args)

    if USE_VLESS_MULTI:
        if GLOBAL_QUEUE_ENABLED:
            return run_global_queue_manager(args)
        return run_vless_manager(args)

    print(
        "[proxy] USE_VLESS_MULTI=False; running one direct worker.",
        flush=True,
    )
    return run_ranker_worker(args)


if __name__ == "__main__":
    raise SystemExit(main())
