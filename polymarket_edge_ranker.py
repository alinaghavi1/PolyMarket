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
import errno
import hashlib
import json
import math
import os
import re
import signal
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Callable, Iterator


def _configure_utf8_stdio() -> None:
    """Prevent Windows console code pages from crashing workers on Unicode market text."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError, OSError):
            pass


_configure_utf8_stdio()

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
#   wallet_test_memory.csv     حافظه دائمی Resume و گزارش پوشش هر والت
#   closed_positions_raw.jsonl دیتای خام کامل هر والت؛ برای فرمول‌های بعدی نگهش دار
#   closed_positions_pages.jsonl حافظه قدیمی صفحه‌ای
#   polymarket_complete_fetch_cache.sqlite3 حافظه دقیق market/activity؛ اگر وسط والت بزرگ قطع شد ادامه می‌دهد
#   closed_positions_raw2.jsonl دیتای آفلاین دوم؛ فقط وقتی fallback روشن باشد استفاده می‌شود
#   closed_positions_pages2.jsonl cache صفحه‌ای دوم؛ فقط برای داده‌های گمشده استفاده می‌شود
#   edge_scores_progress.csv   خروجی زنده مرتب؛ Manager حین اجرا checkpoint می‌زند
#   edge_scores.xlsx           خروجی اصلی مرتب؛ حین اجرا دوره‌ای و در پایان کامل نوشته می‌شود
#   wallet_not_saved_reasons.xlsx دلیل ذخیره نشدن والت‌ها در خروجی آماری
#   wallet_not_saved_reason_stats.xlsx آمار تعداد تکرار هر دلیل حذف
# =============================================================================

# آدرس API عمومی پلی‌مارکت. معمولاً لازم نیست تغییرش بدهی.
BASE_URL = "https://data-api.polymarket.com"

# مود اجرا:
#   1 = استخراج والت‌ها از همه حالت‌های لیدربورد
#   2 = تست کردن والت‌های استخراج‌شده و محاسبه Edge Rally × Net Edge Score
RUN_MODE = 2

# شناسه نسخه برای اینکه معلوم باشد دقیقاً همین فایل جدید اجرا شده است.
BUILD_ID = "global-queue-v58-reconciled-ledger-verification"
# پوشه خروجی. برای اینکه مود 2 بتواند خروجی مود 1 را بخواند، بین دو مود تغییرش نده.
OUT_DIR = "polymarket_edge_output"

# اسم فایل حافظه مود 2 و مرجع قطعی Resume.
# هر تلاش همراه آمار دقیق اینجا ثبت می‌شود، اما فقط ردیف v58 با status=scored،
# coverageStatus=verified، تطبیق دقیق چندمجموعهٔ ردیف‌های /trades و /activity،
# صفر market/outcome گمشده، pagination کامل هر دو endpoint و پاسخ زنده /traded
# برای Resume قطعی Done است. ردیف incomplete در اجرای بعد Retry می‌شود. حذف
# همین فایل در پوشه اصلی خروجی یک شروع تازه ایجاد می‌کند.
TEST_MEMORY_FILE_NAME = "wallet_test_memory.csv"

# Workerهای صف جهانی در فایل نسخه‌بندی‌شده خودشان می‌نویسند تا فایل‌های باز یا
# schema قدیمی اجرای قبل روی ویندوز باعث WinError 5 نشوند. Manager این فایل‌ها را
# به‌صورت زنده داخل TEST_MEMORY_FILE_NAME ادغام می‌کند.
WORKER_TEST_MEMORY_FILE_NAME = "wallet_test_memory_v58.csv"
LEGACY_WORKER_TEST_MEMORY_FILE_NAMES = (
    "wallet_test_memory_v57.csv",
    "wallet_test_memory_v56.csv",
    "wallet_test_memory_v55.csv",
    "wallet_test_memory_v54.csv",
    "wallet_test_memory_v53.csv",
)
TEST_MEMORY_STATE_FILE_NAME = "wallet_test_memory_state.json"
TEST_MEMORY_SCHEMA_VERSION = 8
TRADE_SET_VERIFICATION_VERSION = "reconciled-ledger-multiset-snapshot-v5"
TEST_MEMORY_SYNC_SECONDS = 5.0
TEST_MEMORY_FIELDNAMES = [
    "proxyWallet",
    "userName",
    "status",
    "reason",
    "snapshotStart",
    "snapshotEnd",
    "tradesRawRows",
    "activityRawRows",
    "logicalTradeRows",
    "matchedCoreRows",
    "activityOnlyRows",
    "tradesOnlyRows",
    "exactRepeatedRows",
    "valueDifferenceRows",
    "sideDifferenceRows",
    "onchainVerifiedRows",
    "verifiedTradeRows",
    "unresolvedTradeRows",
    "tradeVerificationStatus",
    "verificationReason",
    "downloadedPositions",
    "downloadedMarkets",
    "downloadedTradeRows",
    "uniqueTradeRows",
    "duplicateTradeRows",
    "activityTradeRows",
    "activityUniqueTradeRows",
    "matchedTradeRows",
    "missingTradeRows",
    "extraTradeRows",
    "tradeRowCoveragePercent",
    "tradeRowVerificationStatus",
    "discoveredTradeOutcomes",
    "apiMatchedTradeOutcomes",
    "apiMissingTradeOutcomes",
    "apiOutcomeCoveragePercent",
    "matchedTradeOutcomes",
    "missingTradeOutcomes",
    "extraDownloadedOutcomes",
    "outcomeCoveragePercent",
    "discoveredTradeMarkets",
    "matchedTradeMarkets",
    "missingTradeMarkets",
    "extraDownloadedMarkets",
    "polymarketTraded",
    "marketCoveragePercent",
    "positionCoveragePercent",
    "tradePaginationComplete",
    "activityPaginationComplete",
    "coverageStatus",
    "missingOutcomeSample",
    "missingMarketSample",
    "verificationVersion",
    "officialTradedSource",
    "officialTradedLive",
    "fetchComplete",
    "testedAt",
]

# برای اینکه عدد Polymarket واقعاً همان لحظه از endpoint رسمی گرفته شده باشد،
# والت بدون پاسخ زنده /traded Done نمی‌شود و با VPN دیگری Retry خواهد شد.
REQUIRE_LIVE_OFFICIAL_TRADED_FOR_MEMORY = True

# False = حافظه بالا دائمی است و بازکردن دوباره برنامه هیچ چرخه خودکاری از صفر
# نمی‌سازد. شروع تازه فقط با حذف wallet_test_memory.csv انجام می‌شود.
REFRESH_EXISTING_WALLETS_ON_EACH_RUN = False

# اسم فایل گزارش والت‌هایی که تحلیل شدند ولی وارد خروجی آماری نشدند.
NOT_SAVED_REASONS_FILE_NAME = "wallet_not_saved_reasons.xlsx"

# اسم فایل آمار تعداد تکرار دلیل‌های ذخیره نشدن والت‌ها.
NOT_SAVED_REASON_STATS_FILE_NAME = "wallet_not_saved_reason_stats.xlsx"

# اسم فایل دیتای خام کامل.
# هر وقت یک والت کامل گرفته شد، closed-positions و همه ردیف‌های current positions
# آن (با برچسب open/resolved و حذف overlap) اینجا ذخیره می‌شود.
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

# Freeze both trade sources to one wallet-local boundary.  Recent indexed data is
# intentionally deferred to the next run instead of racing two independently
# updated endpoints.
SNAPSHOT_FINALITY_LAG_SECONDS = max(
    0, int(os.environ.get("POLYMARKET_SNAPSHOT_FINALITY_LAG_SECONDS", "30"))
)

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

# حالت کامل: هیچ والت سالمی به‌خاطر معیارهای آماری از خروجی حذف نمی‌شود.
# این Master switch علاوه بر فلگ‌های خاموش زیر، جلوی فعال‌شدن تصادفی فیلترها
# از طریق آرگومان‌های خط فرمان را هم می‌گیرد.
FULL_WALLET_INCLUSION_MODE = True

# اگر روشن باشد فقط ابتدای wallet_universe.csv پردازش می‌شود. برای حالت کامل
# خاموش است و MAX_WALLETS_TO_SCORE=None یعنی تمام والت‌ها.
FILTER_MAX_WALLETS_TO_SCORE = False
MAX_WALLETS_TO_SCORE = None

# حداکثر چند closed position برای هر والت گرفته شود.
# عدد کمتر = سریع‌تر ولی ممکن است دیتای والت‌های خیلی بزرگ کامل نباشد.
# عدد بیشتر = کامل‌تر ولی کندتر.
# در حالت کامل، LIMIT_POSITIONS_PER_WALLET=False باعث می‌شود مقدار محدودکنندهٔ
# خط فرمان هم نادیده گرفته شود و این عدد فقط یک سقف فنیِ عملاً دست‌نیافتنی باشد.
LIMIT_POSITIONS_PER_WALLET = False
MAX_POSITIONS_PER_WALLET = 100000000000000

# حداقل تعداد پوزیشن بسته‌شده/نتیجه‌دار برای اینکه والت وارد خروجی score شود.
# فیلتر خاموش و حداقل صفر است؛ والت دارای فقط پوزیشن باز یا حتی صفر ردیف هم
# با امتیازهای صفر در خروجی باقی می‌ماند.
FILTER_MIN_RESOLVED_POSITIONS = False
MIN_RESOLVED_POSITIONS = 0

# حداقل تعداد باخت لازم.
# این کمک می‌کند والت‌هایی که فقط چند برد و تقریباً بدون باخت دارند الکی امتیاز نگیرند.
FILTER_MIN_LOSING_POSITIONS = False
MIN_LOSING_POSITIONS = 0

# حداقل سود بسته‌شده.
# فیلتر خاموش است؛ مقدار صفر فقط برای نمایش تنظیمات نگه داشته شده و اعمال نمی‌شود.
FILTER_MIN_CLOSED_REALIZED_PNL = False
MIN_CLOSED_REALIZED_PNL = 0.0

# مخرج پیش‌فرض فرمول Edge Rally فقط وقتی هیچ ضرری وجود ندارد.
# وقتی ضرر وجود دارد، مخرج همان sumLossRiskSq است و smoothing اضافه نمی‌شود.
SMOOTHING = 1.0

# فیلتر منفی بودن موجودی اخیر همه معاملات؛ اگر روشن باشد والت‌هایی که مقدار فعلی همه معاملاتشان منفی است حذف می‌شوند.
FILTER_ALL_RECENT_BALANCES_NEGATIVE = False

# فیلتر Net Edge منفی؛ اگر روشن باشد والت‌هایی که نت اج منفی دارند حذف می‌شوند.
FILTER_NEGATIVE_NET_EDGE = False

# فیلتر سود یک‌سهمی غیرمثبت؛ اگر روشن باشد والت‌هایی که oneShareNetPnlAfterCosts آن‌ها منفی یا صفر است حذف می‌شوند.
FILTER_NON_POSITIVE_ONE_SHARE_NET_PNL_AFTER_COSTS = False

# فیلتر حداقل Recovery Factor؛ اگر روشن باشد والت‌هایی که کمتر از مقدار زیر باشند حذف می‌شوند.
FILTER_MIN_RECOVERY_FACTOR = False

# حداقل Recovery Factor قابل قبول وقتی فیلتر بالا روشن باشد.
MIN_RECOVERY_FACTOR = 5

# فیلتر فعالیت ۷ روز اخیر؛ اگر روشن باشد والت بدون معامله باز/بسته‌شده در ۷ روز اخیر حذف می‌شود.
FILTER_NO_RECENT_7D_OPEN_OR_CLOSE = False

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
PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE = False

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
# این اعداد فقط همزمانی دانلود را تغییر می‌دهند و هیچ صفحه، Offset یا پوزیشنی را حذف نمی‌کنند.
CLOSED_FETCH_WORKERS = 5
ACTIVITY_FETCH_WORKERS = 4
# حداکثر دو پنجره زمانی Activity همزمان اسکن می‌شوند. درخواست‌های Offset هر دو
# پنجره از یک Pool مشترک استفاده می‌کنند تا تعداد Threadها بی‌رویه چندبرابر نشود.
ACTIVITY_WINDOW_WORKERS = 2

# والت‌های Heavy در انتهای صف هم CPU بسیار بیشتری می‌خواهند و هم به تعداد زیادی
# Activity Window نیاز دارند. به‌جای اجرای ده‌ها Heavy با همزمانی داخلی کامل،
# هر Heavy با Pool کوچک‌تر اجرا می‌شود. این تنظیم فقط concurrency را کم می‌کند؛
# هیچ صفحه، Offset، Market یا Position حذف نمی‌شود.
HEAVY_CLOSED_FETCH_WORKERS = 3
HEAVY_ACTIVITY_FETCH_WORKERS = 2
HEAVY_ACTIVITY_WINDOW_WORKERS = 1

# Tail controller فقط وقتی فعال می‌شود که هیچ Fresh/Retry معمولی نمانده باشد و
# تعداد کل والت‌های حل‌نشده از این حد کمتر باشد. تعداد VPNهای فعال همچنان با
# CPU Auto Tune قبلی مدیریت می‌شود، اما تعداد Workerهای Heavy جداگانه محدود می‌شود.
HEAVY_TAIL_TRIGGER_REMAINING = 256
HEAVY_TAIL_INITIAL_WORKERS = 24
HEAVY_TAIL_MIN_WORKERS = 12
HEAVY_TAIL_MAX_WORKERS = 30
HEAVY_TAIL_CPU_LOW_PERCENT = 55.0
HEAVY_TAIL_CPU_HIGH_PERCENT = 85.0
HEAVY_TAIL_SCALE_UP_STEP = 2
HEAVY_TAIL_SCALE_DOWN_STEP = 4
HEAVY_TAIL_IMMEDIATE_STOP_MAX = 4

# In the final tail, two ready wallets can hash to a bucket already owned by a
# long-running wallet. v47 serialized the whole bucket, leaving those wallets
# pending even with many idle VPNs. v48 gives only the blocked wallet a stable
# wallet-specific output directory and reads its previous bucket as a read-only
# fallback. No checkpoint is discarded and no two workers append to the same
# files. The spillover directory is persisted in the queue DB across restarts.
HEAVY_TAIL_BUCKET_SPILLOVER_ENABLED = True
HEAVY_TAIL_BUCKET_SPILLOVER_ONLY_WHEN_BLOCKED = True
HEAVY_TAIL_BUCKET_SPILLOVER_PREFIX = "spill"

_WORKER_LANE_ENV = "POLYMARKET_QUEUE_LANE"
_WORKER_CLOSED_FETCH_ENV = "POLYMARKET_CLOSED_FETCH_WORKERS"
_WORKER_ACTIVITY_FETCH_ENV = "POLYMARKET_ACTIVITY_FETCH_WORKERS"
_WORKER_ACTIVITY_WINDOW_ENV = "POLYMARKET_ACTIVITY_WINDOW_WORKERS"


def _env_positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return max(1, int(default))
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return max(1, int(default))


def _effective_closed_fetch_workers() -> int:
    return _env_positive_int(_WORKER_CLOSED_FETCH_ENV, CLOSED_FETCH_WORKERS)


def _effective_activity_fetch_workers() -> int:
    return _env_positive_int(_WORKER_ACTIVITY_FETCH_ENV, ACTIVITY_FETCH_WORKERS)


def _effective_activity_window_workers() -> int:
    return _env_positive_int(_WORKER_ACTIVITY_WINDOW_ENV, ACTIVITY_WINDOW_WORKERS)
# بعد از هر پنج پنجره کامل، بازارهای پیدا‌شده و پنجره‌های باقی‌مانده در SQLite
# ثبت می‌شوند. در خاموشی/تعویض VPN، اسکن از همین Checkpoint ادامه پیدا می‌کند.
ACTIVITY_CHECKPOINT_EVERY_WINDOWS = 5
# هر پنجره Activity که خطای موقت می‌دهد داخل همان Worker دوباره امتحان می‌شود.
# در صورت شکست نهایی، خود پنجره و تمام پنجره‌های درحال‌اجرا قبل از خروج Checkpoint می‌شوند.
ACTIVITY_WINDOW_LOCAL_RETRIES = 3

# Market-batchها به‌صورت bounded اجرا می‌شوند؛ نه اینکه هزاران Future یک‌جا ساخته شود.
# هر batch موفق همان لحظه در SQLite ثبت می‌شود. شکست یک batch دیگر باعث دور ریختن
# نتایج موفق نمی‌شود. batch ناموفق پس از Retry به دو نیم تقسیم می‌شود تا market
# مشکل‌دار دقیقاً جدا شود، بدون حذف هیچ market یا position.
MARKET_BATCH_MAX_INFLIGHT_MULTIPLIER = 2
MARKET_BATCH_MULTI_LOCAL_RETRIES = 1
MARKET_BATCH_SINGLE_LOCAL_RETRIES = 4
MARKET_BATCH_RETRY_BASE_SECONDS = 1.5
MARKET_BATCH_RETRY_MAX_SECONDS = 12.0
MARKET_BATCH_PROGRESS_EVERY = 25
MARKET_BATCH_PROXY_ABORT_FAILURE_STREAK = 12

# مقدار /traded فقط برای انتخاب مسیر سریع یا کامل استفاده می‌شود. Cache کردن آن
# هیچ اثری روی completeness ندارد؛ Activity و verification نهایی همچنان کامل اجرا می‌شوند.
OFFICIAL_TRADED_CACHE_TTL_SECONDS = 21600

# حاشیه امن زیر rate limit رسمی Data API.
CLOSED_RATE_LIMIT_CALLS = 135
CLOSED_RATE_LIMIT_PERIOD_SECONDS = 10.0
ACTIVITY_RATE_LIMIT_CALLS = 800
ACTIVITY_RATE_LIMIT_PERIOD_SECONDS = 10.0

# پارامترهای رسمی Activity.
ACTIVITY_PAGE_LIMIT = 500
ACTIVITY_MAX_OFFSET = 5000

# /trades تنها endpoint استانداردی است که شناسهٔ دقیق همه marketهای معامله‌شده
# را می‌دهد. takerOnly باید False باشد تا maker و taker هر دو شمرده شوند. برای
# والت‌های بسیار بزرگ، بازهٔ زمانی به‌صورت تطبیقی شکسته می‌شود تا سقف offset
# ده‌هزار هیچ معامله‌ای را حذف نکند.
TRADE_PAGE_LIMIT = 10000
TRADE_MAX_OFFSET = 10000
TRADE_DISCOVERY_VERSION = "trades-maker-taker-reconciled-core-multiset-v4"

# Combo position endpoint cursor/offset pagination.
COMBO_POSITION_PAGE_LIMIT = 1000
COMBO_POSITION_MAX_OFFSET = 100000

# برای market استانداردی که در /trades هست ولی closed/current آن را پس نمی‌دهند،
# کل Activity همان market خوانده و BUY/SELL/REDEEM به یک پوزیشن بستهٔ قابل امتیاز
# تبدیل می‌شود. اگر حسابداری دقیقاً balance نشود، والت Verified نمی‌شود.
ACTIVITY_RECONSTRUCTION_QUANTITY_TOLERANCE = 1e-5
ACTIVITY_RECONSTRUCTION_VALUE_TOLERANCE = 1e-6

# پارامترهای Current Positions. این endpoint علاوه بر پوزیشن باز، پوزیشن‌های
# نتیجه‌دار ولی redeemنشده را هم می‌دهد؛ بنابراین برای دیتای کامل اجباری است.
# هر صفحه موفق جداگانه در SQLite ثبت می‌شود و Retry همان چرخه از همان offset ادامه می‌دهد.
CURRENT_POSITION_PAGE_LIMIT = 500
CURRENT_POSITION_MAX_OFFSET = 10000
CURRENT_POSITION_PAGE_LOCAL_RETRIES = 3
CURRENT_POSITION_PAGE_RETRY_BASE_SECONDS = 1.5
CURRENT_POSITION_PAGE_RETRY_MAX_SECONDS = 10.0
CURRENT_POSITION_COMPLETE_CACHE_TTL_SECONDS = 3600
CURRENT_POSITION_LOG_ERROR_MAX_CHARS = 1200
# برای پوشش والت‌هایی که هزاران پوزیشن نتیجه‌دار ولی redeemنشده دارند، دریافت
# کامل /positions دیگر اختیاری نیست. اگر این endpoint ناقص بماند، والت Done نمی‌شود
# و با VPN دیگری Retry می‌شود؛ دیتای ناقص به‌عنوان کامل ثبت نخواهد شد.
CURRENT_POSITION_OPTIONAL_FOR_CLOSED_COMPLETENESS = False

# marketهایی که در Activity هستند ولی در Closed/Current دیده نمی‌شوند یک بار
# تازه‌سازی می‌شوند. باقی‌ماندن آن‌ها فقط هشدار است، چون Activity الزاماً برای هر
# TRADE یک ردیف Current یا Closed متناظر ایجاد نمی‌کند.
COVERAGE_REPAIR_PASSES = 1

# کش SQLite برای ادامه دادن والت‌های بسیار بزرگ بعد از توقف برنامه.
COMPLETE_FETCH_CACHE_DB_FILE_NAME = "polymarket_complete_fetch_cache.sqlite3"

# کش‌های قدیمی closed_positions_raw.jsonl که metadata کامل بودن ندارند، ممکن است
# همان داده 37 هزار تایی یا داده تکراری باشند؛ برای اولویت صحت به آن‌ها اعتماد نکن.
TRUST_LEGACY_RAW_CLOSED_POSITION_CACHE = False

COMPLETE_FETCH_VERSION = "complete-market-v8-exact-trade-multiset-outcomes"
# شناسه قدیمی چرخه برای سازگاری دیتابیس حفظ شده است؛ در v53 چرخه خودکار خاموش
# است و فقط حذف wallet_test_memory.csv یک reset epoch تازه می‌سازد.
REFRESH_CYCLE_VERSION = (
    COMPLETE_FETCH_VERSION + "|full-inclusion-v2|exact-trade-multiset-score-v4"
)

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

# این سه تنظیم داخل خود کد هستند.
# برای تغییرشان برنامه را با Ctrl+C ببند، عددها را عوض کن و دوباره اجرا کن.
# صف جهانی، والت‌های کامل‌شده و Cacheها از همان‌جا ادامه پیدا می‌کنند.
VPN_STARTUP_TEST_WORKERS = 15

# تعداد VPNهایی که برنامه در شروع تلاش می‌کند فعال کند.
# اگر تعداد VPNهای سالم کمتر از این عدد باشد، برنامه خودکار همان تعداد واقعی را استفاده می‌کند.
VPN_MAX_ACTIVE_NODES = 60
# هر بار Auto Tune زیر هدف CPU باشد، چند VPN به ظرفیت کاری اضافه شود.
# مثال: 5 یعنی هر 30 ثانیه پنج‌تا پنج‌تا بالا برود تا CPU به 70 درصد برسد
# یا همه VPNهای سالم موجود فعال شوند.
VPN_AUTO_TUNE_UP_STEP = 5
# True  = VPNهای مرده دوره‌ای Restart و دوباره تست می‌شوند.
# False = VPN مرده دیگر بررسی نمی‌شود و برنامه فقط از VPNهای سالم استفاده می‌کند.
# مقدار پیش‌فرض False است.
VPN_DEAD_RECHECK_ENABLED = False

# تعداد Workerهای هم‌زمان برای بررسی مجدد VPNهای مرده؛ فقط وقتی گزینه بالا True باشد.
VPN_DEAD_RECHECK_WORKERS = 21

# رتبه‌بندی دائمی VPNهایی که حداقل یک‌بار سالم بوده‌اند.
# این چرخه کاملاً از Workerهای والت جداست: VPNهای خاموش را موقتاً روشن و تست می‌کند،
# بعد از پایان تست همه آن‌ها مرتب‌سازی را یک‌جا اعمال می‌کند و فقط در صورت تغییر
# بهترین‌ها، Pool کاری را جابه‌جا می‌کند. VPNهایی که از ابتدا هیچ‌وقت سالم نبوده‌اند
# وارد این چرخه نمی‌شوند تا مرتب خطای dead تولید نکنند.
VPN_CONTINUOUS_SORT_ENABLED = False

# تعداد VPNهایی که در چرخه رتبه‌بندی پس‌زمینه هم‌زمان تست می‌شوند.
VPN_CONTINUOUS_SORT_WORKERS = 20

# مکث بعد از پایان یک دور کامل و پیش از شروع دور بعدی، بر حسب ثانیه.
VPN_CONTINUOUS_SORT_PAUSE_SECONDS = 0.0

# True = تعداد VPNهای کاری بر اساس میانگین CPU خودکار کم یا زیاد می‌شود.
CPU_AUTO_TUNE_ENABLED = True
# مرز بالایی CPU: وقتی میانگین 30 ثانیه‌ای به 85 درصد برسد یا بالاتر برود،
# ظرفیت هر بار یک VPN کم می‌شود.
CPU_AUTO_TUNE_HIGH_PERCENT = 85.0
# مرز پایینی CPU: وقتی میانگین کمتر از 70 درصد باشد، ظرفیت با گام
# VPN_AUTO_TUNE_UP_STEP افزایش پیدا می‌کند. بین 70 تا کمتر از 85 ثابت می‌ماند.
CPU_AUTO_TUNE_LOW_PERCENT = 70.0
# مدت بازه میانگین‌گیری بر حسب ثانیه. مثال: 30 یعنی میانگین 30 ثانیه بررسی شود.
CPU_AUTO_TUNE_WINDOW_SECONDS = 30.0

# فاصله نمونه‌برداری CPU بر حسب ثانیه. معمولاً روی 1 بماند.
CPU_AUTO_TUNE_SAMPLE_INTERVAL_SECONDS = 1.0

# محافظ RAM کل سیستم. هر ثانیه یک نمونه می‌گیرد و فقط بعد از جمع‌شدن
# یک پنجره کامل 60 ثانیه‌ای تصمیم می‌گیرد. اگر میانگین این 60 نمونه
# بیشتر از 90 درصد شود، برنامه با توقف امن بسته می‌شود؛ Workerهای ناتمام
# در اجرای بعدی از صف Pending ادامه پیدا می‌کنند.
RAM_SAFETY_STOP_ENABLED = True
RAM_SAFETY_STOP_PERCENT = 90.0
RAM_SAFETY_WINDOW_SECONDS = 60.0
RAM_SAFETY_SAMPLE_INTERVAL_SECONDS = 1.0
RAM_SAFETY_EXIT_CODE = 90

# هنگام کاهش ظرفیت، یک Worker اضافی همان لحظه متوقف می‌شود و والت ناتمام
# بدون از دست رفتن حافظه‌های ذخیره‌شده دوباره به صف جهانی برمی‌گردد.
CPU_AUTO_TUNE_IMMEDIATE_SCALE_DOWN = True

# پیشرفت تست اولیه بعد از چند نتیجه در CMD تازه شود. 1 یعنی از 1/کل نمایش داده می‌شود.
VPN_STARTUP_PROGRESS_EVERY = 1

# رتبه‌بندی VPNها با همان APIهایی که برای تحلیل واقعی پوزیشن‌های والت استفاده می‌شوند.
# هر دور تست از مسیر همان VPN این سه endpoint را می‌خواند:
#   /closed-positions  پوزیشن‌های بسته
#   /positions         پوزیشن‌های باز/فعلی
#   /activity          معاملات و marketهای لازم برای دریافت کامل
# فقط وقتی هر سه پاسخ JSON معتبر بدهند، آن دور موفق حساب می‌شود.
VPN_SPEED_RANKING_ENABLED = True

# چند والت واقعی از ابتدای wallet_universe.csv برای تست مسیر پوزیشن استفاده شود.
# در هر تلاش، یک والت نمونه متفاوت به‌صورت چرخشی انتخاب می‌شود.
VPN_POSITION_TEST_SAMPLE_WALLETS = 2

# تعداد دورهای کامل تست برای Startup، Sort و Dead Recheck.
# هر دور شامل هر سه endpoint بالاست؛ حداقل یک دور کامل موفق برای سالم بودن لازم است.
VPN_SPEED_TEST_ATTEMPTS = 2
VPN_SPEED_TEST_TIMEOUT_SECONDS = 12.0

# حداقل چند دور کامل باید موفق باشد. مقدار 2 یعنی در تست کامل، هر دو دور
# و در نتیجه هر سه endpoint روی هر دو والت نمونه باید بدون خطا جواب بدهند.
VPN_POSITION_TEST_MIN_SUCCESSFUL_ATTEMPTS = 2

# Health Check و Promotion فقط یک دور کامل از همان تست پوزیشن می‌زنند.
VPN_POSITION_QUICK_TEST_ATTEMPTS = 1

# برای هر دور کامل ناموفق این مقدار به امتیاز اضافه می‌شود؛
# بنابراین VPN سریع ولی ناپایدار پایین‌تر از VPN کمی کندتر و پایدار قرار می‌گیرد.
VPN_SPEED_FAILURE_PENALTY_MS = 5000.0

# پس از پایان همه shardها خروجی آماری نهایی به‌صورت خودکار ادغام می‌شود.
VLESS_AUTO_MERGE_OUTPUTS = True

# فایل‌های raw بسیار بزرگ داخل shardها باقی می‌مانند و برای جلوگیری از مصرف دوباره دیسک
# به merged کپی نمی‌شوند. scoreها، memory و خطاها ادغام می‌شوند.
VLESS_MERGE_RAW_JSONL = False

# اگر یک نود وسط اجرا از کار بیفتد، Worker آن متوقف می‌شود و همان shard با cache قبلی
# به یکی از نودهای سالم و بیکار سپرده می‌شود. نود سالم ابتدا shard خودش را تمام می‌کند
# و بعد shard نیمه‌تمام را ادامه می‌دهد تا دو Worker هم‌زمان از یک IP استفاده نکنند.
PROXY_FAILOVER_ENABLED = True

# هر چند ثانیه فقط زنده‌بودن Process محلی Xray نود فعال بررسی شود.
# برای VPN فعال هیچ درخواست آزمایشی اضافه‌ای به API زده نمی‌شود؛ درخواست‌های واقعی
# Worker والت معیار سلامت شبکه‌اند و بعد از چند Fetch Failure خود Worker خارج می‌شود.
PROXY_HEALTH_CHECK_INTERVAL_SECONDS = 15.0
# این Timeout همچنان در Startup، Promotion، Sort و Dead Recheck استفاده می‌شود.
PROXY_HEALTH_CHECK_TIMEOUT_SECONDS = 10.0

# این مقدار برای مسیرهای قدیمی/سازگاری نگه داشته شده است؛ در مسیر صف جهانی،
# خروج Xray فوراً نود را dead می‌کند و خطاهای واقعی Worker با تنظیم زیر کنترل می‌شوند.
PROXY_HEALTH_FAILURE_THRESHOLD = 4

# فاصله و روش بررسی مجدد VPN مرده؛ فقط وقتی VPN_DEAD_RECHECK_ENABLED=True باشد.
PROXY_DEAD_RECHECK_INTERVAL_SECONDS = 60.0
PROXY_DEAD_RESTART_BEFORE_CHECK = True

# حداکثر تعداد اجرای دوباره هر shard روی نودهای سالم دیگر.
PROXY_FAILOVER_MAX_ATTEMPTS_PER_SHARD = 8
PROXY_FAILOVER_RETRY_DELAY_SECONDS = 2.0

# خروجی CMD تمیز: جزئیات داخل فایل لاگ می‌روند و فقط Dashboard نشان داده می‌شود.
CLEAN_CONSOLE_DASHBOARD = True
# فاصله تازه‌سازی عادی داشبورد CMD. با تمام‌شدن هر والت، داشبورد فارغ از این عدد فوراً تازه می‌شود.
CONSOLE_STATUS_INTERVAL_SECONDS = 1.0
# If ANSI/VT cursor updates are unavailable, avoid flooding the terminal.
CONSOLE_FALLBACK_STATUS_INTERVAL_SECONDS = 10.0
CONSOLE_ERROR_NOTICE_INTERVAL_SECONDS = 60.0
ALL_LOG_FILE_NAME = "all_logs.txt"
ERROR_LOG_FILE_NAME = "errors.txt"

# لاگ تشخیصی کم‌حجم برای پیدا کردن گلوگاه واقعی سرعت.
# یک بلوک کامل در شروع، سپس هر چند دقیقه و هنگام خروج نوشته می‌شود.
DIAGNOSTIC_LOG_FILE_NAME = "diagnostics_summary.log"
# Copy/paste friendly proof of position completeness.  Unlike the technical
# diagnostics this contains one bounded line per checked wallet and no proxy data.
POSITION_COMPLETENESS_LOG_FILE_NAME = "position_completeness_summary.log"
DIAGNOSTIC_LOG_INTERVAL_SECONDS = 60.0
DIAGNOSTIC_STALL_SECONDS = 300.0
DIAGNOSTIC_OLDEST_WORKERS = 8
DIAGNOSTIC_ERROR_SAMPLE_LINES = 20
DIAGNOSTIC_RUNTIME_SAMPLE_LINES = 20
DIAGNOSTIC_MAX_INCREMENTAL_READ_BYTES = 16 * 1024 * 1024
DIAGNOSTIC_CRASH_TRACEBACK_LINES = 45

# Unexpected worker crashes are captured in a dedicated, non-spam file.
WORKER_CRASH_LOG_FILE_NAME = "worker_crashes.log"
WORKER_OUTPUT_TAIL_LINES = 160
# After the same wallets have already been claimed this many times, isolate them
# into one-wallet batches so one malformed wallet cannot repeatedly block seven others.
GLOBAL_QUEUE_ISOLATE_AFTER_ATTEMPTS = 2
# Any unexpected non-zero worker exit quarantines that VPN for the current run.
UNEXPECTED_WORKER_EXIT_QUARANTINE = True
# Exit 76 means the wallet fetch pass needs a retry through another VPN; it is not a crash.
WORKER_RETRY_REQUIRED_EXIT_CODE = 76
# Keep the same VPN idle briefly so the requeued wallet is picked by a different active node.
WORKER_RETRY_NODE_COOLDOWN_SECONDS = 45.0
# A wallet that requests failover is deferred instead of immediately blocking the head of the queue.
WORKER_RETRY_BACKOFF_BASE_SECONDS = 60.0
WORKER_RETRY_BACKOFF_MAX_SECONDS = 1800.0

# Retry wallets are isolated into one-wallet worker jobs. This does not change any
# API pagination or position limits; it only prevents one slow wallet from repeatedly
# dragging seven unrelated wallets back into the queue with it.
GLOBAL_QUEUE_RETRY_BATCH_SIZE = 1

# Wallets that repeatedly need failover move into a dedicated heavy lane. They are
# never discarded. A fixed share of runtime worker slots is reserved for this lane
# so heavy wallets cannot starve behind thousands of ordinary retries.
GLOBAL_QUEUE_HEAVY_AFTER_RETRIES = 5
GLOBAL_QUEUE_HEAVY_BATCH_SIZE = 1
GLOBAL_QUEUE_HEAVY_RETRY_BACKOFF_SECONDS = 20.0
GLOBAL_QUEUE_HEAVY_RETRY_JITTER_SECONDS = 15.0
GLOBAL_QUEUE_STARTUP_HEAVY_RELEASE_WINDOW_SECONDS = 15.0
# سهم Heavy تطبیقی است: از 25٪ شروع می‌شود، با بزرگ‌شدن صف تا 40٪ می‌رود
# و وقتی فقط Heavy باقی مانده باشد تمام Workerهای آزاد را می‌گیرد.
GLOBAL_QUEUE_HEAVY_WORKER_SHARE_MIN = 0.25
GLOBAL_QUEUE_HEAVY_WORKER_SHARE_MAX = 0.40
GLOBAL_QUEUE_HEAVY_HIGH_BACKLOG = 500
GLOBAL_QUEUE_HEAVY_MIN_WORKERS = 1

# Retry and heavy wallets are already known to be large or network-sensitive, so they
# receive longer activity-aware watchdog windows. A live worker is still recycled if
# it has both no durable completion and no output for the full thresholds below.
RETRY_WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS = 1200.0
RETRY_WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS = 600.0
HEAVY_WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS = 1200.0
HEAVY_WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS = 600.0

# Retry/network counters are retained for diagnostics only. They never reduce or
# block CPU auto-tune. Runtime capacity is controlled exclusively by the current
# completed 30-second CPU window: <70% +5, 70%-<85% hold, >=85% -1.
CPU_AUTO_TUNE_RETRY76_PRESSURE_COUNT = 2

# Used only by diagnostics/dashboard to describe recency of durable completions.
# It does not gate scale-up and cannot trigger scale-down.
CPU_AUTO_TUNE_PROGRESS_FRESH_SECONDS = 60.0
# Worker فقط وقتی واقعاً گیرکرده محسوب می‌شود که هم هیچ والت پایداری کامل نکرده
# باشد و هم برای مدت قابل‌توجهی هیچ خروجی جدیدی نداشته باشد. Worker فعال که مرتب
# مرحله/صفحه جدید گزارش می‌کند صرفاً به‌خاطر طولانی‌شدن یک والت قطع نمی‌شود.
WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS = 360.0
WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS = 240.0
# Worker فعال صرفاً به‌خاطر طولانی‌شدن یک والت قطع نمی‌شود. بازیابی فقط زمانی
# انجام می‌شود که هم پیشرفت پایداری نداشته باشد و هم خروجی Worker برای مدت تعیین‌شده
# کاملاً ساکت مانده باشد.
WORKER_STALL_CHECK_INTERVAL_SECONDS = 10.0
WORKER_STALL_NODE_COOLDOWN_SECONDS = 90.0

# اگر Xray یک نود Reality این خطای قطعی Handshake را چند بار در بازه کوتاه بدهد،
# آن VPN برای کل اجرای فعلی قرنطینه می‌شود و دیگر Sort/Promotion/Dead Recheck
# نمی‌تواند آن را دوباره وارد چرخه کاری کند.
XRAY_REALITY_CERT_ERROR_MARKER = "reality: received real certificate"
XRAY_REALITY_CERT_ERROR_THRESHOLD = 3
XRAY_REALITY_CERT_ERROR_WINDOW_SECONDS = 30.0

# اگر Worker پروکسی در چند والت پیاپی Fetch Failure بگیرد، خودش با کد مخصوص خارج
# می‌شود تا Manager سریع‌تر آن shard را به یک نود سالم منتقل کند.
PROXY_WORKER_MAX_CONSECUTIVE_FETCH_FAILURES = 3

# ژورنال append-only امتیازها؛ در قطع ناگهانی Worker، والت‌های امتیازگرفته‌شده گم نمی‌شوند.
# v52 برای تغییر schema همین فایل را با os.replace جایگزین می‌کرد و روی ویندوز
# در صورت بازبودن فایل، تمام Workerها با WinError 5 می‌مردند. هر نسخه فایل قدیمی
# را read-only می‌خواند و همه Appendهای تازه را در Journal نسخه‌بندی‌شده می‌نویسد.
SCORE_JOURNAL_FILE_NAME = "edge_scores_journal_v58.csv"
LEGACY_SCORE_JOURNAL_FILE_NAMES = (
    "edge_scores_journal_v57.csv",
    "edge_scores_journal.csv",
    "edge_scores_journal_v53.csv",
    "edge_scores_journal_v54.csv",
    "edge_scores_journal_v55.csv",
    "edge_scores_journal_v56.csv",
)

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


def active_wallet_exclusion_filters(
    *,
    max_wallets: int | None = None,
    min_positions: int | None = None,
    min_losses: int | None = None,
    min_pnl: float | None = None,
) -> list[str]:
    """Return every currently active rule that can omit a wallet from scores."""
    if FULL_WALLET_INCLUSION_MODE:
        return []
    active: list[str] = []
    if FILTER_MAX_WALLETS_TO_SCORE and max_wallets:
        active.append(f"max_wallets={int(max_wallets)}")
    if FILTER_MIN_RESOLVED_POSITIONS:
        active.append(
            f"min_resolved_positions={int(MIN_RESOLVED_POSITIONS if min_positions is None else min_positions)}"
        )
    if FILTER_MIN_LOSING_POSITIONS:
        active.append(
            f"min_losing_positions={int(MIN_LOSING_POSITIONS if min_losses is None else min_losses)}"
        )
    if FILTER_MIN_CLOSED_REALIZED_PNL:
        active.append(
            f"min_realized_pnl_after_costs={MIN_CLOSED_REALIZED_PNL if min_pnl is None else min_pnl}"
        )
    for enabled, name in (
        (FILTER_ALL_RECENT_BALANCES_NEGATIVE, "all_recent_balances_negative"),
        (FILTER_NEGATIVE_NET_EDGE, "negative_net_edge"),
        (
            FILTER_NON_POSITIVE_ONE_SHARE_NET_PNL_AFTER_COSTS,
            "non_positive_one_share_net_pnl_after_costs",
        ),
        (FILTER_MIN_RECOVERY_FACTOR, "min_recovery_factor"),
        (FILTER_NO_RECENT_7D_OPEN_OR_CLOSE, "no_recent_activity"),
        (FILTER_SHORT_HOLD_RATIO, "short_hold_ratio"),
    ):
        if enabled:
            active.append(name)
    return active


def wallet_inclusion_policy_text(
    *,
    max_wallets: int | None = None,
    min_positions: int | None = None,
    min_losses: int | None = None,
    min_pnl: float | None = None,
) -> str:
    active = active_wallet_exclusion_filters(
        max_wallets=max_wallets,
        min_positions=min_positions,
        min_losses=min_losses,
        min_pnl=min_pnl,
    )
    return (
        f"full_inclusion={FULL_WALLET_INCLUSION_MODE} "
        f"active_wallet_filters={'|'.join(active) if active else 'NONE'} "
        f"min_resolved_effective={'OFF' if not active or not FILTER_MIN_RESOLVED_POSITIONS else min_positions} "
        f"min_losses_effective={'OFF' if not active or not FILTER_MIN_LOSING_POSITIONS else min_losses} "
        f"min_pnl_effective={'OFF' if not active or not FILTER_MIN_CLOSED_REALIZED_PNL else min_pnl} "
        f"max_wallets_effective={'ALL' if FULL_WALLET_INCLUSION_MODE or not FILTER_MAX_WALLETS_TO_SCORE else max_wallets} "
        f"position_cap_enabled={LIMIT_POSITIONS_PER_WALLET} "
        f"purge_raw={PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS} "
        f"purge_universe={PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE}"
    )


def epoch_milliseconds(value: float | None = None) -> int:
    """Return an epoch-millisecond timestamp for exact refresh-cycle cutoffs."""
    timestamp = time.time() if value is None else float(value)
    return int(timestamp * 1000.0)


def normalize_epoch_milliseconds(value: Any) -> int:
    """Accept old second timestamps and new millisecond timestamps."""
    timestamp = int(safe_float(value, 0.0))
    if 0 < timestamp < 1_000_000_000_000:
        timestamp *= 1000
    return max(0, timestamp)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _log_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


_POSITION_COMPLETENESS_LOG_LOCK = threading.RLock()


def append_position_completeness_summary(
    path: Path,
    wallet: str,
    score: dict[str, Any],
    *,
    fetch_complete: bool,
    official_traded_live: bool,
) -> None:
    """Append one short, self-contained position/trade completeness verdict."""
    status = str(score.get("coverageStatus") or "unknown").strip()
    sample = str(
        score.get("missingOutcomeSample")
        or score.get("missingMarketSample")
        or "-"
    ).replace("\r", " ").replace("\n", " ")[:240]
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    line = (
        f"[{timestamp}] wallet={str(wallet).lower()} status={status} "
        f"build={BUILD_ID} verification={TRADE_SET_VERIFICATION_VERSION} "
        f"snapshot={score.get('snapshotStart', '-')}..{score.get('snapshotEnd', '-')} "
        f"positions={score.get('positions', '-')} "
        f"api_outcomes={score.get('apiMatchedTradeOutcomes', '-')}/"
        f"{score.get('discoveredTradeOutcomes', '-')} "
        f"api_position_coverage={score.get('positionCoveragePercent', '-')} "
        f"outcomes={score.get('matchedTradeOutcomes', '-')}/"
        f"{score.get('discoveredTradeOutcomes', '-')} "
        f"outcome_coverage={score.get('outcomeCoveragePercent', '-')} "
        f"missing_outcomes={score.get('missingTradeOutcomes', '-')} "
        f"extra_outcomes={score.get('extraDownloadedOutcomes', '-')} "
        f"markets={score.get('matchedTradeMarkets', '-')}/"
        f"{score.get('discoveredTradeMarkets', '-')} "
        f"missing_markets={score.get('missingTradeMarkets', '-')} "
        f"trades={score.get('verifiedTradeRows', '-')}/"
        f"{score.get('logicalTradeRows', '-')} "
        f"unresolved_trades={score.get('unresolvedTradeRows', '-')} "
        f"pagination=trades:{score.get('tradePaginationComplete', False)},"
        f"activity:{score.get('activityPaginationComplete', False)} "
        f"fetch_complete={bool(fetch_complete)} "
        f"official_live={bool(official_traded_live)} "
        f"verdict={'COMPLETE' if status == 'verified' and fetch_complete else 'INCOMPLETE'} "
        f"sample={sample}\n"
    )
    ensure_dir(path.parent)
    with _POSITION_COMPLETENESS_LOG_LOCK:
        with path.open("a", encoding="utf-8", newline="") as file:
            file.write(line)
            file.flush()


def merge_position_completeness_summaries(
    sources: list[Path],
    destination: Path,
) -> int:
    """Create one compact latest-verdict-per-wallet log from worker logs."""
    latest: dict[str, str] = {}
    for directory in sources:
        path = directory / POSITION_COMPLETENESS_LOG_FILE_NAME
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8", errors="replace") as file:
                for raw_line in file:
                    line = raw_line.strip()
                    match = re.search(r"(?:^| )wallet=(0x[0-9a-fA-F]{40})(?: |$)", line)
                    if match:
                        latest[match.group(1).lower()] = line[:1200]
        except OSError:
            continue
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    complete = sum(" verdict=COMPLETE " in f" {line} " for line in latest.values())
    header = (
        f"[{timestamp}] SUMMARY build={BUILD_ID} verification="
        f"{TRADE_SET_VERIFICATION_VERSION} wallets={len(latest)} "
        f"complete={complete} incomplete={len(latest) - complete}\n"
    )
    body = header + "\n".join(latest.values()) + ("\n" if latest else "")
    _atomic_write_text(destination, body)
    return len(latest)


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
    """Thread-safe logs.

    all_logs.txt is the complete master log and always includes errors.
    errors.txt is only a filtered copy of error lines.
    """

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


class StartupReporter:
    """Show durable startup progress in both the console and the master log.

    Startup used to do several potentially slow operations after the last visible
    ``[console]`` line.  A heartbeat keeps the current step visible even while an
    SQLite call or a large resume scan is still running.
    """

    def __init__(
        self,
        logger: RunLogRouter,
        console: Any,
        heartbeat_seconds: float,
        summary_path: Path | None = None,
        build_id: str = "",
    ) -> None:
        self.logger = logger
        self.console = console
        self.heartbeat_seconds = max(0.25, float(heartbeat_seconds))
        self.summary_path = summary_path
        self.build_id = str(build_id or BUILD_ID)
        self.started_monotonic = time.monotonic()
        self._console_lock = threading.RLock()
        self._summary_lock = threading.RLock()

    def _append_summary_event(self, message: str) -> None:
        """Persist startup visibility before the periodic diagnostic loop exists."""
        if self.summary_path is None:
            return
        try:
            ensure_dir(self.summary_path.parent)
            line = (
                f"[{_log_timestamp()}] STARTUP_EVENT build={self.build_id} "
                f"{message.rstrip()}\n"
            )
            with self._summary_lock:
                with self.summary_path.open("a", encoding="utf-8") as file:
                    file.write(line)
                    file.flush()
        except Exception:
            # A diagnostic mirror must never block program startup.
            pass

    def emit(
        self,
        step: str,
        status: str,
        detail: str = "",
        *,
        step_started: float | None = None,
    ) -> None:
        now = time.monotonic()
        step_elapsed = (
            max(0.0, now - float(step_started))
            if step_started is not None
            else 0.0
        )
        total_elapsed = max(0.0, now - self.started_monotonic)
        message = (
            f"step={step} status={status} "
            f"step_elapsed={step_elapsed:.1f}s total_elapsed={total_elapsed:.1f}s"
        )
        cleaned_detail = str(detail or "").strip().replace("\r", " ").replace("\n", " ")
        if cleaned_detail:
            message += f" {cleaned_detail}"
        self.logger.log(
            message,
            source="STARTUP",
            force_error=(status == "failed"),
        )
        self._append_summary_event(message)
        try:
            with self._console_lock:
                self.console.write(f"[{_log_timestamp()}] [STARTUP] {message}\n")
                self.console.flush()
        except Exception:
            # Logging must never become another startup blocker.
            pass

    @contextmanager
    def step(
        self,
        name: str,
        detail: str = "",
    ) -> Iterator[dict[str, Any]]:
        started = time.monotonic()
        state: dict[str, Any] = {"started_monotonic": started}
        stop_heartbeat = threading.Event()
        self.emit(name, "started", detail, step_started=started)

        def heartbeat() -> None:
            while not stop_heartbeat.wait(self.heartbeat_seconds):
                running_detail = str(state.get("progress") or detail or "")
                self.emit(
                    name,
                    "running",
                    running_detail,
                    step_started=started,
                )

        heartbeat_thread = threading.Thread(
            target=heartbeat,
            name=f"startup-heartbeat-{name}",
            daemon=True,
        )
        heartbeat_thread.start()
        try:
            yield state
        except BaseException as exc:
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=0.25)
            failure_detail = str(state.get("progress") or detail or "")
            if failure_detail:
                failure_detail += " "
            failure_detail += f"error={type(exc).__name__}: {exc}"
            self.emit(
                name,
                "failed",
                failure_detail,
                step_started=started,
            )
            raise
        else:
            stop_heartbeat.set()
            heartbeat_thread.join(timeout=0.25)
            self.emit(
                name,
                "done",
                str(state.get("detail") or state.get("progress") or detail or ""),
                step_started=started,
            )


def compact_worker_crash_report_lines(
    report_lines: list[str],
    max_traceback_lines: int = DIAGNOSTIC_CRASH_TRACEBACK_LINES,
) -> list[str]:
    """Keep crash identity plus the useful traceback/output tail for diagnostics."""
    cleaned = [str(line).rstrip("\r\n") for line in report_lines]
    metadata: list[str] = []
    output_index = -1
    for index, line in enumerate(cleaned):
        if line == "OUTPUT_TAIL:":
            output_index = index
            break
        if line and not set(line) <= {"="}:
            metadata.append(line)

    output = cleaned[output_index + 1 :] if output_index >= 0 else cleaned
    while output and (not output[-1].strip() or set(output[-1]) <= {"="}):
        output.pop()
    traceback_indexes = [
        index
        for index, line in enumerate(output)
        if "Traceback (most recent call last):" in line
    ]
    max_lines = max(8, int(max_traceback_lines))
    if traceback_indexes:
        useful_tail = output[traceback_indexes[-1] :][-max_lines:]
        tail_label = "TRACEBACK_TAIL:"
    else:
        useful_tail = output[-min(max_lines, 20) :]
        tail_label = "OUTPUT_TAIL:"

    cause = ""
    exception_pattern = re.compile(
        r"(?:^|\]\s)([A-Za-z_][\w.]*(?:Error|Exception)):\s*(.+)$"
    )
    for line in reversed(output):
        match = exception_pattern.search(line.strip())
        if match:
            cause = f"{match.group(1)}: {match.group(2)}"
            break
    result = ["RECENT_WORKER_CRASH:"]
    result.extend("  " + line for line in metadata[:9])
    result.append(f"  cause={cause or 'not-found-in-output-tail'}")
    result.append(f"  {tail_label}")
    result.extend("    " + line for line in useful_tail)
    return result


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


def _write_csv_rows_replace_safe(
    path: Path,
    fieldnames: list[str],
    rows: Any,
    *,
    operation: str,
) -> str:
    """Write a complete CSV without letting Windows rename locks kill a worker.

    ``os.replace`` is preferred because it is atomic.  Windows can reject that
    rename while another process or an antivirus scanner has the destination
    open.  After a short bounded retry, rewriting the already-closed destination
    in place is safe for these single-writer bucket files and avoids the v52
    WinError 5 crash loop.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    materialized_rows = [dict(row) for row in rows]
    temp_path = path.with_name(
        f"{path.name}.{operation}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with temp_path.open("w", newline="", encoding="utf-8") as target_file:
            writer = csv.DictWriter(target_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in materialized_rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})
            target_file.flush()
            os.fsync(target_file.fileno())

        last_replace_error: PermissionError | None = None
        for attempt in range(6):
            try:
                os.replace(temp_path, path)
                return "atomic-replace"
            except PermissionError as exc:
                last_replace_error = exc
                time.sleep(min(0.05 * (2 ** attempt), 0.8))

        # Read-only attributes also surface as WinError 5.  Make only this exact
        # file writable, then use a bounded direct rewrite fallback.
        try:
            if path.exists():
                path.chmod(path.stat().st_mode | stat.S_IWRITE)
        except OSError:
            pass

        last_direct_error: OSError | None = None
        for attempt in range(6):
            try:
                with path.open("w", newline="", encoding="utf-8") as target_file:
                    writer = csv.DictWriter(target_file, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in materialized_rows:
                        writer.writerow({key: row.get(key, "") for key in fieldnames})
                    target_file.flush()
                    os.fsync(target_file.fileno())
                return "in-place-fallback"
            except (PermissionError, OSError) as exc:
                last_direct_error = exc
                time.sleep(min(0.05 * (2 ** attempt), 0.8))

        if last_direct_error is not None:
            raise last_direct_error
        if last_replace_error is not None:
            raise last_replace_error
        raise OSError(f"Could not write CSV: {path}")
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


def open_csv_append(path: Path, fieldnames: list[str]):
    exists = path.exists() and path.stat().st_size > 0
    if exists:
        with path.open("r", newline="", encoding="utf-8-sig") as source_file:
            reader = csv.DictReader(source_file)
            existing_fieldnames = [
                str(value or "").strip() for value in (reader.fieldnames or [])
            ]
            if existing_fieldnames != fieldnames:
                existing_rows = [dict(row) for row in reader]
                migration_method = _write_csv_rows_replace_safe(
                    path,
                    fieldnames,
                    existing_rows,
                    operation="schema-migration",
                )
                print(
                    f"[csv:schema-migrated] file={path} "
                    f"old_columns={len(existing_fieldnames)} "
                    f"new_columns={len(fieldnames)} method={migration_method}",
                    flush=True,
                )
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
    """Normalize a standard 32-byte CTF condition id."""
    text = str(value or "").strip().lower()
    if re.fullmatch(r"0x[a-f0-9]{64}", text):
        return text
    return ""


def normalize_market_id(value: Any) -> str:
    """Normalize either a standard market id or Polymarket's 31-byte Combo id."""
    text = str(value or "").strip().lower()
    if re.fullmatch(r"0x(?:[a-f0-9]{64}|[a-f0-9]{62})", text):
        return text
    return ""


def is_combo_market_id(value: Any) -> bool:
    return bool(re.fullmatch(r"0x[a-f0-9]{62}", normalize_market_id(value)))


def closed_position_unique_key(pos: dict[str, Any]) -> str:
    """Stable identity for one closed-position row (one outcome asset)."""
    asset = str(pos.get("asset") or "").strip().lower()
    if asset:
        return f"asset:{asset}"

    condition_id = normalize_market_id(pos.get("conditionId"))
    raw_outcome_index = pos.get("outcomeIndex")
    outcome_index = (
        ""
        if raw_outcome_index in (None, "")
        else str(raw_outcome_index).strip()
    )
    outcome = str(pos.get("outcome") or "").strip().lower()
    if condition_id:
        return f"condition:{condition_id}|index:{outcome_index}|outcome:{outcome}"

    return "json:" + json.dumps(pos, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def trade_position_unique_key(pos: dict[str, Any]) -> str:
    """Canonical identity of one traded outcome/asset within one market.

    ``/traded`` counts markets, while both ``/trades`` and position endpoints can
    contain multiple outcome assets for the same market.  Market-only matching
    therefore cannot prove that every scoreable position was downloaded.
    """
    market = normalize_market_id(pos.get("conditionId"))
    asset = str(
        pos.get("asset")
        or pos.get("combo_position_id")
        or ""
    ).strip().lower()
    if market and asset:
        return f"market:{market}|asset:{asset}"

    raw_outcome_index = pos.get("outcomeIndex")
    outcome_index = (
        ""
        if raw_outcome_index in (None, "")
        else str(raw_outcome_index).strip()
    )
    outcome = str(pos.get("outcome") or pos.get("side") or "").strip().lower()
    if market and (outcome_index or outcome):
        return f"market:{market}|index:{outcome_index}|outcome:{outcome}"
    return ""


def position_trade_keys(
    rows: list[dict[str, Any]],
    *,
    include_reconstructed: bool = True,
) -> set[str]:
    """Return exact outcome identities represented by downloaded position rows."""
    result: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if (
            not include_reconstructed
            and str(row.get("_positionSource") or "").strip().lower()
            == "activity-cashflow-reconstruction"
        ):
            continue
        key = trade_position_unique_key(row)
        if key:
            result.add(key)
    return result


def market_from_trade_position_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    match = re.match(r"^market:(0x(?:[a-f0-9]{64}|[a-f0-9]{62}))\|", text)
    return normalize_market_id(match.group(1)) if match else ""


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
            normalize_market_id(row.get("conditionId")),
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
        self._ensure_cache_version()

        self.fallback_conn: sqlite3.Connection | None = None
        self.fallback_fetch_compatible = False
        self.fallback_trade_discovery_compatible = False
        if fallback_path is not None:
            try:
                fallback_resolved = fallback_path.resolve()
                if fallback_path.exists() and fallback_resolved != path.resolve():
                    uri = fallback_resolved.as_uri() + "?mode=ro"
                    self.fallback_conn = sqlite3.connect(uri, uri=True, timeout=60.0)
                    self.fallback_conn.execute("PRAGMA busy_timeout=60000")
                    fallback_version = self._cache_version_from(self.fallback_conn)
                    self.fallback_fetch_compatible = bool(
                        fallback_version == COMPLETE_FETCH_VERSION
                        or fallback_version.startswith("complete-market-v5-")
                    )
                    self.fallback_trade_discovery_compatible = (
                        self._meta_value_from(
                            self.fallback_conn, "market_discovery_version"
                        )
                        == TRADE_DISCOVERY_VERSION
                    )
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
            CREATE TABLE IF NOT EXISTS activity_trades (
                wallet TEXT NOT NULL,
                event_key TEXT NOT NULL,
                position_key TEXT NOT NULL,
                market TEXT NOT NULL,
                occurrences INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (wallet, event_key)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS activity_trades_wallet_position_idx
            ON activity_trades(wallet, position_key)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_scan_state (
                wallet TEXT PRIMARY KEY,
                scan_start INTEGER NOT NULL,
                scan_end INTEGER NOT NULL,
                pending_windows_json TEXT NOT NULL,
                completed_windows INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS official_traded_state (
                wallet TEXT PRIMARY KEY,
                traded INTEGER NOT NULL,
                fetched_at INTEGER NOT NULL
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS current_position_scan_state (
                wallet TEXT PRIMARY KEY,
                scan_id TEXT NOT NULL,
                refresh_token INTEGER NOT NULL DEFAULT 0,
                next_offset INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS current_position_scan_pages (
                wallet TEXT NOT NULL,
                scan_id TEXT NOT NULL,
                page_offset INTEGER NOT NULL,
                rows_json TEXT NOT NULL,
                fetched_at INTEGER NOT NULL,
                PRIMARY KEY (wallet, scan_id, page_offset)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS current_position_complete_state (
                wallet TEXT PRIMARY KEY,
                rows_json TEXT NOT NULL,
                fetched_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        try:
            conn.execute(
                "ALTER TABLE current_position_scan_state "
                "ADD COLUMN refresh_token INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute(
                "ALTER TABLE activity_trades "
                "ADD COLUMN occurrences INTEGER NOT NULL DEFAULT 1"
            )
        except sqlite3.OperationalError:
            pass
        conn.commit()

    @staticmethod
    def _cache_version_from(conn: sqlite3.Connection | None) -> str:
        if conn is None:
            return ""
        try:
            row = conn.execute(
                "SELECT value FROM cache_meta WHERE key='complete_fetch_version'"
            ).fetchone()
        except sqlite3.Error:
            return ""
        return str(row[0]) if row else ""

    @staticmethod
    def _meta_value_from(
        conn: sqlite3.Connection | None,
        key: str,
    ) -> str:
        if conn is None:
            return ""
        try:
            row = conn.execute(
                "SELECT value FROM cache_meta WHERE key=?", (key,)
            ).fetchone()
        except sqlite3.Error:
            return ""
        return str(row[0]) if row else ""

    def _ensure_cache_version(self) -> None:
        """Migrate cache without discarding proven per-market closed rows."""
        previous = self._cache_version_from(self.conn)
        previous_discovery = self._meta_value_from(
            self.conn, "market_discovery_version"
        )
        if (
            previous == COMPLETE_FETCH_VERSION
            and previous_discovery == TRADE_DISCOVERY_VERSION
        ):
            return
        with self.conn:
            if previous != COMPLETE_FETCH_VERSION:
                # closed_market_rows is keyed by an exact standard condition id and
                # remains valid across v5 -> v6. Current snapshots are time-sensitive.
                self.conn.execute("DELETE FROM current_position_scan_pages")
                self.conn.execute("DELETE FROM current_position_scan_state")
                self.conn.execute("DELETE FROM current_position_complete_state")
            if previous_discovery != TRADE_DISCOVERY_VERSION:
                # Older versions either used /activity or retained only market ids.
                # Neither can prove exact row multiplicity/outcome coverage in v57.
                self.conn.execute("DELETE FROM activity_scan_state")
                self.conn.execute("DELETE FROM activity_trades")
                self.conn.execute("DELETE FROM activity_markets")
                self.conn.execute("DELETE FROM activity_state")
            self.conn.execute(
                "INSERT OR REPLACE INTO cache_meta(key,value) VALUES"
                "('complete_fetch_version', ?)",
                (COMPLETE_FETCH_VERSION,),
            )
            self.conn.execute(
                "INSERT OR REPLACE INTO cache_meta(key,value) VALUES"
                "('market_discovery_version', ?)",
                (TRADE_DISCOVERY_VERSION,),
            )
        print(
            f"[cache:migrate] fetch_version={previous or 'legacy'} -> "
            f"{COMPLETE_FETCH_VERSION}; discovery={previous_discovery or 'legacy'} -> "
            f"{TRADE_DISCOVERY_VERSION}; exact closed-market rows preserved",
            flush=True,
        )

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
            self._snapshot_from(self.fallback_conn, wallet)
            if self.fallback_trade_discovery_compatible
            else 0,
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
        ) | (
            self._activity_markets_from(self.fallback_conn, wallet)
            if self.fallback_trade_discovery_compatible
            else set()
        )

    @staticmethod
    def _activity_trade_evidence_from(
        conn: sqlite3.Connection | None,
        wallet: str,
    ) -> dict[str, tuple[str, str]]:
        if conn is None:
            return {}
        try:
            return {
                str(event_key): (str(position_key), str(market))
                for event_key, position_key, market in conn.execute(
                    "SELECT event_key, position_key, market "
                    "FROM activity_trades WHERE wallet=?",
                    (wallet,),
                )
            }
        except sqlite3.Error:
            return {}

    def get_activity_trade_evidence(
        self,
        wallet: str,
    ) -> dict[str, tuple[str, str]]:
        result = self._activity_trade_evidence_from(self.conn, wallet)
        if self.fallback_trade_discovery_compatible:
            fallback = self._activity_trade_evidence_from(
                self.fallback_conn,
                wallet,
            )
            # Local evidence is newer and wins on the extraordinarily unlikely
            # event-key collision.
            result = {**fallback, **result}
        return result

    def get_activity_position_keys(self, wallet: str) -> set[str]:
        return {
            position_key
            for position_key, _market in self.get_activity_trade_evidence(
                wallet
            ).values()
            if position_key
        }

    def get_activity_trade_event_count(self, wallet: str) -> int:
        return sum(
            count
            for _position_key, _market, count in self.get_activity_trade_occurrences(
                wallet
            ).values()
        )

    def get_activity_unique_trade_event_count(self, wallet: str) -> int:
        return len(self.get_activity_trade_occurrences(wallet))

    @staticmethod
    def _activity_trade_occurrences_from(
        conn: sqlite3.Connection | None,
        wallet: str,
    ) -> dict[str, tuple[str, str, int]]:
        if conn is None:
            return {}
        try:
            return {
                str(event_key): (
                    str(position_key),
                    str(market),
                    max(1, int(occurrences or 1)),
                )
                for event_key, position_key, market, occurrences in conn.execute(
                    "SELECT event_key, position_key, market, occurrences "
                    "FROM activity_trades WHERE wallet=?",
                    (wallet,),
                )
            }
        except sqlite3.Error:
            return {}

    def get_activity_trade_occurrences(
        self,
        wallet: str,
    ) -> dict[str, tuple[str, str, int]]:
        result = self._activity_trade_occurrences_from(self.conn, wallet)
        if self.fallback_trade_discovery_compatible:
            fallback = self._activity_trade_occurrences_from(
                self.fallback_conn,
                wallet,
            )
            for event_key, (position_key, market, occurrences) in fallback.items():
                current = result.get(event_key)
                if current is None or occurrences > current[2]:
                    result[event_key] = (position_key, market, occurrences)
        return result

    def get_activity_trade_occurrence_counts(self, wallet: str) -> dict[str, int]:
        return {
            event_key: occurrences
            for event_key, (_position_key, _market, occurrences) in (
                self.get_activity_trade_occurrences(wallet).items()
            )
        }

    def _insert_activity_evidence(
        self,
        wallet: str,
        evidence: set[tuple[str, str, str]],
        occurrence_evidence: dict[str, tuple[str, str, int]] | None = None,
    ) -> None:
        normalized: dict[str, tuple[str, str, int]] = {}
        for event_key, position_key, market in evidence:
            if event_key and position_key and market:
                normalized[event_key] = (position_key, market, 1)
        for event_key, payload in (occurrence_evidence or {}).items():
            if not isinstance(payload, (tuple, list)) or len(payload) != 3:
                continue
            position_key, market, occurrences = payload
            if event_key and position_key and market:
                normalized[str(event_key)] = (
                    str(position_key),
                    str(market),
                    max(1, int(occurrences)),
                )
        if not normalized:
            return
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO activity_trades(
                wallet, event_key, position_key, market, occurrences
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(wallet, event_key) DO UPDATE SET
                position_key=excluded.position_key,
                market=excluded.market,
                occurrences=MAX(activity_trades.occurrences, excluded.occurrences)
            """,
            (
                (wallet, event_key, position_key, market, occurrences)
                for event_key, (position_key, market, occurrences) in normalized.items()
            ),
        )

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

    def get_activity_scan(self, wallet: str) -> dict[str, Any] | None:
        try:
            row = self.conn.execute(
                """
                SELECT scan_start, scan_end, pending_windows_json, completed_windows
                FROM activity_scan_state WHERE wallet=?
                """,
                (wallet,),
            ).fetchone()
        except sqlite3.Error:
            return None
        if not row:
            return None
        try:
            raw_windows = json.loads(str(row[2]))
            pending = [
                (max(int(item[0]), 1), max(int(item[1]), 1))
                for item in raw_windows
                if isinstance(item, (list, tuple)) and len(item) == 2
            ]
        except Exception:
            pending = []
        return {
            "scan_start": int(row[0]),
            "scan_end": int(row[1]),
            "pending_windows": pending,
            "completed_windows": int(row[3] or 0),
        }

    def checkpoint_activity_scan(
        self,
        wallet: str,
        scan_start: int,
        scan_end: int,
        pending_windows: list[tuple[int, int]],
        markets: set[str],
        evidence: set[tuple[str, str, str]],
        completed_windows: int,
        occurrence_evidence: dict[str, tuple[str, str, int]] | None = None,
    ) -> None:
        """Atomically persist market/outcome evidence and unfinished windows."""
        normalized_pending = [
            [max(int(start), 1), max(int(end), 1)]
            for start, end in pending_windows
            if int(end) >= int(start)
        ]
        with self.conn:
            if markets:
                self.conn.executemany(
                    "INSERT OR IGNORE INTO activity_markets(wallet, market) VALUES (?, ?)",
                    ((wallet, market) for market in markets),
                )
            self._insert_activity_evidence(
                wallet,
                evidence,
                occurrence_evidence=occurrence_evidence,
            )
            self.conn.execute(
                """
                INSERT INTO activity_scan_state(
                    wallet, scan_start, scan_end, pending_windows_json,
                    completed_windows, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    scan_start=excluded.scan_start,
                    scan_end=excluded.scan_end,
                    pending_windows_json=excluded.pending_windows_json,
                    completed_windows=excluded.completed_windows,
                    updated_at=excluded.updated_at
                """,
                (
                    wallet,
                    int(scan_start),
                    int(scan_end),
                    json.dumps(normalized_pending, separators=(",", ":")),
                    int(completed_windows),
                    int(time.time()),
                ),
            )

    def finish_activity_scan(
        self,
        wallet: str,
        scan_end: int,
        markets: set[str],
        evidence: set[tuple[str, str, str]],
        occurrence_evidence: dict[str, tuple[str, str, int]] | None = None,
    ) -> None:
        """Commit the last checkpoint and advance snapshot only after full proof."""
        with self.conn:
            if markets:
                self.conn.executemany(
                    "INSERT OR IGNORE INTO activity_markets(wallet, market) VALUES (?, ?)",
                    ((wallet, market) for market in markets),
                )
            self._insert_activity_evidence(
                wallet,
                evidence,
                occurrence_evidence=occurrence_evidence,
            )
            self.conn.execute(
                """
                INSERT INTO activity_state(wallet, snapshot_end) VALUES (?, ?)
                ON CONFLICT(wallet) DO UPDATE SET snapshot_end=excluded.snapshot_end
                """,
                (wallet, int(scan_end)),
            )
            self.conn.execute(
                "DELETE FROM activity_scan_state WHERE wallet=?",
                (wallet,),
            )

    def reset_activity_discovery(self, wallet: str) -> None:
        """Discard only unverified trade discovery so the next retry is fresh.

        Exact closed-position market rows are deliberately preserved.  A
        multiset mismatch can be caused by an unstable page boundary or a
        transient endpoint response; retaining the completed discovery snapshot
        would otherwise repeat the same mismatch forever on every resume.
        """
        with self.conn:
            self.conn.execute(
                "DELETE FROM activity_scan_state WHERE wallet=?",
                (wallet,),
            )
            self.conn.execute(
                "DELETE FROM activity_trades WHERE wallet=?",
                (wallet,),
            )
            self.conn.execute(
                "DELETE FROM activity_markets WHERE wallet=?",
                (wallet,),
            )
            self.conn.execute(
                "DELETE FROM activity_state WHERE wallet=?",
                (wallet,),
            )

    @staticmethod
    def _official_traded_from(
        conn: sqlite3.Connection | None,
        wallet: str,
    ) -> tuple[int, int] | None:
        if conn is None:
            return None
        try:
            row = conn.execute(
                "SELECT traded, fetched_at FROM official_traded_state WHERE wallet=?",
                (wallet,),
            ).fetchone()
        except sqlite3.Error:
            return None
        if not row:
            return None
        return int(row[0]), int(row[1])

    def get_official_traded(self, wallet: str) -> tuple[int, int] | None:
        local = self._official_traded_from(self.conn, wallet)
        fallback = self._official_traded_from(self.fallback_conn, wallet)
        if local is None:
            return fallback
        if fallback is None:
            return local
        return local if local[1] >= fallback[1] else fallback

    def upsert_official_traded(
        self,
        wallet: str,
        traded: int,
        fetched_at: int | None = None,
    ) -> None:
        timestamp = int(time.time()) if fetched_at is None else int(fetched_at)
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO official_traded_state(wallet, traded, fetched_at)
                VALUES (?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    traded=MAX(official_traded_state.traded, excluded.traded),
                    fetched_at=excluded.fetched_at
                """,
                (wallet, max(0, int(traded)), timestamp),
            )

    def get_or_create_current_position_scan(
        self,
        wallet: str,
        refresh_token: int = 0,
    ) -> tuple[str, int, list[dict[str, Any]]]:
        """Return exact resumable Current Positions scan state and saved rows."""
        row = self.conn.execute(
            "SELECT scan_id, next_offset, refresh_token "
            "FROM current_position_scan_state WHERE wallet=?",
            (wallet,),
        ).fetchone()
        requested_token = max(0, int(refresh_token))
        if row and requested_token and int(row[2] or 0) != requested_token:
            old_scan_id = str(row[0])
            with self.conn:
                self.conn.execute(
                    "DELETE FROM current_position_scan_pages "
                    "WHERE wallet=? AND scan_id=?",
                    (wallet, old_scan_id),
                )
                self.conn.execute(
                    "DELETE FROM current_position_scan_state WHERE wallet=?",
                    (wallet,),
                )
            row = None
        if row:
            scan_id = str(row[0])
            next_offset = max(0, int(row[1]))
        else:
            scan_id = f"{int(time.time_ns())}-{os.getpid()}"
            next_offset = 0
            now = int(time.time())
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO current_position_scan_state(
                        wallet, scan_id, refresh_token, next_offset, started_at, updated_at
                    ) VALUES (?, ?, ?, 0, ?, ?)
                    """,
                    (wallet, scan_id, requested_token, now, now),
                )

        rows_all: list[dict[str, Any]] = []
        seen: set[str] = set()
        try:
            cursor = self.conn.execute(
                """
                SELECT rows_json FROM current_position_scan_pages
                WHERE wallet=? AND scan_id=? AND page_offset<?
                ORDER BY page_offset ASC
                """,
                (wallet, scan_id, next_offset),
            )
            for (rows_json,) in cursor:
                try:
                    page_rows = json.loads(str(rows_json))
                except Exception:
                    continue
                if not isinstance(page_rows, list):
                    continue
                for item in page_rows:
                    if not isinstance(item, dict):
                        continue
                    key = closed_position_unique_key(item)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows_all.append(item)
        except sqlite3.Error:
            pass
        return scan_id, next_offset, rows_all

    def checkpoint_current_position_page(
        self,
        wallet: str,
        scan_id: str,
        offset: int,
        rows: list[dict[str, Any]],
    ) -> None:
        """Commit one successful page and advance exact next offset atomically."""
        now = int(time.time())
        next_offset = int(offset) + int(CURRENT_POSITION_PAGE_LIMIT)
        payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO current_position_scan_pages(
                    wallet, scan_id, page_offset, rows_json, fetched_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(wallet, scan_id, page_offset) DO UPDATE SET
                    rows_json=excluded.rows_json,
                    fetched_at=excluded.fetched_at
                """,
                (wallet, scan_id, int(offset), payload, now),
            )
            self.conn.execute(
                """
                UPDATE current_position_scan_state
                SET next_offset=?, updated_at=?
                WHERE wallet=? AND scan_id=?
                """,
                (next_offset, now, wallet, scan_id),
            )

    def finish_current_position_scan(
        self,
        wallet: str,
        scan_id: str,
        rows: list[dict[str, Any]],
    ) -> None:
        """Publish only a terminal, fully paginated snapshot; then clear scan pages."""
        now = int(time.time())
        payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO current_position_complete_state(wallet, rows_json, fetched_at)
                VALUES (?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    rows_json=excluded.rows_json,
                    fetched_at=excluded.fetched_at
                """,
                (wallet, payload, now),
            )
            self.conn.execute(
                "DELETE FROM current_position_scan_pages WHERE wallet=? AND scan_id=?",
                (wallet, scan_id),
            )
            self.conn.execute(
                "DELETE FROM current_position_scan_state WHERE wallet=? AND scan_id=?",
                (wallet, scan_id),
            )

    @staticmethod
    def _current_position_complete_from(
        conn: sqlite3.Connection | None,
        wallet: str,
    ) -> tuple[list[dict[str, Any]], int] | None:
        if conn is None:
            return None
        try:
            row = conn.execute(
                "SELECT rows_json, fetched_at FROM current_position_complete_state WHERE wallet=?",
                (wallet,),
            ).fetchone()
        except sqlite3.Error:
            return None
        if not row:
            return None
        try:
            parsed = json.loads(str(row[0]))
        except Exception:
            return None
        if not isinstance(parsed, list):
            return None
        rows = [item for item in parsed if isinstance(item, dict)]
        return rows, int(row[1])

    def get_current_position_complete(
        self,
        wallet: str,
    ) -> tuple[list[dict[str, Any]], int] | None:
        local = self._current_position_complete_from(self.conn, wallet)
        fallback = (
            self._current_position_complete_from(self.fallback_conn, wallet)
            if self.fallback_fetch_compatible
            else None
        )
        if local is None:
            return fallback
        if fallback is None:
            return local
        return local if local[1] >= fallback[1] else fallback

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
        if missing and self.fallback_conn is not None and self.fallback_fetch_compatible:
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


def fetch_official_traded_count(
    client: PolymarketClient,
    wallet: str,
    cache: CompleteFetchCache,
    *,
    force_refresh: bool = False,
) -> tuple[int, bool, str]:
    """Return official market count plus whether it came from a fresh API response."""
    cached = cache.get_official_traded(wallet)
    now = int(time.time())
    if cached is not None and not force_refresh:
        cached_count, cached_at = cached
        if now - int(cached_at) <= max(0, int(OFFICIAL_TRADED_CACHE_TTL_SECONDS)):
            print(
                f"    [traded] {wallet} official_markets={cached_count} source=cache",
                flush=True,
            )
            return int(cached_count), False, "cache"

    try:
        data = client.get_json(
            "/traded",
            {"user": wallet},
            delay_override=0.0,
            rate_limiter=ACTIVITY_API_LIMITER,
        )
        if not isinstance(data, dict) or "traded" not in data:
            raise RuntimeError(f"Unexpected /traded response: {data!r}")
        api_count = int(safe_float(data.get("traded")))
        cache.upsert_official_traded(wallet, api_count, fetched_at=now)
        print(
            f"    [traded] {wallet} official_markets={api_count} source=api",
            flush=True,
        )
        return api_count, True, "api"
    except Exception as exc:
        if cached is not None:
            cached_count, _cached_at = cached
            print(
                f"    [traded:cache-fallback] {wallet} official_markets={cached_count} "
                f"error={type(exc).__name__}",
                flush=True,
            )
            return int(cached_count), False, "cache-fallback"

        # /traded is only an optimization hint. If it is unavailable, force the
        # complete /trades + market-batch path instead of failing the wallet.
        forced = max(1, int(DIRECT_FAST_PATH_MAX_TRADED_MARKETS) + 1)
        print(
            f"    [traded:unavailable] {wallet} forcing_complete_path=true "
            f"error={type(exc).__name__}",
            flush=True,
        )
        return forced, False, "unavailable"


def activity_request(
    client: PolymarketClient,
    wallet: str,
    start_ts: int,
    end_ts: int,
    offset: int,
) -> list[dict[str, Any]]:
    """Fetch one /trades page including both maker and taker fills."""
    rows = client.get_json(
        "/trades",
        {
            "user": wallet,
            "takerOnly": False,
            "start": max(int(start_ts), 1),
            "end": max(int(end_ts), 1),
            "limit": TRADE_PAGE_LIMIT,
            "offset": offset,
        },
        delay_override=0.0,
        rate_limiter=ACTIVITY_API_LIMITER,
    )
    if not isinstance(rows, list):
        raise RuntimeError(
            f"Unexpected /trades response for {start_ts}..{end_ts} offset={offset}: "
            f"{type(rows).__name__}"
        )
    return rows


def markets_from_activity_rows(rows: list[dict[str, Any]]) -> set[str]:
    """Extract exact standard and Combo market ids from /trades rows."""
    result: set[str] = set()
    for row in rows:
        market = normalize_market_id(
            row.get("conditionId") or row.get("combo_condition_id")
        )
        if market:
            result.add(market)
    return result


def trade_event_unique_key(row: dict[str, Any]) -> str:
    """Return the v58 structural identity shared by /trades and /activity.

    Accounting representations are deliberately excluded.  In particular size,
    price, USDC size and side can describe gross/net or maker/taker views of the
    same fill.  Multiplicity is retained by ``trade_occurrence_evidence_from_rows``.
    A missing hash gets a visibly separate, conservative secondary identity; the
    reconciliation gate refuses to score those rows without on-chain evidence.
    """
    raw_hash = str(row.get("transactionHash") or "").strip().lower()
    valid_hash = bool(re.fullmatch(r"0x[0-9a-f]{64}", raw_hash))
    transaction_hash = raw_hash if valid_hash else "invalid-hash"
    raw_timestamp = str(row.get("timestamp") or "").strip()
    try:
        timestamp = str(int(Decimal(raw_timestamp)))
    except (InvalidOperation, ValueError, OverflowError):
        timestamp = raw_timestamp
    payload = "|".join(
        (
            transaction_hash,
            normalize_market_id(
                row.get("conditionId") or row.get("combo_condition_id")
            ),
            str(row.get("asset") or "").strip().lower(),
            str(row.get("outcomeIndex") if row.get("outcomeIndex") is not None else ""),
            timestamp,
        )
    )
    prefix = "core:" if valid_hash else "secondary-unverified:"
    return prefix + hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()


def trade_evidence_from_rows(
    rows: list[dict[str, Any]],
) -> set[tuple[str, str, str]]:
    """Return (event key, exact outcome key, market) evidence from /trades."""
    return {
        (event_key, position_key, market)
        for event_key, (position_key, market, _occurrences) in (
            trade_occurrence_evidence_from_rows(rows).items()
        )
    }


def trade_occurrence_evidence_from_rows(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[str, str, int]]:
    """Preserve multiplicity of otherwise identical fills returned by the API.

    Polymarket can legitimately return several byte-for-byte identical rows for
    separate fills in one transaction.  ``transactionHash`` plus the visible
    trade fields is therefore not a unique trade id.  v56 stored those rows in a
    set and under-counted both trade totals and reconstructed cash flows.
    """
    result: dict[str, tuple[str, str, int]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("/trades returned a non-object row")
        market = normalize_market_id(
            row.get("conditionId") or row.get("combo_condition_id")
        )
        position_key = trade_position_unique_key(row)
        if not market or not position_key:
            raise RuntimeError(
                "/trades row lacks exact market/outcome identity: "
                f"{repr(row)[:500]}"
            )
        event_key = trade_event_unique_key(row)
        previous = result.get(event_key)
        occurrences = 1 if previous is None else previous[2] + 1
        result[event_key] = (position_key, market, occurrences)
    return result


def merge_trade_occurrence_evidence(
    target: dict[str, tuple[str, str, int]],
    source: dict[str, tuple[str, str, int]],
) -> None:
    """Add multiplicities from disjoint pages/windows into ``target``."""
    for event_key, (position_key, market, occurrences) in source.items():
        previous = target.get(event_key)
        target[event_key] = (
            position_key,
            market,
            max(1, int(occurrences))
            + (max(1, int(previous[2])) if previous is not None else 0),
        )


def _dedupe_activity_windows(
    windows: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    result: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for raw_start, raw_end in windows:
        item = (max(int(raw_start), 1), max(int(raw_end), 1))
        if item[1] < item[0] or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _fetch_activity_window_plan(
    client: PolymarketClient,
    wallet: str,
    window_start: int,
    window_end: int,
    offset_executor: ThreadPoolExecutor,
) -> dict[str, Any]:
    """Fetch one complete window or return two smaller disjoint windows."""
    first_rows = activity_request(
        client, wallet, window_start, window_end, offset=0
    )
    occurrences = trade_occurrence_evidence_from_rows(first_rows)
    evidence = {
        (event_key, position_key, market)
        for event_key, (position_key, market, _count) in occurrences.items()
    }
    markets = {item[2] for item in evidence}
    if len(first_rows) < TRADE_PAGE_LIMIT:
        return {
            "markets": markets,
            "evidence": evidence,
            "occurrences": occurrences,
            "children": [],
        }

    last_rows = activity_request(
        client,
        wallet,
        window_start,
        window_end,
        offset=TRADE_MAX_OFFSET,
    )
    if len(last_rows) == TRADE_PAGE_LIMIT:
        midpoint = window_start + (window_end - window_start) // 2
        if midpoint < window_start or midpoint >= window_end:
            raise RuntimeError(
                "One-second /trades window exceeds offset=10000; "
                "cannot prove complete maker+taker market discovery."
            )
        return {
            "markets": set(),
            "evidence": set(),
            "occurrences": {},
            "children": [
                (window_start, midpoint),
                (midpoint + 1, window_end),
            ],
        }

    last_occurrences = trade_occurrence_evidence_from_rows(last_rows)
    last_evidence = {
        (event_key, position_key, market)
        for event_key, (position_key, market, _count) in last_occurrences.items()
    }
    evidence.update(last_evidence)
    merge_trade_occurrence_evidence(occurrences, last_occurrences)
    markets.update(item[2] for item in last_evidence)
    middle_offsets = list(
        range(TRADE_PAGE_LIMIT, TRADE_MAX_OFFSET, TRADE_PAGE_LIMIT)
    )
    future_map = {
        offset_executor.submit(
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
        page_occurrences = trade_occurrence_evidence_from_rows(rows)
        page_evidence = {
            (event_key, position_key, market)
            for event_key, (position_key, market, _count) in page_occurrences.items()
        }
        evidence.update(page_evidence)
        merge_trade_occurrence_evidence(occurrences, page_occurrences)
        markets.update(item[2] for item in page_evidence)
    return {
        "markets": markets,
        "evidence": evidence,
        "occurrences": occurrences,
        "children": [],
    }


def fetch_activity_markets_range(
    client: PolymarketClient,
    wallet: str,
    start_ts: int,
    end_ts: int,
    cache: CompleteFetchCache | None = None,
) -> tuple[set[str], set[tuple[str, str, str]], int]:
    """Complete adaptive maker+taker /trades scan with SQLite checkpoints."""
    if end_ts < start_ts:
        return set(), set(), int(end_ts)

    scan = cache.get_activity_scan(wallet) if cache is not None else None
    if scan is not None:
        scan_start = max(int(scan["scan_start"]), 1)
        scan_end = max(int(scan["scan_end"]), scan_start)
        pending = _dedupe_activity_windows(
            list(scan.get("pending_windows") or [])
        )
        completed_windows = int(scan.get("completed_windows") or 0)
        print(
            f"    [trades-resume] {wallet} scan={scan_start}..{scan_end} "
            f"pending={len(pending)} completed={completed_windows}",
            flush=True,
        )
    else:
        scan_start = max(int(start_ts), 1)
        scan_end = max(int(end_ts), scan_start)
        pending = [(scan_start, scan_end)]
        completed_windows = 0
        if cache is not None:
            cache.checkpoint_activity_scan(
                wallet,
                scan_start,
                scan_end,
                pending,
                set(),
                set(),
                completed_windows,
            )

    markets: set[str] = set()
    evidence: set[tuple[str, str, str]] = set()
    checkpoint_markets: set[str] = set()
    checkpoint_evidence: set[tuple[str, str, str]] = set()
    checkpoint_occurrences: dict[str, tuple[str, str, int]] = {}
    checkpoint_every = max(1, int(ACTIVITY_CHECKPOINT_EVERY_WINDOWS))
    window_workers = _effective_activity_window_workers()
    offset_workers = _effective_activity_fetch_workers()

    # A completed-but-not-finalized empty pending list is valid after a crash.
    if not pending:
        if cache is not None:
            cache.finish_activity_scan(wallet, scan_end, set(), set())
        return markets, evidence, scan_end

    in_flight: dict[Any, tuple[int, int]] = {}
    activity_window_failures: dict[tuple[int, int], int] = {}
    with ThreadPoolExecutor(max_workers=offset_workers) as offset_executor:
        with ThreadPoolExecutor(max_workers=window_workers) as window_executor:
            while pending or in_flight:
                while pending and len(in_flight) < window_workers:
                    window = pending.pop(0)
                    future = window_executor.submit(
                        _fetch_activity_window_plan,
                        client,
                        wallet,
                        window[0],
                        window[1],
                        offset_executor,
                    )
                    in_flight[future] = window

                completed_future = next(as_completed(list(in_flight)))
                window = in_flight.pop(completed_future)
                try:
                    result = completed_future.result()
                except Exception as exc:
                    failure_key = (int(window[0]), int(window[1]))
                    attempts = activity_window_failures.get(failure_key, 0) + 1
                    activity_window_failures[failure_key] = attempts
                    unfinished = [window] + pending + list(in_flight.values())
                    if cache is not None:
                        cache.checkpoint_activity_scan(
                            wallet,
                            scan_start,
                            scan_end,
                            _dedupe_activity_windows(unfinished),
                            checkpoint_markets,
                            checkpoint_evidence,
                            completed_windows,
                            occurrence_evidence=checkpoint_occurrences,
                        )
                        checkpoint_markets.clear()
                        checkpoint_evidence.clear()
                        checkpoint_occurrences.clear()
                    if attempts <= max(0, int(ACTIVITY_WINDOW_LOCAL_RETRIES)):
                        pending.append(window)
                        pending = _dedupe_activity_windows(pending)
                        print(
                            f"    [trades-window:retry] {wallet} "
                            f"window={window[0]}..{window[1]} "
                            f"attempt={attempts}/{ACTIVITY_WINDOW_LOCAL_RETRIES} "
                            f"error={type(exc).__name__}",
                            flush=True,
                        )
                        continue
                    raise RuntimeError(
                        f"Trades window failed after {attempts} attempts "
                        f"for {window[0]}..{window[1]}: {exc!r}"
                    ) from exc
                activity_window_failures.pop(
                    (int(window[0]), int(window[1])),
                    None,
                )
                children = _dedupe_activity_windows(
                    list(result.get("children") or [])
                )
                if children:
                    pending.extend(children)
                    pending = _dedupe_activity_windows(pending)
                    if cache is not None:
                        unfinished = pending + list(in_flight.values())
                        cache.checkpoint_activity_scan(
                            wallet,
                            scan_start,
                            scan_end,
                            _dedupe_activity_windows(unfinished),
                            checkpoint_markets,
                            checkpoint_evidence,
                            completed_windows,
                            occurrence_evidence=checkpoint_occurrences,
                        )
                        checkpoint_markets.clear()
                        checkpoint_evidence.clear()
                        checkpoint_occurrences.clear()
                    continue

                found = set(result.get("markets") or set())
                found_evidence = set(result.get("evidence") or set())
                found_occurrences = dict(result.get("occurrences") or {})
                markets.update(found)
                evidence.update(found_evidence)
                checkpoint_markets.update(found)
                checkpoint_evidence.update(found_evidence)
                merge_trade_occurrence_evidence(
                    checkpoint_occurrences,
                    found_occurrences,
                )
                completed_windows += 1

                if (
                    cache is not None
                    and completed_windows % checkpoint_every == 0
                ):
                    unfinished = pending + list(in_flight.values())
                    cache.checkpoint_activity_scan(
                        wallet,
                        scan_start,
                        scan_end,
                        _dedupe_activity_windows(unfinished),
                        checkpoint_markets,
                        checkpoint_evidence,
                        completed_windows,
                        occurrence_evidence=checkpoint_occurrences,
                    )
                    checkpoint_markets.clear()
                    checkpoint_evidence.clear()
                    checkpoint_occurrences.clear()

                if completed_windows % 20 == 0:
                    raw_trade_rows_so_far = (
                        cache.get_activity_trade_event_count(wallet)
                        if cache is not None
                        else len(evidence)
                    )
                    print(
                        f"    [trade-markets] {wallet} windows={completed_windows} "
                        f"unique_markets={len(markets)} "
                        f"unique_outcomes={len({item[1] for item in evidence})} "
                        f"unique_trade_rows={len(evidence)} "
                        f"raw_trade_rows={raw_trade_rows_so_far} "
                        f"pending={len(pending)+len(in_flight)}",
                        flush=True,
                    )

    if cache is not None:
        cache.finish_activity_scan(
            wallet,
            scan_end,
            checkpoint_markets,
            checkpoint_evidence,
            occurrence_evidence=checkpoint_occurrences,
        )
    return markets, evidence, scan_end


def independent_activity_trade_request(
    client: PolymarketClient,
    wallet: str,
    start_ts: int,
    end_ts: int,
    offset: int,
) -> list[dict[str, Any]]:
    """Fetch one stable ASC page from the independent user-activity endpoint."""
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
            "offset": int(offset),
        },
        delay_override=0.0,
        rate_limiter=ACTIVITY_API_LIMITER,
    )
    if not isinstance(rows, list):
        raise RuntimeError(
            f"Unexpected /activity TRADE response for {start_ts}..{end_ts} "
            f"offset={offset}: {type(rows).__name__}"
        )
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("/activity TRADE returned a non-object row")
        row_type = str(row.get("type") or "TRADE").strip().upper()
        if row_type != "TRADE":
            raise RuntimeError(
                f"/activity type=TRADE returned type={row_type or 'blank'}"
            )
        normalized.append(row)
    return normalized


def _fetch_independent_activity_window_plan(
    client: PolymarketClient,
    wallet: str,
    window_start: int,
    window_end: int,
    offset_executor: ThreadPoolExecutor,
) -> dict[str, Any]:
    """Read one complete /activity window or split it before the offset cap."""
    first_rows = independent_activity_trade_request(
        client,
        wallet,
        window_start,
        window_end,
        0,
    )
    occurrences = trade_occurrence_evidence_from_rows(first_rows)
    if len(first_rows) < ACTIVITY_PAGE_LIMIT:
        return {"occurrences": occurrences, "children": []}

    last_rows = independent_activity_trade_request(
        client,
        wallet,
        window_start,
        window_end,
        ACTIVITY_MAX_OFFSET,
    )
    if len(last_rows) == ACTIVITY_PAGE_LIMIT:
        midpoint = window_start + (window_end - window_start) // 2
        if midpoint < window_start or midpoint >= window_end:
            raise RuntimeError(
                "One-second /activity TRADE window exceeds offset=5000; "
                "independent trade-row completeness cannot be proven."
            )
        return {
            "occurrences": {},
            "children": [
                (window_start, midpoint),
                (midpoint + 1, window_end),
            ],
        }

    merge_trade_occurrence_evidence(
        occurrences,
        trade_occurrence_evidence_from_rows(last_rows),
    )
    middle_offsets = list(
        range(ACTIVITY_PAGE_LIMIT, ACTIVITY_MAX_OFFSET, ACTIVITY_PAGE_LIMIT)
    )
    future_map = {
        offset_executor.submit(
            independent_activity_trade_request,
            client,
            wallet,
            window_start,
            window_end,
            offset,
        ): offset
        for offset in middle_offsets
    }
    for future in as_completed(future_map):
        merge_trade_occurrence_evidence(
            occurrences,
            trade_occurrence_evidence_from_rows(future.result()),
        )
    return {"occurrences": occurrences, "children": []}


def fetch_independent_activity_trade_occurrences(
    client: PolymarketClient,
    wallet: str,
    snapshot_end: int,
) -> dict[str, tuple[str, str, int]]:
    """Build a complete trade multiset from /activity for cross-verification.

    This endpoint is intentionally separate from the /trades source used for
    discovery.  A wallet is not marked complete merely because a dataset agrees
    with counts derived from itself.
    """
    end_ts = max(1, int(snapshot_end))
    pending: list[tuple[int, int]] = [(1, end_ts)]
    occurrences: dict[str, tuple[str, str, int]] = {}
    completed_windows = 0
    offset_workers = _effective_activity_fetch_workers()
    with ThreadPoolExecutor(max_workers=offset_workers) as offset_executor:
        while pending:
            window = pending.pop(0)
            last_error: Exception | None = None
            result: dict[str, Any] | None = None
            for attempt in range(1, max(1, int(ACTIVITY_WINDOW_LOCAL_RETRIES)) + 2):
                try:
                    result = _fetch_independent_activity_window_plan(
                        client,
                        wallet,
                        window[0],
                        window[1],
                        offset_executor,
                    )
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt > max(0, int(ACTIVITY_WINDOW_LOCAL_RETRIES)):
                        break
                    print(
                        f"    [activity-verify:retry] {wallet} "
                        f"window={window[0]}..{window[1]} "
                        f"attempt={attempt}/{ACTIVITY_WINDOW_LOCAL_RETRIES} "
                        f"error={type(exc).__name__}",
                        flush=True,
                    )
            if result is None:
                raise RuntimeError(
                    f"Independent /activity verification failed for "
                    f"{window[0]}..{window[1]}: {last_error!r}"
                ) from last_error

            children = _dedupe_activity_windows(
                list(result.get("children") or [])
            )
            if children:
                pending[0:0] = children
                continue
            merge_trade_occurrence_evidence(
                occurrences,
                dict(result.get("occurrences") or {}),
            )
            completed_windows += 1
            if completed_windows % 20 == 0:
                print(
                    f"    [activity-verify] {wallet} windows={completed_windows} "
                    f"raw_rows={sum(item[2] for item in occurrences.values())} "
                    f"unique_rows={len(occurrences)} pending={len(pending)}",
                    flush=True,
                )
    return occurrences


def compare_trade_occurrence_multisets(
    trades_occurrences: dict[str, tuple[str, str, int]],
    activity_occurrences: dict[str, tuple[str, str, int]],
) -> dict[str, Any]:
    """Compare row multiplicity, not merely unique hashes or outcome assets."""
    trade_counts = {
        event_key: max(0, int(payload[2]))
        for event_key, payload in trades_occurrences.items()
    }
    activity_counts = {
        event_key: max(0, int(payload[2]))
        for event_key, payload in activity_occurrences.items()
    }
    all_keys = set(trade_counts) | set(activity_counts)
    matched = sum(
        min(trade_counts.get(key, 0), activity_counts.get(key, 0))
        for key in all_keys
    )
    missing = sum(
        max(0, activity_counts.get(key, 0) - trade_counts.get(key, 0))
        for key in all_keys
    )
    extra = sum(
        max(0, trade_counts.get(key, 0) - activity_counts.get(key, 0))
        for key in all_keys
    )
    trades_raw = sum(trade_counts.values())
    activity_raw = sum(activity_counts.values())
    logical_rows = sum(max(trade_counts.get(key, 0), activity_counts.get(key, 0)) for key in all_keys)
    invalid_hash_rows = sum(
        max(trade_counts.get(key, 0), activity_counts.get(key, 0))
        for key in all_keys
        if key.startswith("secondary-unverified:")
    )
    unresolved = missing + extra + invalid_hash_rows
    verified_rows = max(0, logical_rows - unresolved)
    if logical_rows > 0:
        coverage = verified_rows / logical_rows * 100.0
    else:
        coverage = 100.0
    verification_status = (
        "verified_api" if unresolved == 0 else
        "needs_onchain" if invalid_hash_rows or missing or extra else
        "incomplete_unresolved"
    )
    return {
        "snapshotStart": 1,
        "tradesRawRows": trades_raw,
        "activityRawRows": activity_raw,
        "logicalTradeRows": logical_rows,
        "matchedCoreRows": matched,
        "activityOnlyRows": missing,
        "tradesOnlyRows": extra,
        "exactRepeatedRows": sum(max(0, count - 1) for count in trade_counts.values()),
        "valueDifferenceRows": 0,
        "sideDifferenceRows": 0,
        "onchainVerifiedRows": 0,
        "verifiedTradeRows": verified_rows,
        "unresolvedTradeRows": unresolved,
        "tradeVerificationStatus": verification_status,
        "verificationReason": (
            "core-identity-and-multiplicity-match"
            if unresolved == 0
            else "missing-or-ambiguous-rows-require-targeted-onchain-verification"
        ),
        "downloadedTradeRows": trades_raw,
        "uniqueTradeRows": len(trade_counts),
        "duplicateTradeRows": max(0, trades_raw - len(trade_counts)),
        "activityTradeRows": activity_raw,
        "activityUniqueTradeRows": len(activity_counts),
        "matchedTradeRows": matched,
        "missingTradeRows": missing,
        "extraTradeRows": extra,
        "tradeRowCoveragePercent": f"{coverage:.2f}%",
        "tradeRowVerificationStatus": "verified" if unresolved == 0 else verification_status,
        "tradeRowSetsEqual": bool(missing == 0 and extra == 0),
    }


def get_complete_activity_markets(
    client: PolymarketClient,
    wallet: str,
    cache: CompleteFetchCache,
    snapshot_end: int | None = None,
) -> tuple[set[str], set[str], int, int, set[str], set[str]]:
    cached_markets = cache.get_activity_markets(wallet)
    cached_position_keys = cache.get_activity_position_keys(wallet)
    existing_scan = cache.get_activity_scan(wallet)
    previous_end = cache.get_activity_snapshot_end(wallet)
    if existing_scan is not None:
        start_ts = int(existing_scan["scan_start"])
        requested_end = int(existing_scan["scan_end"])
    else:
        requested_end = max(1, int(snapshot_end if snapshot_end is not None else time.time()))
        start_ts = max(previous_end - 1, 1) if previous_end else 1

    print(
        f"    [trade-markets] {wallet} cached={len(cached_markets)} "
        f"cached_outcomes={len(cached_position_keys)} "
        f"scan={start_ts}..{requested_end}",
        flush=True,
    )
    scanned_markets, scanned_evidence, snapshot_end = fetch_activity_markets_range(
        client,
        wallet,
        start_ts,
        requested_end,
        cache=cache,
    )
    all_markets = cached_markets | scanned_markets | cache.get_activity_markets(wallet)
    all_position_keys = (
        cached_position_keys
        | {item[1] for item in scanned_evidence}
        | cache.get_activity_position_keys(wallet)
    )
    trade_event_count = cache.get_activity_trade_event_count(wallet)
    if cache is None:
        trade_event_count = len(scanned_evidence)
    new_markets = all_markets - cached_markets
    touched_existing_markets = scanned_markets & cached_markets
    print(
        f"    [trade-markets:done] {wallet} total={len(all_markets)} "
        f"outcomes={len(all_position_keys)} trade_rows={trade_event_count} "
        f"new={len(new_markets)} touched_existing={len(touched_existing_markets)} "
        f"snapshot={snapshot_end}",
        flush=True,
    )
    return (
        all_markets,
        all_position_keys,
        trade_event_count,
        snapshot_end,
        new_markets,
        touched_existing_markets,
    )


def normalize_combo_position(row: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one v1 Combo position to the common scoring schema."""
    market = normalize_market_id(
        row.get("combo_condition_id") or row.get("conditionId")
    )
    if not market or not is_combo_market_id(market):
        return None
    status = str(row.get("status") or "").strip().upper()
    reported_shares = max(0.0, safe_float(row.get("shares_balance")))
    avg_price = max(0.0, safe_float(row.get("entry_avg_price_usdc")))
    total_cost = max(
        0.0,
        safe_float(
            row.get("total_cost_usdc"),
            safe_float(row.get("entry_cost_usdc")),
        ),
    )
    payout = max(0.0, safe_float(row.get("realized_payout_usdc")))
    resolved = bool(status.startswith("RESOLVED") or row.get("resolved_at"))
    redeemable = str(row.get("redeemable") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    # A resolved winning Combo can be unredeemed. The endpoint then reports zero
    # realized payout even though every share is already worth one USDC.
    gross_entry_cost = max(0.0, safe_float(row.get("gross_entry_cost_usdc")))
    inferred_entry_shares = (
        gross_entry_cost / avg_price
        if gross_entry_cost > 0.0 and avg_price > 0.0
        else 0.0
    )
    shares = reported_shares
    if shares <= 0.0 and resolved:
        shares = payout if payout > 0.0 else inferred_entry_shares
    if resolved and "WIN" in status and payout <= 0.0 and shares > 0.0:
        payout = shares
    exit_price = payout / shares if resolved and shares > 0.0 else 0.0
    legs = row.get("legs") if isinstance(row.get("legs"), list) else []
    leg_titles = [
        str((leg.get("market") or {}).get("title") or "").strip()
        for leg in legs
        if isinstance(leg, dict) and isinstance(leg.get("market"), dict)
    ]
    title = "Combo: " + " | ".join(value for value in leg_titles[:4] if value)
    if title == "Combo: ":
        title = f"Combo ({int(safe_float(row.get('legs_total')))} legs)"
    normalized = {
        "asset": str(row.get("combo_position_id") or ""),
        "conditionId": market,
        "avgPrice": avg_price,
        # Polymarket's totalBought is share quantity, while total cost is derived
        # from avgPrice. Keep size explicit so adjusted scoring never guesses it.
        "totalBought": shares,
        "size": shares,
        "realizedPnl": payout - total_cost if resolved else 0.0,
        "exitPrice": exit_price,
        "curPrice": exit_price if resolved else avg_price,
        "title": title,
        "outcome": str(row.get("side") or "Combo"),
        "outcomeIndex": str(row.get("side") or ""),
        "timestamp": row.get("resolved_at") or row.get("updated_at") or row.get("first_entry_at"),
        "openTimestamp": row.get("first_entry_at"),
        "closeTimestamp": row.get("resolved_at") or row.get("updated_at"),
        "redeemable": redeemable,
        "_positionSource": "combo-position-v1",
        "_positionState": "resolved" if resolved else "open",
    }
    return normalized


def fetch_combo_positions_complete(
    client: PolymarketClient,
    wallet: str,
    wanted_markets: set[str],
) -> tuple[list[dict[str, Any]], set[str]]:
    """Fetch every Combo position and return normalized rows plus represented ids."""
    wanted = {market for market in wanted_markets if is_combo_market_id(market)}
    if not wanted:
        return [], set()
    result: list[dict[str, Any]] = []
    represented: set[str] = set()
    seen: set[str] = set()
    for offset in range(0, COMBO_POSITION_MAX_OFFSET + 1, COMBO_POSITION_PAGE_LIMIT):
        payload = client.get_json(
            "/v1/positions/combos",
            {
                "user": wallet,
                "limit": COMBO_POSITION_PAGE_LIMIT,
                "offset": offset,
            },
            delay_override=0.0,
            rate_limiter=ACTIVITY_API_LIMITER,
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("combos"), list):
            raise RuntimeError(
                f"Unexpected Combo positions response at offset={offset}: "
                f"{type(payload).__name__}"
            )
        rows = payload["combos"]
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            market = normalize_market_id(
                raw.get("combo_condition_id") or raw.get("conditionId")
            )
            if market not in wanted:
                continue
            normalized = normalize_combo_position(raw)
            if normalized is None:
                continue
            key = closed_position_unique_key(normalized)
            if key in seen:
                continue
            seen.add(key)
            result.append(normalized)
            represented.add(market)
        pagination = payload.get("pagination")
        has_more = bool(
            isinstance(pagination, dict) and pagination.get("has_more")
        )
        if not has_more:
            print(
                f"    [combo-positions] {wallet} wanted={len(wanted)} "
                f"represented={len(represented)} rows={len(result)}",
                flush=True,
            )
            return dedupe_closed_positions(result), represented
        if not rows:
            raise RuntimeError(
                "Combo positions pagination reported has_more=true with an empty page"
            )
    raise RuntimeError(
        f"Combo positions exceeded offset={COMBO_POSITION_MAX_OFFSET}; "
        "completeness cannot be proven"
    )


def market_activity_request(
    client: PolymarketClient,
    wallet: str,
    market: str,
    start_ts: int,
    end_ts: int,
    offset: int,
) -> list[dict[str, Any]]:
    rows = client.get_json(
        "/activity",
        {
            "user": wallet,
            "market": market,
            "start": max(1, int(start_ts)),
            "end": max(1, int(end_ts)),
            "sortBy": "TIMESTAMP",
            "sortDirection": "ASC",
            "limit": ACTIVITY_PAGE_LIMIT,
            "offset": int(offset),
        },
        delay_override=0.0,
        rate_limiter=ACTIVITY_API_LIMITER,
    )
    if not isinstance(rows, list):
        raise RuntimeError(
            f"Unexpected market Activity response market={market} "
            f"window={start_ts}..{end_ts} offset={offset}"
        )
    return [row for row in rows if isinstance(row, dict)]


def _activity_row_key(row: dict[str, Any]) -> str:
    tx_hash = str(row.get("transactionHash") or "").strip().lower()
    if tx_hash:
        return "|".join(
            (
                tx_hash,
                str(row.get("type") or "").upper(),
                str(row.get("asset") or ""),
                str(row.get("outcomeIndex") or ""),
                str(row.get("side") or "").upper(),
                str(row.get("size") or ""),
                str(row.get("usdcSize") or ""),
            )
        )
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fetch_market_activity_complete(
    client: PolymarketClient,
    wallet: str,
    market: str,
    *,
    snapshot_end: int | None = None,
) -> list[dict[str, Any]]:
    """Fetch all activity for one market with exact adaptive window splitting."""
    end_ts = max(2, int(snapshot_end or time.time()))
    pending = [(1, end_ts)]
    all_rows: list[dict[str, Any]] = []
    while pending:
        start_ts, window_end = pending.pop(0)
        first = market_activity_request(
            client, wallet, market, start_ts, window_end, 0
        )
        if len(first) < ACTIVITY_PAGE_LIMIT:
            pages = [first]
        else:
            last = market_activity_request(
                client,
                wallet,
                market,
                start_ts,
                window_end,
                ACTIVITY_MAX_OFFSET,
            )
            if len(last) == ACTIVITY_PAGE_LIMIT:
                midpoint = start_ts + (window_end - start_ts) // 2
                if midpoint < start_ts or midpoint >= window_end:
                    raise RuntimeError(
                        f"One-second Activity for market {market} exceeds "
                        f"offset={ACTIVITY_MAX_OFFSET}"
                    )
                pending[0:0] = [
                    (start_ts, midpoint),
                    (midpoint + 1, window_end),
                ]
                continue
            pages = [first]
            for offset in range(
                ACTIVITY_PAGE_LIMIT,
                ACTIVITY_MAX_OFFSET,
                ACTIVITY_PAGE_LIMIT,
            ):
                pages.append(
                    market_activity_request(
                        client, wallet, market, start_ts, window_end, offset
                    )
                )
            pages.append(last)
        for page in pages:
            for row in page:
                if normalize_condition_id(row.get("conditionId")) != market:
                    continue
                # Identical visible rows can be separate fills in the same
                # transaction.  Preserve every occurrence; disjoint timestamp
                # windows and stable ASC pages prevent pagination duplicates.
                all_rows.append(row)
    return sorted(
        all_rows,
        key=lambda row: (
            safe_float(row.get("timestamp")),
            _activity_row_key(row),
        ),
    )


def reconstruct_positions_from_activity_rows(
    market: str,
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    """Rebuild fully exited outcome positions from BUY/SELL/REDEEM cash flows."""
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_type = str(row.get("type") or "").strip().upper()
        if row_type not in {"TRADE", "REDEEM"}:
            continue
        raw_outcome_index = row.get("outcomeIndex")
        if raw_outcome_index in (None, ""):
            continue
        outcome_index = str(raw_outcome_index).strip()
        group = groups.setdefault(
            outcome_index,
            {
                "buy_qty": 0.0,
                "buy_cost": 0.0,
                "exit_qty": 0.0,
                "exit_value": 0.0,
                "first_ts": None,
                "last_ts": None,
                "template": row,
                "asset": "",
                "has_trade": False,
            },
        )
        qty = max(0.0, safe_float(row.get("size")))
        value = max(
            0.0,
            safe_float(
                row.get("usdcSize"),
                qty * max(0.0, safe_float(row.get("price"))),
            ),
        )
        timestamp = parse_timestamp(row.get("timestamp"))
        if timestamp is not None:
            group["first_ts"] = (
                timestamp
                if group["first_ts"] is None
                else min(float(group["first_ts"]), timestamp)
            )
            group["last_ts"] = (
                timestamp
                if group["last_ts"] is None
                else max(float(group["last_ts"]), timestamp)
            )
        if row_type == "TRADE":
            group["has_trade"] = True
            asset = str(row.get("asset") or "").strip()
            if asset:
                group["asset"] = asset
            side = str(row.get("side") or "").strip().upper()
            if side == "BUY":
                group["buy_qty"] += qty
                group["buy_cost"] += value
            elif side == "SELL":
                group["exit_qty"] += qty
                group["exit_value"] += value
        elif row_type == "REDEEM":
            group["exit_qty"] += qty
            group["exit_value"] += value
        group["template"] = row

    reconstructed: list[dict[str, Any]] = []
    failures: list[str] = []
    for outcome_index, group in sorted(groups.items()):
        buy_qty = float(group["buy_qty"])
        exit_qty = float(group["exit_qty"])
        buy_cost = float(group["buy_cost"])
        exit_value = float(group["exit_value"])
        if not group["has_trade"] or buy_qty <= 0.0:
            failures.append(f"outcome={outcome_index}:no-buy-history")
            continue
        quantity_tolerance = max(
            float(ACTIVITY_RECONSTRUCTION_QUANTITY_TOLERANCE),
            buy_qty * float(ACTIVITY_RECONSTRUCTION_QUANTITY_TOLERANCE),
        )
        if abs(buy_qty - exit_qty) > quantity_tolerance:
            failures.append(
                f"outcome={outcome_index}:unbalanced-qty "
                f"buy={buy_qty:.12g} exit={exit_qty:.12g}"
            )
            continue
        template = dict(group["template"])
        avg_price = buy_cost / buy_qty
        exit_price = exit_value / exit_qty if exit_qty > 0.0 else 0.0
        reconstructed.append(
            {
                "asset": group["asset"] or f"reconstructed:{market}:{outcome_index}",
                "conditionId": market,
                "avgPrice": avg_price,
                "totalBought": buy_qty,
                "size": buy_qty,
                "realizedPnl": exit_value - buy_cost,
                "exitPrice": exit_price,
                "curPrice": exit_price,
                "title": template.get("title", ""),
                "slug": template.get("slug", ""),
                "eventSlug": template.get("eventSlug", ""),
                "outcome": template.get("outcome", ""),
                "outcomeIndex": outcome_index,
                "timestamp": group["last_ts"],
                "openTimestamp": group["first_ts"],
                "closeTimestamp": group["last_ts"],
                "_positionSource": "activity-cashflow-reconstruction",
                "_positionState": "resolved",
            }
        )
    if failures:
        return reconstructed, "; ".join(failures[:10])
    if not reconstructed:
        return [], "no-reconstructable-buy-sell-or-redeem-cashflow"
    return dedupe_closed_positions(reconstructed), ""


def reconstruct_missing_standard_markets(
    client: PolymarketClient,
    wallet: str,
    markets: set[str],
    *,
    snapshot_end: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    positions: list[dict[str, Any]] = []
    failures: dict[str, str] = {}
    for index, market in enumerate(sorted(markets), start=1):
        rows = fetch_market_activity_complete(
            client,
            wallet,
            market,
            snapshot_end=snapshot_end,
        )
        reconstructed, reason = reconstruct_positions_from_activity_rows(
            market, rows
        )
        # Keep every individually balanced outcome even when another outcome in
        # the same market is still open/unbalanced. Exact outcome verification
        # below decides whether the requested gap was actually repaired.
        positions.extend(reconstructed)
        if reason:
            failures[market] = reason
        print(
            f"    [activity-reconstruct] {wallet} {index}/{len(markets)} "
            f"market={market} events={len(rows)} positions={len(reconstructed)} "
            f"status={'failed' if reason else 'ok'}",
            flush=True,
        )
    return dedupe_closed_positions(positions), failures


def complete_positions_for_trade_markets(
    client: PolymarketClient,
    wallet: str,
    positions: list[dict[str, Any]],
    trade_markets: set[str],
    trade_position_keys: set[str] | None = None,
    *,
    snapshot_end: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fill and verify exact traded outcome assets, not merely market ids."""
    normalized_trade_markets = {
        market
        for value in trade_markets
        if (market := normalize_market_id(value))
    }
    normalized_trade_position_keys = {
        str(value).strip().lower()
        for value in (trade_position_keys or set())
        if market_from_trade_position_key(value)
    }
    standard_trade_markets = {
        market
        for market in normalized_trade_markets
        if not is_combo_market_id(market)
    }
    combo_trade_markets = normalized_trade_markets - standard_trade_markets
    combined = dedupe_closed_positions(positions)
    before_repair_markets = position_market_ids(combined)
    before_repair_position_keys = position_trade_keys(combined)
    api_position_keys = position_trade_keys(
        combined,
        include_reconstructed=False,
    )
    missing_position_keys_before = (
        normalized_trade_position_keys - before_repair_position_keys
    )
    missing_standard_outcome_markets = {
        market
        for value in missing_position_keys_before
        if (market := market_from_trade_position_key(value))
        and not is_combo_market_id(market)
    }
    # An outcome may be absent inside a market that already has another outcome
    # row. Repairing only missing market ids was the false-100% bug in v55.
    missing_standard = (
        standard_trade_markets - before_repair_markets
    ) | missing_standard_outcome_markets
    reconstructed: list[dict[str, Any]] = []
    reconstruction_failures: dict[str, str] = {}
    if missing_standard:
        reconstructed, reconstruction_failures = reconstruct_missing_standard_markets(
            client,
            wallet,
            missing_standard,
            snapshot_end=snapshot_end,
        )
        combined = dedupe_closed_positions(combined + reconstructed)

    combo_positions, represented_combo_markets = fetch_combo_positions_complete(
        client,
        wallet,
        combo_trade_markets,
    )
    combined = dedupe_closed_positions(combined + combo_positions)
    # Combo rows come from the official position endpoint and therefore belong
    # in the pre-reconstruction/API coverage numerator.
    api_position_keys = position_trade_keys(
        combined,
        include_reconstructed=False,
    )
    downloaded_markets = position_market_ids(combined)
    matched = downloaded_markets & normalized_trade_markets
    missing = normalized_trade_markets - downloaded_markets
    extra = downloaded_markets - normalized_trade_markets
    downloaded_position_keys = position_trade_keys(combined)
    if normalized_trade_position_keys:
        matched_position_keys = (
            downloaded_position_keys & normalized_trade_position_keys
        )
        missing_position_keys = (
            normalized_trade_position_keys - downloaded_position_keys
        )
        extra_position_keys = (
            downloaded_position_keys - normalized_trade_position_keys
        )
        api_matched_position_keys = (
            api_position_keys & normalized_trade_position_keys
        )
        api_missing_position_keys = (
            normalized_trade_position_keys - api_position_keys
        )
    else:
        matched_position_keys = set()
        missing_position_keys = set()
        extra_position_keys = set()
        api_matched_position_keys = set()
        api_missing_position_keys = set()
    outcome_denominator = len(normalized_trade_position_keys)
    api_outcome_coverage = (
        len(api_matched_position_keys) / outcome_denominator * 100.0
        if outcome_denominator
        else 0.0
    )
    final_outcome_coverage = (
        len(matched_position_keys) / outcome_denominator * 100.0
        if outcome_denominator
        else 0.0
    )
    outcome_set_complete = bool(
        normalized_trade_position_keys and not missing_position_keys
    ) or bool(not normalized_trade_markets and not normalized_trade_position_keys)
    metadata = {
        "tradeSetComplete": not missing and outcome_set_complete,
        "outcomeSetComplete": outcome_set_complete,
        "tradeMarkets": len(normalized_trade_markets),
        "standardTradeMarkets": len(standard_trade_markets),
        "comboTradeMarkets": len(combo_trade_markets),
        "tradeOutcomes": outcome_denominator,
        "downloadedTradeOutcomes": len(downloaded_position_keys),
        "apiMatchedTradeOutcomes": len(api_matched_position_keys),
        "apiMissingTradeOutcomes": len(api_missing_position_keys),
        "apiOutcomeCoveragePercent": f"{api_outcome_coverage:.2f}%",
        "matchedTradeOutcomes": len(matched_position_keys),
        "missingTradeOutcomes": len(missing_position_keys),
        "extraDownloadedOutcomes": len(extra_position_keys),
        "outcomeCoveragePercent": f"{final_outcome_coverage:.2f}%",
        "missingTradeOutcomeKeys": sorted(missing_position_keys),
        "missingTradeOutcomeSample": sorted(missing_position_keys)[:20],
        "extraDownloadedOutcomeSample": sorted(extra_position_keys)[:20],
        "downloadedMarkets": len(downloaded_markets),
        "matchedTradeMarkets": len(matched),
        "missingTradeMarkets": len(missing),
        "extraDownloadedMarkets": len(extra),
        "missingTradeMarketIds": sorted(missing),
        "missingTradeMarketSample": sorted(missing)[:20],
        "extraDownloadedMarketSample": sorted(extra)[:20],
        "reconstructedMarkets": len(position_market_ids(reconstructed)),
        "reconstructedPositions": len(reconstructed),
        "reconstructionFailures": reconstruction_failures,
        "comboPositions": len(combo_positions),
        "representedComboMarkets": len(represented_combo_markets),
    }
    print(
        f"    [trade-set-verify] {wallet} markets="
        f"{len(matched)}/{len(normalized_trade_markets)} missing={len(missing)} "
        f"outcomes={len(matched_position_keys)}/{outcome_denominator} "
        f"missing_outcomes={len(missing_position_keys)} "
        f"api_outcomes={len(api_matched_position_keys)}/{outcome_denominator} "
        f"extra_markets={len(extra)} extra_outcomes={len(extra_position_keys)} "
        f"reconstructed={metadata['reconstructedMarkets']} "
        f"combos={len(represented_combo_markets)}/{len(combo_trade_markets)}",
        flush=True,
    )
    return combined, metadata


def fetch_current_positions_complete(
    client: PolymarketClient,
    wallet: str,
    cache: CompleteFetchCache,
    refresh_token: int = 0,
) -> tuple[list[dict[str, Any]], bool, str]:
    """Live Current Positions with exact page resume.

    A failed page is left checkpointed for a later VPN. A previously complete
    snapshot may be returned only as an explicitly non-live fallback; callers that
    require complete wallet data reject it. Partial pages are never published.
    """
    scan_id, start_offset, rows_all = cache.get_or_create_current_position_scan(
        wallet,
        refresh_token=refresh_token,
    )
    seen = {closed_position_unique_key(row) for row in rows_all}
    if start_offset > 0:
        print(
            f"    [current-positions:resume] {wallet} offset={start_offset} "
            f"cached_rows={len(rows_all)}",
            flush=True,
        )

    for offset in range(
        start_offset,
        CURRENT_POSITION_MAX_OFFSET + 1,
        CURRENT_POSITION_PAGE_LIMIT,
    ):
        params = {
            "user": wallet,
            "sizeThreshold": 0,
            "limit": CURRENT_POSITION_PAGE_LIMIT,
            "offset": offset,
            "sortBy": "TITLE",
            "sortDirection": "ASC",
        }
        rows: Any = None
        last_error: Exception | None = None
        attempts = max(1, int(CURRENT_POSITION_PAGE_LOCAL_RETRIES) + 1)
        for attempt in range(1, attempts + 1):
            try:
                rows = client.get_json(
                    "/positions",
                    params,
                    delay_override=0.0,
                    rate_limiter=CLOSED_API_LIMITER,
                )
                if not isinstance(rows, list):
                    preview = repr(rows)
                    raise RuntimeError(
                        f"Unexpected /positions response at offset={offset}: "
                        f"type={type(rows).__name__} body={preview[:500]}"
                    )
                if any(not isinstance(item, dict) for item in rows):
                    raise RuntimeError(
                        f"Unexpected /positions row type at offset={offset}"
                    )
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                delay = min(
                    float(CURRENT_POSITION_PAGE_RETRY_MAX_SECONDS),
                    float(CURRENT_POSITION_PAGE_RETRY_BASE_SECONDS)
                    * (2 ** (attempt - 1)),
                )
                detail = repr(exc)[: max(100, int(CURRENT_POSITION_LOG_ERROR_MAX_CHARS))]
                print(
                    f"    [current-positions:retry] {wallet} offset={offset} "
                    f"attempt={attempt}/{attempts - 1} sleep={delay:.1f}s "
                    f"error={detail}",
                    flush=True,
                )
                time.sleep(max(0.0, delay))

        if last_error is not None:
            detail = repr(last_error)[: max(100, int(CURRENT_POSITION_LOG_ERROR_MAX_CHARS))]
            cached_complete = cache.get_current_position_complete(wallet)
            if cached_complete is not None:
                cached_rows, fetched_at = cached_complete
                age = max(0, int(time.time()) - int(fetched_at))
                print(
                    f"    [current-positions:cache-fallback] {wallet} "
                    f"failed_offset={offset} rows={len(cached_rows)} age={age}s "
                    f"partial_scan_saved=true error={detail}",
                    flush=True,
                )
                return cached_rows, False, "cache-fallback"
            print(
                f"    [current-positions:unavailable] {wallet} "
                f"failed_offset={offset} partial_scan_saved=true "
                f"wallet_completeness=false error={detail}",
                flush=True,
            )
            return [], False, "unavailable"

        assert isinstance(rows, list)
        cache.checkpoint_current_position_page(wallet, scan_id, offset, rows)
        for row in rows:
            key = closed_position_unique_key(row)
            if key not in seen:
                seen.add(key)
                rows_all.append(row)

        if len(rows) < CURRENT_POSITION_PAGE_LIMIT:
            cache.finish_current_position_scan(wallet, scan_id, rows_all)
            print(
                f"    [current-positions:done] {wallet} rows={len(rows_all)} "
                f"pages={(offset // CURRENT_POSITION_PAGE_LIMIT) + 1} source=live",
                flush=True,
            )
            return rows_all, True, "live"

    cached_complete = cache.get_current_position_complete(wallet)
    if cached_complete is not None:
        cached_rows, fetched_at = cached_complete
        age = max(0, int(time.time()) - int(fetched_at))
        print(
            f"    [current-positions:max-offset-cache] {wallet} "
            f"rows={len(cached_rows)} age={age}s partial_scan_saved=true",
            flush=True,
        )
        return cached_rows, False, "cache-fallback-max-offset"
    print(
        f"    [current-positions:max-offset-unavailable] {wallet} "
        f"partial_scan_saved=true wallet_completeness=false",
        flush=True,
    )
    return [], False, "unavailable-max-offset"


def api_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def is_resolved_current_position(row: dict[str, Any]) -> bool:
    """Current-position rows are scoreable only after the market is redeemable."""
    return api_boolean(row.get("redeemable"))


def normalize_resolved_current_position(row: dict[str, Any]) -> dict[str, Any]:
    """Map one redeemable /positions row onto the closed-position scoring schema."""
    normalized = dict(row)
    original_realized = safe_float(row.get("realizedPnl"))
    if row.get("cashPnl") not in (None, ""):
        terminal_pnl = safe_float(row.get("cashPnl"))
    else:
        terminal_pnl = original_realized
    normalized["_positionSource"] = "current-redeemable"
    normalized["_apiRealizedPnl"] = original_realized
    normalized["realizedPnl"] = terminal_pnl
    if not any(
        normalized.get(key) not in (None, "")
        for key in ("timestamp", "resolvedAt", "closedAt", "redeemedAt")
    ) and normalized.get("endDate") not in (None, ""):
        normalized["resolvedAt"] = normalized.get("endDate")
    return normalized


def merge_closed_and_current_positions(
    closed_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Merge settled rows without double-counting assets present in both endpoints.

    /closed-positions remains authoritative on overlap. Redeemable /positions rows
    fill the historical gap for resolved-but-unredeemed outcomes. Open rows are kept
    in the raw combined dataset but tagged so scoring never counts them as wins/losses.
    """
    merged_by_key: dict[str, dict[str, Any]] = {}
    for row in dedupe_closed_positions(closed_rows):
        item = dict(row)
        item.setdefault("_positionSource", "closed")
        item.setdefault("_positionState", "resolved")
        merged_by_key[closed_position_unique_key(item)] = item

    resolved_current = 0
    open_current = 0
    overlap = 0
    resolved_added = 0
    open_added = 0
    for row in current_rows:
        if not isinstance(row, dict):
            continue
        if is_resolved_current_position(row):
            resolved_current += 1
            normalized = normalize_resolved_current_position(row)
            normalized["_positionState"] = "resolved"
            added_kind = "resolved"
        else:
            open_current += 1
            normalized = dict(row)
            normalized["_positionSource"] = "current-open"
            normalized["_positionState"] = "open"
            added_kind = "open"
        key = closed_position_unique_key(normalized)
        if key in merged_by_key:
            overlap += 1
            continue
        merged_by_key[key] = normalized
        if added_kind == "resolved":
            resolved_added += 1
        else:
            open_added += 1

    merged = dedupe_closed_positions(list(merged_by_key.values()))
    return merged, {
        "closedRows": len(dedupe_closed_positions(closed_rows)),
        "currentRows": len(current_rows),
        "resolvedCurrentRows": resolved_current,
        "openCurrentRows": open_current,
        "closedCurrentOverlap": overlap,
        "resolvedCurrentAdded": resolved_added,
        "openCurrentAdded": open_added,
        "combinedResolvedRows": len(merged) - open_added,
        "combinedPositionRows": len(merged),
    }


def position_market_ids(rows: list[dict[str, Any]]) -> set[str]:
    return {
        market
        for row in rows
        if isinstance(row, dict)
        and (market := normalize_market_id(row.get("conditionId")))
    }

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


def _fetch_closed_market_batch_with_local_retry(
    client: PolymarketClient,
    wallet: str,
    batch: list[str],
) -> dict[str, list[dict[str, Any]]]:
    retry_limit = (
        int(MARKET_BATCH_SINGLE_LOCAL_RETRIES)
        if len(batch) <= 1
        else int(MARKET_BATCH_MULTI_LOCAL_RETRIES)
    )
    attempts = max(1, retry_limit + 1)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fetch_closed_market_batch(client, wallet, batch)
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            delay = min(
                float(MARKET_BATCH_RETRY_MAX_SECONDS),
                float(MARKET_BATCH_RETRY_BASE_SECONDS) * (2 ** (attempt - 1)),
            )
            print(
                f"    [market-batch:retry] {wallet} markets={len(batch)} "
                f"attempt={attempt}/{attempts - 1} "
                f"sleep={delay:.1f}s error={type(exc).__name__}",
                flush=True,
            )
            time.sleep(max(0.0, delay))
    assert last_error is not None
    raise last_error


def fetch_closed_markets_parallel(
    client: PolymarketClient,
    wallet: str,
    markets: set[str],
    cache: CompleteFetchCache,
    *,
    force_refresh: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """Exact per-market resume with bounded futures and failure isolation.

    Every successful batch is committed immediately. A later failure cannot erase
    successful work. Failed multi-market batches are split until the exact failing
    market is isolated; no market is silently skipped.
    """
    market_list = sorted(markets)
    cached = {} if force_refresh else cache.get_closed_rows(wallet, market_list)
    missing = [market for market in market_list if market not in cached]
    result = dict(cached)
    fetched_rows_count = sum(len(rows) for rows in result.values())

    initial_batches = chunked(missing, CLOSED_MARKET_BATCH_SIZE)
    if not initial_batches:
        return result

    workers = max(1, min(_effective_closed_fetch_workers(), len(initial_batches)))
    max_inflight = max(
        workers,
        workers * max(1, int(MARKET_BATCH_MAX_INFLIGHT_MULTIPLIER)),
    )
    pending: deque[list[str]] = deque(initial_batches)
    in_flight: dict[Any, list[str]] = {}
    failed_singletons: dict[str, str] = {}
    completed_groups = 0
    completed_markets = 0
    split_count = 0
    retry_failures = 0
    failure_streak = 0
    abort_error = ""

    print(
        f"    [market-batches] {wallet} markets={len(market_list)} "
        f"cached={len(cached)} fetch={len(missing)} "
        f"batches={len(initial_batches)} workers={workers} "
        f"max_inflight={max_inflight}",
        flush=True,
    )

    def submit_ready(executor: ThreadPoolExecutor) -> None:
        while pending and len(in_flight) < max_inflight:
            batch = pending.popleft()
            future = executor.submit(
                _fetch_closed_market_batch_with_local_retry,
                client,
                wallet,
                batch,
            )
            in_flight[future] = batch

    with ThreadPoolExecutor(max_workers=workers) as executor:
        submit_ready(executor)
        while in_flight or pending:
            if not in_flight:
                submit_ready(executor)
                if not in_flight:
                    break

            future = next(as_completed(tuple(in_flight)))
            batch = in_flight.pop(future)
            try:
                rows_by_market = future.result()
            except Exception as exc:
                retry_failures += 1
                failure_streak += 1
                if not abort_error:
                    if len(batch) > 1:
                        midpoint = max(1, len(batch) // 2)
                        left = batch[:midpoint]
                        right = batch[midpoint:]
                        # appendleft in reverse order preserves deterministic order
                        if right:
                            pending.appendleft(right)
                        if left:
                            pending.appendleft(left)
                        split_count += 1
                        print(
                            f"    [market-batch:split] {wallet} markets={len(batch)} "
                            f"into={len(left)}+{len(right)} "
                            f"error={type(exc).__name__}",
                            flush=True,
                        )
                    else:
                        market = batch[0]
                        failed_singletons[market] = repr(exc)
                        print(
                            f"    [market-batch:singleton-failed] {wallet} "
                            f"market={market} error={exc!r}",
                            flush=True,
                        )

                    if failure_streak >= max(
                        1, int(MARKET_BATCH_PROXY_ABORT_FAILURE_STREAK)
                    ):
                        abort_error = (
                            f"{failure_streak} consecutive market-batch failures; "
                            "preserved every successful checkpoint and requesting VPN failover"
                        )
                        # Stop creating new work, but drain already-running futures so
                        # any success they produce is still committed to SQLite.
                        pending.clear()
                else:
                    print(
                        f"    [market-batch:drain-failed] {wallet} "
                        f"markets={len(batch)} error={type(exc).__name__}",
                        flush=True,
                    )
            else:
                failure_streak = 0
                result.update(rows_by_market)
                cache.upsert_closed_rows(wallet, rows_by_market)
                completed_groups += 1
                completed_markets += len(rows_by_market)
                fetched_rows_count += sum(len(rows) for rows in rows_by_market.values())
                progress_every = max(1, int(MARKET_BATCH_PROGRESS_EVERY))
                if (
                    completed_groups == 1
                    or completed_groups % progress_every == 0
                    or (not pending and len(in_flight) == 0)
                ):
                    print(
                        f"    [market-batches:progress] {wallet} "
                        f"groups={completed_groups} "
                        f"cached_markets={len(result)}/{len(market_list)} "
                        f"remaining={max(0, len(market_list)-len(result))} "
                        f"inflight={len(in_flight)} queued={len(pending)} "
                        f"splits={split_count} failures={retry_failures} "
                        f"closed_rows={fetched_rows_count}",
                        flush=True,
                    )
            if not abort_error:
                submit_ready(executor)

    if abort_error:
        raise RuntimeError(
            f"Market-batch pass aborted for failover: {abort_error}"
        )
    if failed_singletons:
        sample = list(sorted(failed_singletons.items()))[:5]
        raise RuntimeError(
            f"{len(failed_singletons)} individual market batch(es) could not be "
            f"proven complete after retries; successful markets remain checkpointed; "
            f"sample={sample!r}"
        )

    missing_after = [market for market in market_list if market not in result]
    if missing_after:
        raise RuntimeError(
            f"Market-batch completeness check failed: "
            f"{len(missing_after)} market(s) have no cached result"
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
    refresh_token: int = 0,
    initial_current_snapshot: tuple[list[dict[str, Any]], bool, str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Current Positions is required because resolved-but-unredeemed outcomes do not
    # necessarily appear in /closed-positions at all.
    if initial_current_snapshot is None:
        initial_current, initial_current_live, initial_current_source = (
            fetch_current_positions_complete(
                client,
                wallet,
                cache,
                refresh_token=refresh_token,
            )
        )
    else:
        initial_current, initial_current_live, initial_current_source = (
            initial_current_snapshot
        )
    if not initial_current_live:
        raise RuntimeError(
            f"Current Positions could not be fetched completely "
            f"(source={initial_current_source}); refusing incomplete wallet data"
        )
    initial_current_markets = {
        market
        for row in initial_current
        if (market := normalize_condition_id(row.get("conditionId")))
    }

    (
        activity_markets,
        activity_position_keys,
        activity_trade_rows,
        snapshot_end,
        _initial_new_markets,
        initial_touched_markets,
    ) = get_complete_activity_markets(
        client, wallet, cache
    )
    standard_activity_markets = {
        market for market in activity_markets if not is_combo_market_id(market)
    }
    combo_activity_markets = activity_markets - standard_activity_markets
    if not activity_markets:
        combined, merge_metadata = merge_closed_and_current_positions(
            [], initial_current
        )
        requested_cap = max(int(max_positions), 0)
        cap_applied = requested_cap < len(combined)
        if cap_applied:
            combined = combined[:requested_cap]
        return combined, {
            "complete": not cap_applied,
            "fetchMethod": "trades-market-batches-exact-set",
            "activityMarkets": 0,
            "downloadedTradeRows": activity_trade_rows,
            "tradePaginationComplete": True,
            "tradeMarkets": 0,
            "tradeOutcomes": len(activity_position_keys),
            "standardTradeMarkets": 0,
            "comboTradeMarkets": 0,
            "downloadedMarkets": len(position_market_ids(combined)),
            "matchedTradeMarkets": 0,
            "missingTradeMarkets": 0,
            "extraDownloadedMarkets": len(position_market_ids(combined)),
            "missingTradeMarketIds": [],
            "missingTradeMarketSample": [],
            "tradeSetComplete": True,
            "closedMarkets": 0,
            "currentMarkets": len(initial_current_markets),
            "currentPositionsSource": initial_current_source,
            "currentPositionsLiveComplete": initial_current_live,
            "snapshotEnd": snapshot_end,
            "capApplied": cap_applied,
            **merge_metadata,
        }

    rows_by_market = fetch_closed_markets_parallel(
        client, wallet, standard_activity_markets, cache
    )
    # Catch markets created while the long scan was running. This uses the same
    # resumable checkpoint engine, so a stop during catch-up also continues exactly.
    (
        _all_after_catchup,
        all_position_keys_after_catchup,
        trade_event_count_after_catchup,
        catchup_end,
        catchup_new_markets,
        catchup_touched_markets,
    ) = get_complete_activity_markets(
        client, wallet, cache
    )
    activity_position_keys = all_position_keys_after_catchup
    activity_trade_rows = trade_event_count_after_catchup
    catchup_markets = catchup_new_markets | catchup_touched_markets
    if catchup_markets:
        activity_markets |= catchup_new_markets
        standard_catchup_markets = {
            market for market in catchup_markets if not is_combo_market_id(market)
        }
        standard_activity_markets |= {
            market for market in catchup_new_markets if not is_combo_market_id(market)
        }
        combo_activity_markets |= {
            market for market in catchup_new_markets if is_combo_market_id(market)
        }
        if standard_catchup_markets:
            rows_by_market.update(
                fetch_closed_markets_parallel(
                    client,
                    wallet,
                    standard_catchup_markets,
                    cache,
                    force_refresh=True,
                )
            )

    current_rows, current_live, current_source = fetch_current_positions_complete(
        client,
        wallet,
        cache,
        refresh_token=refresh_token,
    )
    if not current_live:
        raise RuntimeError(
            f"Final Current Positions verification failed "
            f"(source={current_source}); refusing incomplete wallet data"
        )
    current_markets = {
        market
        for row in current_rows
        if (market := normalize_condition_id(row.get("conditionId")))
    }

    # Refresh markets known to be volatile. This is only an optimization; exact
    # completeness comes from a successful per-market batch result, including [] rows.
    volatile_markets = (
        initial_current_markets
        | current_markets
        | {
            market
            for market in (initial_touched_markets | catchup_markets)
            if not is_combo_market_id(market)
        }
    )
    if volatile_markets:
        rows_by_market.update(
            fetch_closed_markets_parallel(
                client, wallet, volatile_markets, cache, force_refresh=True
            )
        )

    # Every activity market with an empty closed result may simply be open/current.
    # Refresh those empty results once at the end so a market that closed during a
    # long scan is captured even when /positions is unavailable.
    empty_markets = {
        market
        for market in activity_markets
        if market in rows_by_market and not rows_by_market.get(market)
    }
    if empty_markets:
        print(
            f"    [empty-market-final-refresh] {wallet} markets={len(empty_markets)} "
            f"current_source={current_source}",
            flush=True,
        )
        rows_by_market.update(
            fetch_closed_markets_parallel(
                client, wallet, empty_markets, cache, force_refresh=True
            )
        )

    # A market is proven fetched when it exists as a key, even if its successful
    # response contains zero closed rows. Only absent keys require repair.
    missing_proof: set[str] = standard_activity_markets - set(rows_by_market)
    for repair_pass in range(1, COVERAGE_REPAIR_PASSES + 1):
        if not missing_proof:
            break
        print(
            f"    [coverage-repair] {wallet} pass={repair_pass} "
            f"unproven_markets={len(missing_proof)}",
            flush=True,
        )
        rows_by_market.update(
            fetch_closed_markets_parallel(
                client, wallet, missing_proof, cache, force_refresh=True
            )
        )
        missing_proof = standard_activity_markets - set(rows_by_market)

    if missing_proof:
        raise RuntimeError(
            f"Closed market completeness could not be proven for "
            f"{len(missing_proof)} market(s) after exact retries"
        )

    closed_positions = flatten_closed_market_rows(rows_by_market)
    resolved_positions, merge_metadata = merge_closed_and_current_positions(
        closed_positions,
        current_rows,
    )
    resolved_positions, exact_trade_metadata = complete_positions_for_trade_markets(
        client,
        wallet,
        resolved_positions,
        activity_markets,
        activity_position_keys,
        snapshot_end=catchup_end,
    )
    closed_markets = {
        market for market, rows in rows_by_market.items() if rows
    }
    activity_only_markets = {
        market
        for market in standard_activity_markets
        if not rows_by_market.get(market) and market not in current_markets
    }
    activity_only_sample = sorted(activity_only_markets)[:20]
    if activity_only_markets:
        print(
            f"    [coverage-warning] {wallet} activity_only_markets="
            f"{len(activity_only_markets)}; each market batch completed successfully; "
            f"current_source={current_source}",
            flush=True,
        )

    requested_cap = max(int(max_positions), 0)
    cap_applied = requested_cap < len(resolved_positions)
    if cap_applied:
        resolved_positions = resolved_positions[:requested_cap]

    metadata = {
        "complete": not cap_applied,
        "fetchMethod": "trades-market-batches-exact-outcomes",
        "activityMarkets": len(activity_markets),
        "downloadedTradeRows": activity_trade_rows,
        "tradePaginationComplete": True,
        "standardTradeMarkets": len(standard_activity_markets),
        "comboTradeMarkets": len(combo_activity_markets),
        "provenMarketBatches": len(standard_activity_markets - missing_proof),
        "closedMarkets": len(closed_markets),
        "currentMarkets": len(current_markets),
        "currentPositionsSource": current_source,
        "currentPositionsLiveComplete": current_live,
        "currentEndpointOptional": bool(CURRENT_POSITION_OPTIONAL_FOR_CLOSED_COMPLETENESS),
        "closedPositions": len(closed_positions),
        "resolvedPositionsCombined": merge_metadata["combinedResolvedRows"],
        "positionsCombined": len(resolved_positions),
        "snapshotEnd": catchup_end,
        "missingMarkets": len(activity_only_markets),
        "activityOnlyMarkets": len(activity_only_markets),
        "activityOnlyMarketSample": activity_only_sample,
        "coverageWarning": bool(activity_only_markets),
        "capApplied": cap_applied,
        **exact_trade_metadata,
        **merge_metadata,
    }
    print(
        f"    [positions-complete] {wallet} closed_positions={len(closed_positions)} "
        f"resolved_current_added={merge_metadata['resolvedCurrentAdded']} "
        f"open_current_added={merge_metadata['openCurrentAdded']} "
        f"combined_positions={len(resolved_positions)} "
        f"combined_resolved={merge_metadata['combinedResolvedRows']} "
        f"trade_markets={len(activity_markets)} proven_batches={len(rows_by_market)} "
        f"closed_markets={len(closed_markets)} current_markets={len(current_markets)} "
        f"current_source={current_source} "
        f"activity_only_markets={len(activity_only_markets)}",
        flush=True,
    )
    return resolved_positions, metadata

def fetch_closed_positions(
    client: PolymarketClient,
    wallet: str,
    limit: int = 50,
    max_positions: int = 100000,
    progress_every: int = 500,
    page_cache: dict[str, dict[int, list[dict[str, Any]]]] | None = None,
    page_cache_file=None,
    complete_cache: CompleteFetchCache | None = None,
    refresh_token: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch a complete resolved dataset from closed plus redeemable current rows."""
    del limit, progress_every, page_cache, page_cache_file

    if complete_cache is None:
        raise RuntimeError("CompleteFetchCache is required for accurate mode-2 fetching.")

    traded_count, traded_live, traded_source = fetch_official_traded_count(
        client,
        wallet,
        complete_cache,
        force_refresh=bool(refresh_token),
    )
    (
        exact_trade_markets,
        exact_trade_position_keys,
        trade_event_count,
        trade_snapshot_end,
        _new_trade_markets,
        _touched_trade_markets,
    ) = get_complete_activity_markets(client, wallet, complete_cache)

    requested_cap = max(int(max_positions), 0)
    user_requested_small_cap = requested_cap <= CLOSED_POSITIONS_MAX_API_OFFSET
    initial_current_snapshot: tuple[list[dict[str, Any]], bool, str] | None = None
    should_try_direct = bool(
        (traded_live and traded_count <= DIRECT_FAST_PATH_MAX_TRADED_MARKETS)
        or user_requested_small_cap
        or not COMPLETE_CLOSED_POSITION_FETCH
    )
    if should_try_direct:
        current_rows, current_live, current_source = fetch_current_positions_complete(
            client,
            wallet,
            complete_cache,
            refresh_token=refresh_token,
        )
        initial_current_snapshot = (current_rows, current_live, current_source)
        closed_rows, closed_complete, reason = fetch_closed_positions_direct_asc(
            client, wallet, max_positions
        )
        if closed_complete and current_live:
            combined, merge_metadata = merge_closed_and_current_positions(
                closed_rows,
                current_rows,
            )
            combined, exact_trade_metadata = complete_positions_for_trade_markets(
                client,
                wallet,
                combined,
                exact_trade_markets,
                exact_trade_position_keys,
                snapshot_end=trade_snapshot_end,
            )
            coverage_proven = bool(
                traded_live
                and len(exact_trade_markets) == max(0, int(traded_count))
                and exact_trade_metadata.get("tradeSetComplete")
                and exact_trade_metadata.get("outcomeSetComplete")
            )
            cap_applied = requested_cap < len(combined)
            if cap_applied:
                combined = combined[:requested_cap]
            if coverage_proven and not cap_applied:
                metadata = {
                    "complete": True,
                    "fetchMethod": "direct-closed-plus-current",
                    "directClosedMethod": reason,
                    "officialTradedMarkets": traded_count,
                    "officialTradedSource": traded_source,
                    "officialTradedLive": traded_live,
                    "officialCountMatchesDiscoveredTrades": True,
                    "tradeSetVerified": True,
                    "downloadedTradeRows": trade_event_count,
                    "tradePaginationComplete": True,
                    "coveredMarkets": exact_trade_metadata["matchedTradeMarkets"],
                    "currentPositionsSource": current_source,
                    "currentPositionsLiveComplete": current_live,
                    "capApplied": False,
                    **exact_trade_metadata,
                    **merge_metadata,
                }
                print(
                    f"    [positions:done] {wallet} combined={len(combined)} "
                    f"closed={len(closed_rows)} current={len(current_rows)} "
                    f"resolved_current_added={merge_metadata['resolvedCurrentAdded']} "
                    f"matched_markets={exact_trade_metadata['matchedTradeMarkets']}/"
                    f"{traded_count} matched_outcomes="
                    f"{exact_trade_metadata['matchedTradeOutcomes']}/"
                    f"{len(exact_trade_position_keys)} "
                    f"method=direct-closed-current-exact-outcomes",
                    flush=True,
                )
                return combined, metadata
            print(
                f"    [positions:fallback] {wallet} direct coverage not proven "
                f"matched_markets={exact_trade_metadata['matchedTradeMarkets']} "
                f"discovered={len(exact_trade_markets)} official={traded_count} "
                f"missing={exact_trade_metadata['missingTradeMarkets']} "
                f"missing_outcomes={exact_trade_metadata['missingTradeOutcomes']} "
                f"official_source={traded_source} cap_applied={cap_applied}; "
                "switching to exact trade-market batches",
                flush=True,
            )
        else:
            print(
                f"    [positions:fallback] {wallet} direct_closed_complete="
                f"{closed_complete} current_live_complete={current_live} "
                f"closed_reason={reason} current_source={current_source}; "
                "switching to activity + market batches",
                flush=True,
            )

        if not COMPLETE_CLOSED_POSITION_FETCH:
            metadata = {
                "complete": False,
                "fetchMethod": "incomplete-direct-disabled-full-fetch",
                "officialTradedMarkets": traded_count,
                "officialTradedSource": traded_source,
                "closedPositions": len(closed_rows),
                "currentPositions": len(current_rows),
            }
            raise RuntimeError(
                "COMPLETE_CLOSED_POSITION_FETCH=False prevented proof of complete "
                f"wallet data: {metadata}"
            )

    positions, metadata = fetch_closed_positions_market_complete(
        client,
        wallet,
        max_positions,
        complete_cache,
        refresh_token=refresh_token,
        initial_current_snapshot=(
            initial_current_snapshot
            if initial_current_snapshot is not None and initial_current_snapshot[1]
            else None
        ),
    )
    metadata["officialTradedMarkets"] = traded_count
    metadata["officialTradedSource"] = traded_source
    metadata["officialTradedLive"] = traded_live
    metadata["officialCountMatchesDiscoveredTrades"] = bool(
        traded_live
        and int(metadata.get("tradeMarkets") or 0) == max(0, int(traded_count))
    )
    metadata["tradeSetVerified"] = bool(
        metadata.get("tradeSetComplete")
        and metadata["officialCountMatchesDiscoveredTrades"]
    )
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
    """Parse API timestamps without relying on the platform C time range.

    Polymarket occasionally returns placeholder ISO dates (for example year 1)
    or timestamps in micro/nanoseconds.  ``datetime.timestamp()`` can raise
    ``OSError: [Errno 22]`` for those values on Windows and used to terminate an
    entire Worker.  Normalize common epoch units, reject values outside the
    useful Polymarket range, and convert ISO values with pure datetime
    arithmetic so malformed metadata can never kill scoring.
    """

    def validate_epoch_seconds(raw: float) -> float | None:
        if not math.isfinite(raw):
            return None
        if raw < 0.0 or raw > 4_102_444_800.0:
            return None
        return raw

    def normalize_epoch(raw: float) -> float | None:
        if not math.isfinite(raw):
            return None
        magnitude = abs(raw)
        if magnitude >= 100_000_000_000_000_000:
            raw /= 1_000_000_000.0  # nanoseconds
        elif magnitude >= 100_000_000_000_000:
            raw /= 1_000_000.0  # microseconds
        elif magnitude >= 100_000_000_000:
            raw /= 1_000.0  # milliseconds
        # Polymarket data cannot legitimately predate Unix time or extend past
        # year 2100.  Keeping the range bounded also protects fromtimestamp().
        return validate_epoch_seconds(raw)

    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return normalize_epoch(float(value))
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", text):
        return normalize_epoch(float(text))
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed_utc = parsed.astimezone(timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return validate_epoch_seconds((parsed_utc - epoch).total_seconds())
    except (ValueError, OverflowError, OSError):
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
    try:
        return datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, OverflowError, OSError):
        return "unknown"


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
    open_positions = 0
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
        if str(pos.get("_positionState") or "").strip().lower() == "open":
            open_positions += 1
            recent_balance_values.append(current_position_value(pos))
            continue
        position_trade_ts = trade_timestamp(pos)
        if position_trade_ts is not None:
            first_trade_ts = (
                position_trade_ts if first_trade_ts is None else min(first_trade_ts, position_trade_ts)
            )
            last_trade_ts = (
                position_trade_ts if last_trade_ts is None else max(last_trade_ts, position_trade_ts)
            )
            day_key = trade_day_key(pos)
            if day_key != "unknown":
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
    historical_position_count = len(pnl_series)
    average_trades_per_day = (
        historical_position_count / trading_days if trading_days else 0.0
    )
    max_trades_in_one_day = max(trades_by_day.values()) if trades_by_day else 0
    days_since_last_trade = (now_ts - last_trade_ts) / (24 * 60 * 60) if last_trade_ts is not None else 0.0
    if first_trade_ts is not None and last_trade_ts is not None:
        first_trade_day = datetime.fromtimestamp(first_trade_ts, timezone.utc).date()
        last_trade_day = datetime.fromtimestamp(last_trade_ts, timezone.utc).date()
        calendar_trade_span_days = max((last_trade_day - first_trade_day).days + 1, 1)
    else:
        calendar_trade_span_days = 0
    trades_per_calendar_day_first_to_last = (
        historical_position_count / calendar_trade_span_days
        if calendar_trade_span_days
        else 0.0
    )
    profit_per_trade_after_costs = (
        realized_pnl_after_costs / historical_position_count
        if historical_position_count
        else 0.0
    )
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
        "downloadedMarkets": len(position_market_ids(positions)),
        "openPositions": open_positions,
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


def add_polymarket_trade_metrics(
    score: dict[str, Any],
    official_traded_markets: int | None,
    *,
    downloaded_market_ids: set[str] | None = None,
    traded_market_ids: set[str] | None = None,
    downloaded_position_ids: set[str] | None = None,
    api_downloaded_position_ids: set[str] | None = None,
    traded_position_ids: set[str] | None = None,
    downloaded_trade_rows: int = 0,
    trade_pagination_complete: bool = False,
    activity_pagination_complete: bool = False,
    trade_row_verification: dict[str, Any] | None = None,
    official_traded_live: bool = False,
    fetch_complete: bool = False,
) -> dict[str, Any]:
    """Add independent market and exact outcome/asset coverage evidence."""
    enriched = dict(score)
    normalized_downloaded_markets = (
        {
            market
            for value in downloaded_market_ids
            if (market := normalize_market_id(value))
        }
        if downloaded_market_ids is not None
        else None
    )
    normalized_traded_markets = (
        {
            market
            for value in traded_market_ids
            if (market := normalize_market_id(value))
        }
        if traded_market_ids is not None
        else None
    )
    normalized_downloaded_positions = (
        {
            str(value).strip().lower()
            for value in downloaded_position_ids
            if market_from_trade_position_key(value)
        }
        if downloaded_position_ids is not None
        else None
    )
    normalized_api_positions = (
        {
            str(value).strip().lower()
            for value in api_downloaded_position_ids
            if market_from_trade_position_key(value)
        }
        if api_downloaded_position_ids is not None
        else normalized_downloaded_positions
    )
    normalized_traded_positions = (
        {
            str(value).strip().lower()
            for value in traded_position_ids
            if market_from_trade_position_key(value)
        }
        if traded_position_ids is not None
        else None
    )
    fetched_markets = (
        len(normalized_downloaded_markets)
        if normalized_downloaded_markets is not None
        else max(
            0,
            int(
                safe_float(
                    enriched.get("downloadedMarkets"),
                    safe_float(enriched.get("positions")),
                )
            ),
        )
    )
    official_count = (
        max(0, int(official_traded_markets))
        if official_traded_markets is not None
        else None
    )
    exact_market_sets_available = bool(
        normalized_downloaded_markets is not None
        and normalized_traded_markets is not None
    )
    exact_outcome_sets_available = bool(
        normalized_downloaded_positions is not None
        and normalized_api_positions is not None
        and normalized_traded_positions is not None
    )
    if exact_market_sets_available:
        assert normalized_downloaded_markets is not None
        assert normalized_traded_markets is not None
        matched_market_ids = normalized_downloaded_markets & normalized_traded_markets
        missing_market_ids = normalized_traded_markets - normalized_downloaded_markets
        extra_market_ids = normalized_downloaded_markets - normalized_traded_markets
        discovered_market_count: int | str = len(normalized_traded_markets)
        matched_market_count = len(matched_market_ids)
    else:
        matched_market_ids = set()
        missing_market_ids = set()
        extra_market_ids = set()
        discovered_market_count = ""
        matched_market_count = fetched_markets

    if exact_outcome_sets_available:
        assert normalized_downloaded_positions is not None
        assert normalized_api_positions is not None
        assert normalized_traded_positions is not None
        matched_position_ids = (
            normalized_downloaded_positions & normalized_traded_positions
        )
        missing_position_ids = (
            normalized_traded_positions - normalized_downloaded_positions
        )
        extra_position_ids = (
            normalized_downloaded_positions - normalized_traded_positions
        )
        api_matched_position_ids = (
            normalized_api_positions & normalized_traded_positions
        )
        api_missing_position_ids = (
            normalized_traded_positions - normalized_api_positions
        )
        discovered_outcome_count: int | str = len(normalized_traded_positions)
    else:
        matched_position_ids = set()
        missing_position_ids = set()
        extra_position_ids = set()
        api_matched_position_ids = set()
        api_missing_position_ids = set()
        discovered_outcome_count = ""

    count_matches = bool(
        exact_market_sets_available
        and official_count is not None
        and int(discovered_market_count) == official_count
    )
    row_verification = dict(trade_row_verification or {})
    row_verification_available = trade_row_verification is not None
    row_verification_status = str(
        row_verification.get("tradeRowVerificationStatus") or "unavailable"
    ).strip().lower()
    missing_trade_rows = max(
        0,
        int(safe_float(row_verification.get("missingTradeRows"), 0.0)),
    )
    extra_trade_rows = max(
        0,
        int(safe_float(row_verification.get("extraTradeRows"), 0.0)),
    )
    if official_count is None:
        coverage_status = "official-count-unavailable"
    elif not exact_market_sets_available:
        coverage_status = "count-only-unverified"
    elif not official_traded_live:
        coverage_status = "official-count-not-live"
    elif not count_matches:
        coverage_status = "official-count-mismatch"
    elif not trade_pagination_complete:
        coverage_status = "trade-pagination-incomplete"
    elif not activity_pagination_complete:
        coverage_status = "activity-pagination-incomplete"
    elif not row_verification_available:
        coverage_status = "trade-row-verification-unavailable"
    elif (
        row_verification_status != "verified"
        or missing_trade_rows
        or extra_trade_rows
    ):
        coverage_status = "trade-row-mismatch"
    elif missing_market_ids:
        coverage_status = "missing-markets"
    elif not exact_outcome_sets_available:
        coverage_status = "outcome-set-unavailable"
    elif missing_position_ids:
        coverage_status = "missing-outcomes"
    elif not fetch_complete:
        coverage_status = "fetch-not-complete"
    else:
        coverage_status = "verified"

    enriched["downloadedMarkets"] = fetched_markets
    for audit_field in (
        "snapshotStart", "snapshotEnd", "tradesRawRows", "activityRawRows",
        "logicalTradeRows", "matchedCoreRows", "activityOnlyRows",
        "tradesOnlyRows", "exactRepeatedRows", "valueDifferenceRows",
        "sideDifferenceRows", "onchainVerifiedRows", "verifiedTradeRows",
        "unresolvedTradeRows", "tradeVerificationStatus", "verificationReason",
    ):
        enriched[audit_field] = row_verification.get(audit_field, "")
    enriched["downloadedTradeRows"] = max(
        0,
        int(
            safe_float(
                row_verification.get("downloadedTradeRows"),
                downloaded_trade_rows,
            )
        ),
    )
    enriched["uniqueTradeRows"] = (
        max(0, int(safe_float(row_verification.get("uniqueTradeRows"))))
        if row_verification_available
        else ""
    )
    enriched["duplicateTradeRows"] = (
        max(0, int(safe_float(row_verification.get("duplicateTradeRows"))))
        if row_verification_available
        else ""
    )
    enriched["activityTradeRows"] = (
        max(0, int(safe_float(row_verification.get("activityTradeRows"))))
        if row_verification_available
        else ""
    )
    enriched["activityUniqueTradeRows"] = (
        max(0, int(safe_float(row_verification.get("activityUniqueTradeRows"))))
        if row_verification_available
        else ""
    )
    enriched["matchedTradeRows"] = (
        max(0, int(safe_float(row_verification.get("matchedTradeRows"))))
        if row_verification_available
        else ""
    )
    enriched["missingTradeRows"] = (
        missing_trade_rows if row_verification_available else ""
    )
    enriched["extraTradeRows"] = (
        extra_trade_rows if row_verification_available else ""
    )
    enriched["tradeRowCoveragePercent"] = (
        str(row_verification.get("tradeRowCoveragePercent") or "0.00%")
        if row_verification_available
        else ""
    )
    enriched["tradeRowVerificationStatus"] = (
        row_verification_status if row_verification_available else "unavailable"
    )
    enriched["discoveredTradeOutcomes"] = discovered_outcome_count
    enriched["apiMatchedTradeOutcomes"] = (
        len(api_matched_position_ids) if exact_outcome_sets_available else ""
    )
    enriched["apiMissingTradeOutcomes"] = (
        len(api_missing_position_ids) if exact_outcome_sets_available else ""
    )
    enriched["matchedTradeOutcomes"] = (
        len(matched_position_ids) if exact_outcome_sets_available else ""
    )
    enriched["missingTradeOutcomes"] = (
        len(missing_position_ids) if exact_outcome_sets_available else ""
    )
    enriched["extraDownloadedOutcomes"] = (
        len(extra_position_ids) if exact_outcome_sets_available else ""
    )
    enriched["discoveredTradeMarkets"] = discovered_market_count
    enriched["matchedTradeMarkets"] = matched_market_count
    enriched["missingTradeMarkets"] = (
        len(missing_market_ids) if exact_market_sets_available else ""
    )
    enriched["extraDownloadedMarkets"] = (
        len(extra_market_ids) if exact_market_sets_available else ""
    )
    enriched["polymarketTraded"] = "" if official_count is None else official_count
    enriched["tradePaginationComplete"] = bool(trade_pagination_complete)
    enriched["activityPaginationComplete"] = bool(activity_pagination_complete)
    enriched["coverageStatus"] = coverage_status
    enriched["missingOutcomeSample"] = "|".join(sorted(missing_position_ids)[:10])
    enriched["missingMarketSample"] = "|".join(sorted(missing_market_ids)[:10])
    enriched["verificationVersion"] = TRADE_SET_VERIFICATION_VERSION

    if exact_outcome_sets_available and int(discovered_outcome_count) > 0:
        outcome_denominator = int(discovered_outcome_count)
        api_outcome_percent = len(api_matched_position_ids) / outcome_denominator * 100.0
        outcome_percent = len(matched_position_ids) / outcome_denominator * 100.0
    else:
        api_outcome_percent = 0.0
        outcome_percent = 0.0
    enriched["apiOutcomeCoveragePercent"] = f"{api_outcome_percent:.2f}%"
    enriched["outcomeCoveragePercent"] = f"{outcome_percent:.2f}%"
    # Keep the user's original "how much did the API actually download?" column
    # honest: it is measured before cash-flow reconstruction. The separate
    # outcomeCoveragePercent reports the final set after successful repairs, and
    # marketCoveragePercent uses the official /traded market denominator.
    enriched["positionCoveragePercent"] = enriched["apiOutcomeCoveragePercent"]

    if official_count is not None and official_count > 0:
        market_coverage_percent = matched_market_count / official_count * 100.0
        per_trade_score = (
            safe_float(enriched.get("oneShareNetPnlAfterCosts")) / official_count
        )
        enriched["marketCoveragePercent"] = f"{market_coverage_percent:.2f}%"
        enriched["oneShareNetPnlAfterCostsPerTrade"] = per_trade_score
    else:
        # Division by zero (or a fully unavailable /traded response) has no
        # meaningful ratio. A numeric zero keeps deterministic sorting; an empty
        # polymarketTraded cell separately identifies an unavailable response.
        enriched["marketCoveragePercent"] = "0.00%"
        enriched["oneShareNetPnlAfterCostsPerTrade"] = 0.0
    return enriched


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
    refresh_since_ms: int = 0,
    test_memory_file_name: str = TEST_MEMORY_FILE_NAME,
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
    memory_path = out_dir / str(test_memory_file_name or TEST_MEMORY_FILE_NAME)
    page_cache_path = out_dir / CLOSED_POSITION_PAGE_CACHE_FILE_NAME
    secondary_page_cache_path = out_dir / SECONDARY_CLOSED_POSITION_PAGE_CACHE_FILE_NAME
    universe_path = out_dir / "wallet_universe.csv"
    complete_fetch_db_path = out_dir / COMPLETE_FETCH_CACHE_DB_FILE_NAME
    position_summary_path = out_dir / POSITION_COMPLETENESS_LOG_FILE_NAME

    ranked_wallets = (
        list(wallets.values())
        if preserve_wallet_order
        else sorted(wallets.values(), key=lambda item: item.best_pnl, reverse=True)
    )
    if (
        not FULL_WALLET_INCLUSION_MODE
        and FILTER_MAX_WALLETS_TO_SCORE
        and max_wallets
    ):
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
        score_sources.extend(
            fallback_out_dir / name for name in LEGACY_SCORE_JOURNAL_FILE_NAMES
        )
        score_sources.append(fallback_out_dir / SCORE_JOURNAL_FILE_NAME)
    score_sources.append(progress_path)
    score_sources.extend(out_dir / name for name in LEGACY_SCORE_JOURNAL_FILE_NAMES)
    score_sources.append(score_journal_path)
    for source in score_sources:
        for row in load_progress_scores(source):
            wallet = str(row.get("proxyWallet") or "").lower()
            if wallet and wallet in shard_wallet_set:
                score_by_wallet[wallet] = row

    not_saved_reasons_by_wallet: dict[str, dict[str, Any]] = {}
    not_saved_reason_stats: dict[str, dict[str, Any]] = {}
    memory_statuses = load_test_memory_statuses(
        memory_path,
        min_tested_at_ms=refresh_since_ms,
    )
    if fallback_out_dir is not None:
        for fallback_memory in test_memory_paths_for_directory(fallback_out_dir):
            for wallet, status_row in load_test_memory_statuses(
                fallback_memory,
                min_tested_at_ms=refresh_since_ms,
            ).items():
                previous = memory_statuses.get(wallet)
                if previous is None or status_row[1] >= previous[1]:
                    memory_statuses[wallet] = status_row
    tested_wallets = set(memory_statuses)
    filtered_wallets_to_purge = {
        wallet
        for wallet, (status, _tested_at) in memory_statuses.items()
        if status == "filtered"
    }
    filtered_wallets_to_purge &= shard_wallet_set
    tested_wallets &= shard_wallet_set
    # Only v58 reconciled row-multiset and outcome evidence is authoritative. Older
    # rows can contain false-100% values and must not remain visible while those
    # wallets are waiting for exact re-verification.
    current_scored_wallets = {
        wallet
        for wallet, (status, _tested_at) in memory_statuses.items()
        if status == "scored"
    }
    score_by_wallet = {
        wallet: row
        for wallet, row in score_by_wallet.items()
        if wallet in current_scored_wallets
    }
    if tested_wallets:
        print(
            f"[resume] loaded persistent memory for {len(tested_wallets)} "
            f"shard wallets reset_after_ms={refresh_since_ms}",
            flush=True,
        )

    cached_positions = load_closed_positions_cache(
        raw_path,
        min_fetched_at_ms=refresh_since_ms,
    )
    if fallback_out_dir is not None:
        fallback_cached_positions = load_closed_positions_cache(
            fallback_out_dir / RAW_CLOSED_POSITIONS_LOG_FILE_NAME,
            min_fetched_at_ms=refresh_since_ms,
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
        secondary_cached_positions = load_closed_positions_cache(
            secondary_raw_path,
            min_fetched_at_ms=refresh_since_ms,
        )
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
        TEST_MEMORY_FIELDNAMES,
    )
    score_journal_file, score_journal_writer = open_csv_append(
        score_journal_path,
        score_fieldnames,
    )
    consecutive_fetch_failures = 0
    fetch_failed_wallets: set[str] = set()

    def drop_stale_score_for_filtered_wallet(wallet: str) -> None:
        score_by_wallet.pop(wallet, None)
        if refresh_since_ms > 0:
            # Rewrite the compact CSV so a filtered wallet cannot keep an older-cycle
            # score visible if the worker stops shortly after this decision.
            write_sorted_scores_csv(
                score_by_wallet.values(),
                progress_path,
                score_fieldnames,
            )

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
                        reason="already completed in persistent wallet_test_memory",
                    )
                continue

            snapshot_start = int(time.time())
            wallet_snapshot_end = max(1, snapshot_start - SNAPSHOT_FINALITY_LAG_SECONDS)
            print(
                f"[closed] {index}/{len(ranked_wallets)} {seed.user_name} "
                f"{seed.proxy_wallet} snapshot=1..{wallet_snapshot_end} "
                f"finality_lag={SNAPSHOT_FINALITY_LAG_SECONDS}s",
                flush=True,
            )
            fetch_metadata: dict[str, Any] = {}
            # v58 always refreshes reconciled trade multiplicity plus market/outcome evidence before
            # accepting a wallet.
            # Raw JSONL remains a durable backup, but cannot prove that no trade was
            # added after its timestamp. SQLite still reuses every exact market row.
            loaded_from_complete_raw_cache = False
            if loaded_from_complete_raw_cache:
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
                        refresh_token=refresh_since_ms,
                    )
                except Exception as exc:
                    fetch_failed_wallets.add(seed.proxy_wallet)
                    consecutive_fetch_failures += 1
                    print(
                        f"[fetch-failed] wallet={seed.proxy_wallet} "
                        f"error={type(exc).__name__}: {exc}",
                        flush=True,
                    )
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
            # Take a final maker+taker market+outcome snapshot and compare its
            # market count to live /traded. A second pass closes the race where
            # the wallet trades while the first snapshot is being finalized.
            traded_market_ids: set[str] = set()
            traded_position_ids: set[str] = set()
            downloaded_trade_rows = 0
            traded_count = 0
            official_traded_live = False
            official_traded_source = ""
            trade_snapshot_end = wallet_snapshot_end
            for verification_pass in range(1, 3):
                (
                    traded_market_ids,
                    traded_position_ids,
                    downloaded_trade_rows,
                    trade_snapshot_end,
                    _new_trade_ids,
                    _touched_trade_ids,
                ) = get_complete_activity_markets(
                    client,
                    seed.proxy_wallet,
                    complete_fetch_cache,
                    snapshot_end=trade_snapshot_end,
                )
                (
                    traded_count,
                    official_traded_live,
                    official_traded_source,
                ) = fetch_official_traded_count(
                    client,
                    seed.proxy_wallet,
                    complete_fetch_cache,
                    force_refresh=True,
                )
                if official_traded_live and len(traded_market_ids) == max(
                    0, int(traded_count)
                ):
                    break
                print(
                    f"    [trade-set-final:retry] {seed.proxy_wallet} "
                    f"pass={verification_pass}/2 discovered={len(traded_market_ids)} "
                    f"official={traded_count} live={official_traded_live}",
                    flush=True,
                )
            if REQUIRE_LIVE_OFFICIAL_TRADED_FOR_MEMORY and not official_traded_live:
                complete_fetch_cache.close()
                raise WorkerRetryRequired(
                    f"live Polymarket /traded verification unavailable for "
                    f"{seed.proxy_wallet}; source={official_traded_source}; "
                    "downloaded positions are checkpointed and only verification will retry"
                )
            official_traded_markets: int | None = (
                max(0, int(traded_count))
                if official_traded_source != "unavailable"
                else None
            )
            primary_trade_occurrences = (
                complete_fetch_cache.get_activity_trade_occurrences(
                    seed.proxy_wallet
                )
            )
            activity_pagination_complete = False
            try:
                independent_trade_occurrences = (
                    fetch_independent_activity_trade_occurrences(
                        client,
                        seed.proxy_wallet,
                        trade_snapshot_end,
                    )
                )
                activity_pagination_complete = True
                trade_row_verification = compare_trade_occurrence_multisets(
                    primary_trade_occurrences,
                    independent_trade_occurrences,
                )
                trade_row_verification["snapshotStart"] = snapshot_start
                trade_row_verification["snapshotEnd"] = trade_snapshot_end
            except Exception as exc:
                trade_row_verification = {
                    "downloadedTradeRows": sum(
                        max(0, int(payload[2]))
                        for payload in primary_trade_occurrences.values()
                    ),
                    "uniqueTradeRows": len(primary_trade_occurrences),
                    "duplicateTradeRows": max(
                        0,
                        sum(
                            max(0, int(payload[2]))
                            for payload in primary_trade_occurrences.values()
                        )
                        - len(primary_trade_occurrences),
                    ),
                    "activityTradeRows": 0,
                    "activityUniqueTradeRows": 0,
                    "matchedTradeRows": 0,
                    "missingTradeRows": 0,
                    "extraTradeRows": 0,
                    "tradeRowCoveragePercent": "0.00%",
                    "tradeRowVerificationStatus": "unavailable",
                }
                print(
                    f"    [activity-verify:failed] {seed.proxy_wallet} "
                    f"error={type(exc).__name__}: {exc}",
                    flush=True,
                )
            print(
                f"    [trade-row-verify] {seed.proxy_wallet} "
                f"trades_raw={trade_row_verification['downloadedTradeRows']} "
                f"trades_unique={trade_row_verification['uniqueTradeRows']} "
                f"duplicates={trade_row_verification['duplicateTradeRows']} "
                f"activity_raw={trade_row_verification['activityTradeRows']} "
                f"matched={trade_row_verification['matchedTradeRows']} "
                f"missing={trade_row_verification['missingTradeRows']} "
                f"extra={trade_row_verification['extraTradeRows']} "
                f"coverage={trade_row_verification['tradeRowCoveragePercent']} "
                f"status={trade_row_verification['tradeRowVerificationStatus']}",
                flush=True,
            )
            if (
                activity_pagination_complete
                and str(
                    trade_row_verification.get("tradeRowVerificationStatus")
                    or ""
                ).strip().lower()
                != "verified"
            ):
                complete_fetch_cache.reset_activity_discovery(seed.proxy_wallet)
                print(
                    f"    [trade-row-verify:retry-armed] {seed.proxy_wallet} "
                    "unverified primary trade snapshot cleared; exact closed-market "
                    "cache preserved for the next VPN retry",
                    flush=True,
                )
            fetch_complete = bool(
                fetch_metadata.get("complete")
                if fetch_metadata
                else loaded_from_complete_raw_cache
            )
            # Repair a market added since the main fetch (or a raw checkpoint that
            # predates exact verification), then score the repaired rows.
            positions, final_repair_metadata = complete_positions_for_trade_markets(
                client,
                seed.proxy_wallet,
                positions,
                traded_market_ids,
                traded_position_ids,
                snapshot_end=trade_snapshot_end,
            )
            fetch_complete = bool(
                fetch_complete
                and final_repair_metadata.get("tradeSetComplete")
                and final_repair_metadata.get("outcomeSetComplete")
            )
            score = score_positions(positions, smoothing=smoothing)
            downloaded_market_ids = position_market_ids(positions)
            score = add_polymarket_trade_metrics(
                score,
                official_traded_markets,
                downloaded_market_ids=downloaded_market_ids,
                traded_market_ids=traded_market_ids,
                downloaded_position_ids=position_trade_keys(positions),
                api_downloaded_position_ids=position_trade_keys(
                    positions,
                    include_reconstructed=False,
                ),
                traded_position_ids=traded_position_ids,
                downloaded_trade_rows=downloaded_trade_rows,
                trade_pagination_complete=True,
                activity_pagination_complete=activity_pagination_complete,
                trade_row_verification=trade_row_verification,
                official_traded_live=official_traded_live,
                fetch_complete=fetch_complete,
            )
            final_fetch_metadata = {
                **fetch_metadata,
                **final_repair_metadata,
                "officialTradedMarkets": official_traded_markets,
                "officialTradedSource": official_traded_source,
                "officialTradedLive": official_traded_live,
                "downloadedTradeRows": downloaded_trade_rows,
                "tradePaginationComplete": True,
                "activityPaginationComplete": activity_pagination_complete,
                **trade_row_verification,
                "coverageStatus": score["coverageStatus"],
                "verificationVersion": TRADE_SET_VERIFICATION_VERSION,
            }
            append_position_completeness_summary(
                position_summary_path,
                seed.proxy_wallet,
                score,
                fetch_complete=fetch_complete,
                official_traded_live=official_traded_live,
            )
            raw_file.write(
                json.dumps(
                    {
                        "proxyWallet": seed.proxy_wallet,
                        "positions": positions,
                        "complete": score["coverageStatus"] == "verified",
                        "fetchVersion": COMPLETE_FETCH_VERSION,
                        "fetchedAt": epoch_milliseconds(),
                        "fetchMetadata": final_fetch_metadata,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            raw_file.flush()
            if score["coverageStatus"] == "verified":
                cached_positions[seed.proxy_wallet] = positions
            print(
                f"    [memory:coverage] {seed.proxy_wallet} "
                f"downloaded_positions={score['positions']} "
                f"downloaded_markets={score['downloadedMarkets']} "
                f"trade_rows={score['downloadedTradeRows']} "
                f"unique_trade_rows={score['uniqueTradeRows']} "
                f"duplicate_trade_rows={score['duplicateTradeRows']} "
                f"trade_row_match={score['matchedTradeRows']}/"
                f"{score['activityTradeRows']} "
                f"trade_row_coverage={score['tradeRowCoveragePercent']} "
                f"markets={score['matchedTradeMarkets']}/"
                f"{score['discoveredTradeMarkets']} "
                f"outcomes={score['matchedTradeOutcomes']}/"
                f"{score['discoveredTradeOutcomes']} "
                f"api_outcomes={score['apiMatchedTradeOutcomes']}/"
                f"{score['discoveredTradeOutcomes']} "
                f"missing_outcomes={score['missingTradeOutcomes']} "
                f"extra_outcomes={score['extraDownloadedOutcomes']} "
                f"polymarket_traded="
                f"{score['polymarketTraded'] if score['polymarketTraded'] != '' else 'unavailable'} "
                f"coverage={score['positionCoveragePercent']} "
                f"one_share_per_trade={score['oneShareNetPnlAfterCostsPerTrade']:.12g} "
                f"source={official_traded_source or 'unknown'} "
                f"status={score['coverageStatus']} live={official_traded_live} "
                f"fetch_complete={fetch_complete}",
                flush=True,
            )
            if score["coverageStatus"] != "verified":
                reason = (
                    f"exact trade-outcome verification failed: "
                    f"status={score['coverageStatus']} "
                    f"discovered={score['discoveredTradeMarkets']} "
                    f"official={score['polymarketTraded']} "
                    f"matched={score['matchedTradeMarkets']} "
                    f"missing={score['missingTradeMarkets']} "
                    f"extra={score['extraDownloadedMarkets']} "
                    f"missing_trade_rows={score['missingTradeRows']} "
                    f"extra_trade_rows={score['extraTradeRows']} "
                    f"missing_outcomes={score['missingTradeOutcomes']} "
                    f"extra_outcomes={score['extraDownloadedOutcomes']} "
                    f"sample={score['missingOutcomeSample'] or score['missingMarketSample']}"
                )
                fetch_failed_wallets.add(seed.proxy_wallet)
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="incomplete",
                    reason=reason,
                    score=score,
                    official_traded_source=official_traded_source,
                    official_traded_live=official_traded_live,
                    fetch_complete=fetch_complete,
                )
                fail_writer.writerow(
                    {"proxyWallet": seed.proxy_wallet, "error": reason}
                )
                fail_file.flush()
                write_not_saved_reason(
                    not_saved_reasons_by_wallet,
                    not_saved_reason_stats,
                    not_saved_reasons_path,
                    not_saved_reason_stats_path,
                    seed,
                    status="incomplete",
                    reason=reason,
                    score=score,
                )
                print(
                    f"    [memory:incomplete] {seed.proxy_wallet} {reason}",
                    flush=True,
                )
                write_live_score_outputs(
                    score_by_wallet.values(), score_path, out_dir, score_fieldnames
                )
                continue
            if (
                not FULL_WALLET_INCLUSION_MODE
                and FILTER_MIN_RESOLVED_POSITIONS
                and score["resolvedPositions"] < min_positions
            ):
                drop_stale_score_for_filtered_wallet(seed.proxy_wallet)
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=f"resolvedPositions {score['resolvedPositions']} < {min_positions}",
                    score=score,
                    official_traded_source=official_traded_source,
                    official_traded_live=official_traded_live,
                    fetch_complete=fetch_complete,
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
            if (
                not FULL_WALLET_INCLUSION_MODE
                and FILTER_MIN_LOSING_POSITIONS
                and score["losses"] < min_losses
            ):
                drop_stale_score_for_filtered_wallet(seed.proxy_wallet)
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=f"losses {score['losses']} < {min_losses}",
                    score=score,
                    official_traded_source=official_traded_source,
                    official_traded_live=official_traded_live,
                    fetch_complete=fetch_complete,
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
            if (
                not FULL_WALLET_INCLUSION_MODE
                and FILTER_MIN_CLOSED_REALIZED_PNL
                and score["realizedPnlAfterCosts"] < min_pnl
            ):
                drop_stale_score_for_filtered_wallet(seed.proxy_wallet)
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=f"realizedPnlAfterCosts {score['realizedPnlAfterCosts']} < {min_pnl}",
                    score=score,
                    official_traded_source=official_traded_source,
                    official_traded_live=official_traded_live,
                    fetch_complete=fetch_complete,
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
                drop_stale_score_for_filtered_wallet(seed.proxy_wallet)
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=filter_reason,
                    score=score,
                    official_traded_source=official_traded_source,
                    official_traded_live=official_traded_live,
                    fetch_complete=fetch_complete,
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
                score=score,
                official_traded_source=official_traded_source,
                official_traded_live=official_traded_live,
                fetch_complete=fetch_complete,
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
    if (
        not FULL_WALLET_INCLUSION_MODE
        and PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS
    ):
        rewrite_closed_positions_cache(raw_path, cached_positions)
        rewrite_closed_position_page_cache(page_cache_path, page_cache)
    # در اجرای shard، فایل universe باید ثابت بماند؛ تغییر تعداد/ترتیب ردیف‌ها باعث
    # عوض‌شدن modulo و جابه‌جایی والت‌ها در اجرای Failover می‌شود.
    if (
        not FULL_WALLET_INCLUSION_MODE
        and PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE
        and shard_count == 1
        and filtered_wallets_to_purge
    ):
        rewrite_wallet_universe_without_wallets(universe_path, filtered_wallets_to_purge)
    complete_fetch_cache.close()


def mode_2_filter_reason(score: dict[str, Any]) -> str:
    if FULL_WALLET_INCLUSION_MODE:
        return ""
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


def load_closed_positions_cache(
    raw_path: Path,
    min_fetched_at_ms: int = 0,
) -> dict[str, list[dict[str, Any]]]:
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
            fetched_at_ms = normalize_epoch_milliseconds(row.get("fetchedAt"))
            fresh_enough = (
                min_fetched_at_ms <= 0 or fetched_at_ms >= int(min_fetched_at_ms)
            )
            trusted = (
                complete
                and fetch_version == COMPLETE_FETCH_VERSION
                and fresh_enough
            )
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
            file.write(json.dumps({"proxyWallet": wallet, "positions": positions, "complete": True, "fetchVersion": COMPLETE_FETCH_VERSION, "fetchedAt": epoch_milliseconds()}, ensure_ascii=False) + "\n")


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
    if FULL_WALLET_INCLUSION_MODE:
        return
    filtered_wallets_to_purge.add(wallet)
    if PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS:
        cached_positions.pop(wallet, None)
        page_cache.pop(wallet, None)


def load_test_memory_statuses(
    memory_path: Path,
    min_tested_at_ms: int = 0,
) -> dict[str, tuple[str, int]]:
    latest: dict[str, tuple[str, int]] = {}
    if not memory_path.exists():
        return latest
    with memory_path.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            wallet = str(row.get("proxyWallet") or "").lower()
            status = str(row.get("status") or "").lower()
            tested_at_ms = normalize_epoch_milliseconds(row.get("testedAt"))
            if not wallet or status not in {"scored", "filtered"}:
                continue
            if not test_memory_row_is_exactly_verified(row):
                continue
            if FULL_WALLET_INCLUSION_MODE and status == "filtered":
                # A legacy filter decision cannot be authoritative after filters
                # were globally disabled; the wallet must be scored once in full.
                continue
            if min_tested_at_ms > 0 and tested_at_ms < int(min_tested_at_ms):
                continue
            previous = latest.get(wallet)
            if previous is None or tested_at_ms >= previous[1]:
                latest[wallet] = (status, tested_at_ms)
    return latest


def _csv_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def test_memory_row_is_exactly_verified(row: dict[str, Any]) -> bool:
    """Only current exact-set evidence is authoritative for Resume/Done."""
    status = str(row.get("status") or "").strip().lower()
    if status not in {"scored", "filtered"}:
        return False
    if str(row.get("verificationVersion") or "").strip() != TRADE_SET_VERIFICATION_VERSION:
        return False
    snapshot_start = int(safe_float(row.get("snapshotStart"), 0.0))
    snapshot_end = int(safe_float(row.get("snapshotEnd"), 0.0))
    if snapshot_start <= 0 or snapshot_end <= 0 or snapshot_end > snapshot_start:
        return False
    logical_trade_rows = int(safe_float(row.get("logicalTradeRows"), -1.0))
    verified_trade_rows = int(safe_float(row.get("verifiedTradeRows"), -2.0))
    unresolved_trade_rows = int(safe_float(row.get("unresolvedTradeRows"), -1.0))
    if (
        logical_trade_rows < 0
        or verified_trade_rows != logical_trade_rows
        or unresolved_trade_rows != 0
        or str(row.get("tradeVerificationStatus") or "").strip().lower()
        not in {"verified_api", "verified_api_value_differences", "verified_onchain"}
    ):
        return False
    if str(row.get("coverageStatus") or "").strip().lower() != "verified":
        return False
    if int(safe_float(row.get("missingTradeMarkets"), -1.0)) != 0:
        return False
    if int(safe_float(row.get("missingTradeOutcomes"), -1.0)) != 0:
        return False
    if int(safe_float(row.get("missingTradeRows"), -1.0)) != 0:
        return False
    if int(safe_float(row.get("extraTradeRows"), -1.0)) != 0:
        return False
    if (
        str(row.get("tradeRowVerificationStatus") or "").strip().lower()
        != "verified"
    ):
        return False
    downloaded_trade_rows = int(
        safe_float(row.get("downloadedTradeRows"), -1.0)
    )
    activity_trade_rows = int(
        safe_float(row.get("activityTradeRows"), -2.0)
    )
    matched_trade_rows = int(
        safe_float(row.get("matchedTradeRows"), -3.0)
    )
    if (
        downloaded_trade_rows < 0
        or downloaded_trade_rows != activity_trade_rows
        or matched_trade_rows != activity_trade_rows
    ):
        return False
    discovered = int(safe_float(row.get("discoveredTradeMarkets"), -1.0))
    official = int(safe_float(row.get("polymarketTraded"), -2.0))
    matched = int(safe_float(row.get("matchedTradeMarkets"), -1.0))
    if discovered < 0 or official < 0 or matched < 0:
        return False
    if discovered != official or matched != official:
        return False
    discovered_outcomes = int(
        safe_float(row.get("discoveredTradeOutcomes"), -1.0)
    )
    matched_outcomes = int(
        safe_float(row.get("matchedTradeOutcomes"), -2.0)
    )
    if discovered_outcomes < 0 or matched_outcomes != discovered_outcomes:
        return False
    return bool(
        _csv_truthy(row.get("officialTradedLive"))
        and _csv_truthy(row.get("tradePaginationComplete"))
        and _csv_truthy(row.get("activityPaginationComplete"))
        and _csv_truthy(row.get("fetchComplete"))
    )


def load_test_memory(memory_path: Path, min_tested_at_ms: int = 0) -> set[str]:
    return set(load_test_memory_statuses(memory_path, min_tested_at_ms))


def test_memory_quality_summary(
    memory_path: Path,
    min_tested_at_ms: int = 0,
) -> dict[str, int]:
    """Count schema-complete verification rows versus migrated legacy rows."""
    summary = {
        "rows": 0,
        "verified_complete": 0,
        "missing_verification_fields": 0,
    }
    if not memory_path.exists():
        return summary
    try:
        with memory_path.open("r", encoding="utf-8-sig", newline="") as file:
            for row in csv.DictReader(file):
                wallet = str(row.get("proxyWallet") or "").strip().lower()
                status = str(row.get("status") or "").strip().lower()
                tested_at_ms = normalize_epoch_milliseconds(row.get("testedAt"))
                if not wallet or status not in {"scored", "filtered", "incomplete"}:
                    continue
                if min_tested_at_ms > 0 and tested_at_ms < int(min_tested_at_ms):
                    continue
                summary["rows"] += 1
                complete = test_memory_row_is_exactly_verified(row)
                if complete:
                    summary["verified_complete"] += 1
                else:
                    summary["missing_verification_fields"] += 1
    except (OSError, csv.Error):
        return summary
    return summary


def test_memory_paths_for_directory(directory: Path) -> list[Path]:
    """Return new worker memory first, followed by the legacy/user-facing file."""
    candidates = [
        directory / WORKER_TEST_MEMORY_FILE_NAME,
        *(directory / name for name in LEGACY_WORKER_TEST_MEMORY_FILE_NAMES),
        directory / TEST_MEMORY_FILE_NAME,
    ]
    result: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = os.path.normcase(os.path.abspath(str(path)))
        if key not in seen:
            seen.add(key)
            result.append(path)
    return result


def load_test_memory_from_directory(
    directory: Path,
    min_tested_at_ms: int = 0,
) -> set[str]:
    completed: set[str] = set()
    for path in test_memory_paths_for_directory(directory):
        completed |= load_test_memory(path, min_tested_at_ms=min_tested_at_ms)
    return completed


def _test_memory_row_quality(row: dict[str, Any]) -> tuple[int, int]:
    populated = sum(
        1
        for key in TEST_MEMORY_FIELDNAMES
        if str(row.get(key) or "").strip()
    )
    verified = int(test_memory_row_is_exactly_verified(row))
    live = int(_csv_truthy(row.get("officialTradedLive")))
    return verified * 2 + live, populated


def merge_test_memory_files(
    sources: list[Path],
    destination: Path,
    *,
    min_tested_at_ms: int = 0,
) -> int:
    """Compact every memory source into one latest, schema-complete row per wallet."""
    latest: dict[str, tuple[int, dict[str, Any]]] = {}
    normalized_destination = os.path.normcase(os.path.abspath(str(destination)))
    unique_sources: list[Path] = []
    seen_sources: set[str] = set()
    for source in sources:
        key = os.path.normcase(os.path.abspath(str(source)))
        if key not in seen_sources:
            seen_sources.add(key)
            unique_sources.append(source)
    # Reading the destination first lets a live compact operation preserve rows
    # already mirrored from workers that are no longer in the current source list.
    unique_sources.sort(
        key=lambda value: 0
        if os.path.normcase(os.path.abspath(str(value))) == normalized_destination
        else 1
    )
    for source in unique_sources:
        if not source.exists() or source.stat().st_size <= 0:
            continue
        try:
            with source.open("r", encoding="utf-8-sig", newline="") as file:
                for row in csv.DictReader(file):
                    wallet = str(row.get("proxyWallet") or "").strip().lower()
                    status = str(row.get("status") or "").strip().lower()
                    tested_at_ms = normalize_epoch_milliseconds(row.get("testedAt"))
                    if not wallet or status not in {"scored", "filtered", "incomplete"}:
                        continue
                    if FULL_WALLET_INCLUSION_MODE and status == "filtered":
                        continue
                    if min_tested_at_ms > 0 and tested_at_ms < int(min_tested_at_ms):
                        continue
                    normalized = {
                        key: row.get(key, "") for key in TEST_MEMORY_FIELDNAMES
                    }
                    normalized["proxyWallet"] = wallet
                    normalized["status"] = status
                    normalized["testedAt"] = tested_at_ms
                    previous = latest.get(wallet)
                    if previous is None or tested_at_ms > previous[0] or (
                        tested_at_ms == previous[0]
                        and _test_memory_row_quality(normalized)
                        > _test_memory_row_quality(previous[1])
                    ):
                        latest[wallet] = (tested_at_ms, normalized)
        except (OSError, csv.Error):
            # A Worker can be flushing its final line while the Manager mirrors
            # memory. The next five-second sync will read it again.
            continue
    ordered_rows = [
        row
        for _tested_at, row in sorted(
            latest.values(),
            key=lambda item: (item[0], str(item[1].get("proxyWallet") or "")),
        )
    ]
    _write_csv_rows_replace_safe(
        destination,
        TEST_MEMORY_FIELDNAMES,
        ordered_rows,
        operation="memory-compact",
    )
    return len(ordered_rows)


def initialize_persistent_test_memory(root: Path) -> dict[str, Any]:
    """Initialize resume state; deleting the visible CSV creates a new epoch."""
    memory_path = root / TEST_MEMORY_FILE_NAME
    state_path = root / TEST_MEMORY_STATE_FILE_NAME
    state_exists = state_path.exists()
    state: dict[str, Any] = {}
    if state_exists:
        try:
            loaded = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                state = loaded
        except (OSError, json.JSONDecodeError):
            state = {}

    previous_schema_version = int(safe_float(state.get("schemaVersion"), 0.0))
    legacy_state_migration = bool(
        state_exists and previous_schema_version < TEST_MEMORY_SCHEMA_VERSION
    )
    reset_after_ms = normalize_epoch_milliseconds(state.get("resetAfterMs"))
    # Older builds had no durable proof that the visible CSV had ever been
    # created successfully.  Treating a missing file during that migration as
    # a deliberate deletion caused the false reset_requested=True seen in the
    # user's v56 log.  From schema v6 onward the explicit marker arms deletion
    # as the only reset signal.
    deletion_reset_armed = bool(
        previous_schema_version >= TEST_MEMORY_SCHEMA_VERSION
        and state.get("visibleMemoryExpected") is True
    )
    reset_requested = bool(
        state_exists and deletion_reset_armed and not memory_path.exists()
    )
    first_migration = bool(not state_exists or legacy_state_migration)
    if legacy_state_migration:
        reset_after_ms = 0
    if reset_requested:
        reset_after_ms = epoch_milliseconds()

    # Migrate an existing v52 CSV in place, or create the visible header before
    # recording state. This ordering prevents a crash from looking like deletion.
    memory_file, _memory_writer = open_csv_append(
        memory_path,
        TEST_MEMORY_FIELDNAMES,
    )
    memory_file.close()

    now_ms = epoch_milliseconds()
    new_state = {
        "schemaVersion": TEST_MEMORY_SCHEMA_VERSION,
        "resetAfterMs": reset_after_ms,
        "initializedAt": normalize_epoch_milliseconds(state.get("initializedAt"))
        or now_ms,
        "updatedAt": now_ms,
        "visibleMemoryExpected": True,
    }
    _atomic_write_text(
        state_path,
        json.dumps(new_state, ensure_ascii=False, indent=2) + "\n",
    )
    return {
        "memory_path": memory_path,
        "state_path": state_path,
        "reset_after_ms": reset_after_ms,
        "reset_requested": reset_requested,
        "first_migration": first_migration,
        "legacy_state_migration": legacy_state_migration,
    }


def load_filtered_wallets_from_memory(
    memory_path: Path,
    min_tested_at_ms: int = 0,
) -> set[str]:
    return {
        wallet
        for wallet, (status, _tested_at) in load_test_memory_statuses(
            memory_path,
            min_tested_at_ms,
        ).items()
        if status == "filtered"
    }


def write_test_memory_row(
    writer: csv.DictWriter,
    file,
    seed: WalletSeed,
    status: str,
    reason: str,
    *,
    score: dict[str, Any] | None = None,
    official_traded_source: str = "",
    official_traded_live: bool = False,
    fetch_complete: bool = False,
) -> None:
    score = score or {}
    writer.writerow(
        {
            "proxyWallet": seed.proxy_wallet,
            "userName": seed.user_name,
            "status": status,
            "reason": reason,
            "snapshotStart": score.get("snapshotStart", ""),
            "snapshotEnd": score.get("snapshotEnd", ""),
            "tradesRawRows": score.get("tradesRawRows", ""),
            "activityRawRows": score.get("activityRawRows", ""),
            "logicalTradeRows": score.get("logicalTradeRows", ""),
            "matchedCoreRows": score.get("matchedCoreRows", ""),
            "activityOnlyRows": score.get("activityOnlyRows", ""),
            "tradesOnlyRows": score.get("tradesOnlyRows", ""),
            "exactRepeatedRows": score.get("exactRepeatedRows", ""),
            "valueDifferenceRows": score.get("valueDifferenceRows", ""),
            "sideDifferenceRows": score.get("sideDifferenceRows", ""),
            "onchainVerifiedRows": score.get("onchainVerifiedRows", ""),
            "verifiedTradeRows": score.get("verifiedTradeRows", ""),
            "unresolvedTradeRows": score.get("unresolvedTradeRows", ""),
            "tradeVerificationStatus": score.get("tradeVerificationStatus", ""),
            "verificationReason": score.get("verificationReason", ""),
            "downloadedPositions": score.get("positions", ""),
            "downloadedMarkets": score.get("downloadedMarkets", ""),
            "downloadedTradeRows": score.get("downloadedTradeRows", ""),
            "uniqueTradeRows": score.get("uniqueTradeRows", ""),
            "duplicateTradeRows": score.get("duplicateTradeRows", ""),
            "activityTradeRows": score.get("activityTradeRows", ""),
            "activityUniqueTradeRows": score.get("activityUniqueTradeRows", ""),
            "matchedTradeRows": score.get("matchedTradeRows", ""),
            "missingTradeRows": score.get("missingTradeRows", ""),
            "extraTradeRows": score.get("extraTradeRows", ""),
            "tradeRowCoveragePercent": score.get("tradeRowCoveragePercent", ""),
            "tradeRowVerificationStatus": score.get(
                "tradeRowVerificationStatus", ""
            ),
            "discoveredTradeOutcomes": score.get("discoveredTradeOutcomes", ""),
            "apiMatchedTradeOutcomes": score.get("apiMatchedTradeOutcomes", ""),
            "apiMissingTradeOutcomes": score.get("apiMissingTradeOutcomes", ""),
            "apiOutcomeCoveragePercent": score.get("apiOutcomeCoveragePercent", ""),
            "matchedTradeOutcomes": score.get("matchedTradeOutcomes", ""),
            "missingTradeOutcomes": score.get("missingTradeOutcomes", ""),
            "extraDownloadedOutcomes": score.get("extraDownloadedOutcomes", ""),
            "outcomeCoveragePercent": score.get("outcomeCoveragePercent", ""),
            "discoveredTradeMarkets": score.get("discoveredTradeMarkets", ""),
            "matchedTradeMarkets": score.get("matchedTradeMarkets", ""),
            "missingTradeMarkets": score.get("missingTradeMarkets", ""),
            "extraDownloadedMarkets": score.get("extraDownloadedMarkets", ""),
            "polymarketTraded": score.get("polymarketTraded", ""),
            "marketCoveragePercent": score.get("marketCoveragePercent", ""),
            "positionCoveragePercent": score.get("positionCoveragePercent", ""),
            "tradePaginationComplete": score.get("tradePaginationComplete", ""),
            "activityPaginationComplete": score.get(
                "activityPaginationComplete", ""
            ),
            "coverageStatus": score.get("coverageStatus", ""),
            "missingOutcomeSample": score.get("missingOutcomeSample", ""),
            "missingMarketSample": score.get("missingMarketSample", ""),
            "verificationVersion": score.get(
                "verificationVersion", TRADE_SET_VERIFICATION_VERSION
            ),
            "officialTradedSource": official_traded_source,
            "officialTradedLive": bool(official_traded_live),
            "fetchComplete": bool(fetch_complete),
            "testedAt": epoch_milliseconds(),
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
        "downloadedMarkets",
        "downloadedTradeRows",
        "uniqueTradeRows",
        "duplicateTradeRows",
        "activityTradeRows",
        "activityUniqueTradeRows",
        "matchedTradeRows",
        "missingTradeRows",
        "extraTradeRows",
        "tradeRowCoveragePercent",
        "tradeRowVerificationStatus",
        "discoveredTradeOutcomes",
        "apiMatchedTradeOutcomes",
        "apiMissingTradeOutcomes",
        "apiOutcomeCoveragePercent",
        "matchedTradeOutcomes",
        "missingTradeOutcomes",
        "extraDownloadedOutcomes",
        "outcomeCoveragePercent",
        "discoveredTradeMarkets",
        "matchedTradeMarkets",
        "missingTradeMarkets",
        "extraDownloadedMarkets",
        "polymarketTraded",
        "marketCoveragePercent",
        "positionCoveragePercent",
        "tradePaginationComplete",
        "activityPaginationComplete",
        "coverageStatus",
        "missingOutcomeSample",
        "missingMarketSample",
        "verificationVersion",
        "resolvedPositions",
        "wins",
        "losses",
        "realizedPnlClosed",
        "realizedPnlClosedRaw",
        "realizedPnlAfterCosts",
        "oneShareNetPnlAfterCosts",
        "oneShareNetPnlAfterCostsPerTrade",
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
        "downloadedMarkets": score.get("downloadedMarkets", ""),
        "downloadedTradeRows": score.get("downloadedTradeRows", ""),
        "uniqueTradeRows": score.get("uniqueTradeRows", ""),
        "duplicateTradeRows": score.get("duplicateTradeRows", ""),
        "activityTradeRows": score.get("activityTradeRows", ""),
        "activityUniqueTradeRows": score.get("activityUniqueTradeRows", ""),
        "matchedTradeRows": score.get("matchedTradeRows", ""),
        "missingTradeRows": score.get("missingTradeRows", ""),
        "extraTradeRows": score.get("extraTradeRows", ""),
        "tradeRowCoveragePercent": score.get("tradeRowCoveragePercent", ""),
        "tradeRowVerificationStatus": score.get(
            "tradeRowVerificationStatus", ""
        ),
        "discoveredTradeOutcomes": score.get("discoveredTradeOutcomes", ""),
        "apiMatchedTradeOutcomes": score.get("apiMatchedTradeOutcomes", ""),
        "apiMissingTradeOutcomes": score.get("apiMissingTradeOutcomes", ""),
        "apiOutcomeCoveragePercent": score.get("apiOutcomeCoveragePercent", ""),
        "matchedTradeOutcomes": score.get("matchedTradeOutcomes", ""),
        "missingTradeOutcomes": score.get("missingTradeOutcomes", ""),
        "extraDownloadedOutcomes": score.get("extraDownloadedOutcomes", ""),
        "outcomeCoveragePercent": score.get("outcomeCoveragePercent", ""),
        "discoveredTradeMarkets": score.get("discoveredTradeMarkets", ""),
        "matchedTradeMarkets": score.get("matchedTradeMarkets", ""),
        "missingTradeMarkets": score.get("missingTradeMarkets", ""),
        "extraDownloadedMarkets": score.get("extraDownloadedMarkets", ""),
        "polymarketTraded": score.get("polymarketTraded", ""),
        "marketCoveragePercent": score.get("marketCoveragePercent", ""),
        "positionCoveragePercent": score.get("positionCoveragePercent", ""),
        "tradePaginationComplete": score.get("tradePaginationComplete", ""),
        "activityPaginationComplete": score.get(
            "activityPaginationComplete", ""
        ),
        "coverageStatus": score.get("coverageStatus", ""),
        "missingOutcomeSample": score.get("missingOutcomeSample", ""),
        "missingMarketSample": score.get("missingMarketSample", ""),
        "verificationVersion": score.get("verificationVersion", ""),
        "openPositions": score.get("openPositions", ""),
        "resolvedPositions": score.get("resolvedPositions", ""),
        "wins": score.get("wins", ""),
        "losses": score.get("losses", ""),
        "realizedPnlClosed": score.get("realizedPnlClosed", ""),
        "realizedPnlClosedRaw": score.get("realizedPnlClosedRaw", ""),
        "realizedPnlAfterCosts": score.get("realizedPnlAfterCosts", ""),
        "oneShareNetPnlAfterCosts": score.get("oneShareNetPnlAfterCosts", ""),
        "oneShareNetPnlAfterCostsPerTrade": score.get(
            "oneShareNetPnlAfterCostsPerTrade", ""
        ),
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
        "testedAt": epoch_milliseconds(),
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
            safe_float(row.get("oneShareNetPnlAfterCostsPerTrade")),
            safe_float(row.get("oneShareNetPnlAfterCosts")),
            safe_float(row.get("edgeRally"), safe_float(row.get("netEdgeScore"), safe_float(row.get("netEdge")))),
            safe_float(row.get("adjustedWinRate")),
            safe_float(row.get("expectedPayoff")),
        ),
        reverse=True,
    )
    return score_rows


def write_sorted_scores_csv(rows: Any, path: Path, fieldnames: list[str]) -> str:
    if path.exists() and not OVERWRITE_OUTPUT_FILES:
        return "skipped-existing"
    score_rows = sorted_score_rows(rows)
    ranked_rows: list[dict[str, Any]] = []
    for rank, row in enumerate(score_rows, start=1):
        ranked = dict(row)
        ranked["rank"] = rank
        ranked_rows.append(ranked)
    return _write_csv_rows_replace_safe(
        path,
        fieldnames,
        ranked_rows,
        operation="sorted-score-checkpoint",
    )


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
        ("oneShareNetPnlAfterCostsPerTrade", True),
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
) -> str:
    score_rows = (
        sorted_rows_by_factor(rows, sort_factor, descending) if sort_factor else sorted_score_rows(rows)
    )
    return write_table_xlsx(score_rows, path, fieldnames, sheet_name=sheet_name)


def write_table_xlsx(rows: Any, path: Path, fieldnames: list[str], sheet_name: str) -> str:
    if path.exists() and not OVERWRITE_OUTPUT_FILES:
        return "skipped-existing"
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
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(
        f"{path.name}.xlsx-checkpoint.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with zipfile.ZipFile(temp_path, "w", zipfile.ZIP_DEFLATED) as xlsx:
            xlsx.writestr("[Content_Types].xml", '<?xml version="1.0" encoding="UTF-8"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
            xlsx.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
            safe_sheet_name = excel_cell_value(sheet_name[:31])
            xlsx.writestr("xl/workbook.xml", f'<?xml version="1.0" encoding="UTF-8"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="{safe_sheet_name}" sheetId="1" r:id="rId1"/></sheets></workbook>')
            xlsx.writestr("xl/_rels/workbook.xml.rels", '<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>')
            xlsx.writestr("xl/worksheets/sheet1.xml", worksheet)

        last_error: OSError | None = None
        for attempt in range(6):
            try:
                os.replace(temp_path, path)
                return "atomic-replace"
            except (PermissionError, OSError) as exc:
                last_error = exc
                time.sleep(min(0.05 * (2 ** attempt), 0.8))
        if last_error is not None:
            raise last_error
        raise OSError(f"Could not publish XLSX checkpoint: {path}")
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


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
        "snapshotStart",
        "snapshotEnd",
        "tradesRawRows",
        "activityRawRows",
        "logicalTradeRows",
        "matchedCoreRows",
        "activityOnlyRows",
        "tradesOnlyRows",
        "exactRepeatedRows",
        "valueDifferenceRows",
        "sideDifferenceRows",
        "onchainVerifiedRows",
        "verifiedTradeRows",
        "unresolvedTradeRows",
        "tradeVerificationStatus",
        "verificationReason",
        "downloadedMarkets",
        "downloadedTradeRows",
        "uniqueTradeRows",
        "duplicateTradeRows",
        "activityTradeRows",
        "activityUniqueTradeRows",
        "matchedTradeRows",
        "missingTradeRows",
        "extraTradeRows",
        "tradeRowCoveragePercent",
        "tradeRowVerificationStatus",
        "discoveredTradeOutcomes",
        "apiMatchedTradeOutcomes",
        "apiMissingTradeOutcomes",
        "apiOutcomeCoveragePercent",
        "matchedTradeOutcomes",
        "missingTradeOutcomes",
        "extraDownloadedOutcomes",
        "outcomeCoveragePercent",
        "discoveredTradeMarkets",
        "matchedTradeMarkets",
        "missingTradeMarkets",
        "extraDownloadedMarkets",
        "polymarketTraded",
        "marketCoveragePercent",
        "positionCoveragePercent",
        "tradePaginationComplete",
        "activityPaginationComplete",
        "coverageStatus",
        "missingOutcomeSample",
        "missingMarketSample",
        "verificationVersion",
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
        "oneShareNetPnlAfterCostsPerTrade",
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
    parser.add_argument("--refresh-since-ms", type=int, default=0, help=argparse.SUPPRESS)
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
            "[wallet inclusion] "
            + wallet_inclusion_policy_text(
                max_wallets=max_wallets,
                min_positions=min_positions,
                min_losses=min_losses,
                min_pnl=min_pnl,
            )
        )
        print(
            "[mode 2 filters] "
            f"min_resolved={FILTER_MIN_RESOLVED_POSITIONS}:{MIN_RESOLVED_POSITIONS} "
            f"min_losses={FILTER_MIN_LOSING_POSITIONS}:{MIN_LOSING_POSITIONS} "
            f"min_pnl={FILTER_MIN_CLOSED_REALIZED_PNL}:{MIN_CLOSED_REALIZED_PNL} "
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
        print(
            f"[memory] {TEST_MEMORY_FILE_NAME} is persistent and authoritative; "
            f"restart_resumes_done_wallets=true reset_only_when_file_deleted=true "
            f"refresh_each_completed_run={REFRESH_EXISTING_WALLETS_ON_EACH_RUN}"
        )
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
            f"closed_workers={_effective_closed_fetch_workers()} "
            f"activity_workers={_effective_activity_fetch_workers()} "
            f"activity_windows={_effective_activity_window_workers()} "
            f"checkpoint_every={ACTIVITY_CHECKPOINT_EVERY_WINDOWS} "
            f"market_inflight_x={MARKET_BATCH_MAX_INFLIGHT_MULTIPLIER} "
            f"market_progress_every={MARKET_BATCH_PROGRESS_EVERY} "
            f"cache_db={COMPLETE_FETCH_CACHE_DB_FILE_NAME}"
        )
        print("[live output] edge_scores_progress.csv updates during scoring")
        print(
            "[final output] edge_scores.xlsx is sorted highest-to-lowest by "
            "oneShareNetPnlAfterCostsPerTrade; edge_scores_by_<factor>.xlsx files "
            "are also produced after scoring finishes"
        )
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


def proxy_json_request(proxy_url: str, url: str, timeout: float = 12.0) -> Any:
    """Fetch and decode one JSON response through a specific local Xray proxy."""
    raw_text = proxy_text_request(proxy_url, url, timeout=timeout)
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as exc:
        preview = raw_text[:200].replace("\n", " ")
        raise RuntimeError(f"Invalid JSON from {url}: {preview!r}") from exc


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
    line_callback: Any = None,
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
                args=(proc, source, logger, None, None, line_callback),
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
    output_tail: deque[str] | None = None,
    activity_state: dict[str, Any] | None = None,
    line_callback: Any = None,
) -> None:
    """Route child output and retain a bounded tail for post-crash diagnostics."""
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\r\n")
        if not line:
            continue
        if output_tail is not None:
            output_tail.append(line)
        if activity_state is not None:
            activity_state["last_output_monotonic"] = time.monotonic()
            activity_state["last_output_line"] = line
        logger.log(line, source=prefix)
        if line_callback is not None:
            try:
                line_callback(line)
            except Exception as exc:
                logger.log(
                    f"[xray-line-callback-failed] source={prefix} error={exc!r}",
                    source="SYSTEM",
                    force_error=True,
                )


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
        score_sources = [directory / "edge_scores_progress.csv"]
        score_sources.extend(
            directory / name for name in LEGACY_SCORE_JOURNAL_FILE_NAMES
        )
        score_sources.append(directory / SCORE_JOURNAL_FILE_NAME)
        for score_source in score_sources:
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

    memory_sources = [
        path
        for directory in source_dirs
        for path in test_memory_paths_for_directory(directory)
    ]
    merge_test_memory_files(
        memory_sources,
        merged_dir / TEST_MEMORY_FILE_NAME,
    )
    merge_position_completeness_summaries(
        source_dirs,
        merged_dir / POSITION_COMPLETENESS_LOG_FILE_NAME,
    )

    completed_wallets: set[str] = set()
    for directory in source_dirs:
        completed_wallets |= load_test_memory_from_directory(directory)

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
    all_log_path = root / ALL_LOG_FILE_NAME
    error_log_path = root / ERROR_LOG_FILE_NAME
    diagnostic_path = root / DIAGNOSTIC_LOG_FILE_NAME
    worker_crash_path = root / WORKER_CRASH_LOG_FILE_NAME

    def existing_file_size(path: Path) -> int:
        try:
            return path.stat().st_size
        except OSError:
            return 0

    # Cursors begin at the exact byte where this process started appending, so
    # diagnostics never confuse older-run errors/events with the current run.
    run_all_log_start_offset = existing_file_size(all_log_path)
    run_error_log_start_offset = existing_file_size(error_log_path)
    run_worker_crash_start_offset = existing_file_size(worker_crash_path)

    logger = RunLogRouter(all_log_path, error_log_path)
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
    if (
        not FULL_WALLET_INCLUSION_MODE
        and FILTER_MAX_WALLETS_TO_SCORE
        and progress_max_wallets
    ):
        progress_wallet_rows = progress_wallet_rows[: int(progress_max_wallets)]
    progress_wallet_set = {item.proxy_wallet for item in progress_wallet_rows}
    total_wallet_count = len(progress_wallet_set)
    fallback_completed_wallets = (
        load_test_memory_from_directory(fallback_out_dir) & progress_wallet_set
    )

    script_path = Path(__file__).resolve()
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"

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
                completed |= load_test_memory_from_directory(part_dir)
        return len(completed & progress_wallet_set)

    def console_line(text: str, *, newline: bool = False) -> None:
        nonlocal last_console_width
        if not CLEAN_CONSOLE_DASHBOARD:
            return
        # Print complete lines instead of carriage-return redraws. This keeps
        # Windows CMD output copyable and prevents status/error text overlap.
        with console_output_lock:
            if text:
                console_stdout.write(text.rstrip() + "\n")
            elif newline:
                console_stdout.write("\n")
            last_console_width = 0
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
                f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: {new_errors} new "
                f"entr{'y' if new_errors == 1 else 'ies'} -> {ERROR_LOG_FILE_NAME}\n\n"
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

            # فقط وقتی روشن باشد، نودهای dead دوره‌ای Restart و دوباره تست می‌شوند.
            if VPN_DEAD_RECHECK_ENABLED:
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
                if VPN_DEAD_RECHECK_ENABLED:
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
GLOBAL_MANAGER_LOCK_FILE_NAME = "global_manager.lock"
GLOBAL_QUEUE_CACHE_DIR_NAME = "_queue_cache"
GLOBAL_QUEUE_RUNTIME_DIR_NAME = "_queue_runtime"
GLOBAL_QUEUE_BUCKET_COUNT = 512
GLOBAL_QUEUE_BATCH_SIZE = 8
GLOBAL_QUEUE_COMPLETE_ALL_WALLETS = True
GLOBAL_QUEUE_MAX_ATTEMPTS_PER_WALLET = 0  # در Complete-all نادیده گرفته می‌شود؛ هیچ والت نهایی حذف نمی‌شود.
GLOBAL_QUEUE_IMPORT_OLD_PARTS = True
GLOBAL_QUEUE_FINAL_MERGE_ON_EXIT = True
GLOBAL_QUEUE_MERGE_RAW_JSONL = False
# User-facing score files must not stay empty/old until all 31k wallets finish.
# Worker journals remain the per-wallet durable source of truth; the Manager
# republishes the sorted root CSV frequently and the main XLSX at a slower rate.
GLOBAL_QUEUE_LIVE_OUTPUT_SYNC_ENABLED = True
GLOBAL_QUEUE_LIVE_CSV_SYNC_SECONDS = 15.0
GLOBAL_QUEUE_LIVE_XLSX_SYNC_SECONDS = 60.0
ACTIVE_VPN_FILE_NAME = "active_vpns.txt"
ACTIVE_VPN_UPDATE_INTERVAL_SECONDS = 10.0

# During startup every potentially slow stage prints a heartbeat at this interval.
# This changes visibility only; it never changes queue/checkpoint behaviour.
STARTUP_HEARTBEAT_SECONDS = 5.0
STARTUP_SCAN_PROGRESS_EVERY_SOURCES = 25
STARTUP_VPN_PARSE_PROGRESS_EVERY_LINKS = 250

# Stale workers/Xray processes are signaled with the operating-system kill API,
# then polled together inside one total budget so Windows handles are released.
# The budget is global, never multiplied by the number of PIDs.
STALE_PID_CLEANUP_TOTAL_TIMEOUT_SECONDS = 3.0
STALE_PID_CLEANUP_PROGRESS_EVERY = 100

# این فایل فقط یک‌بار، بلافاصله بعد از پایان تست اولیه VPNها ساخته می‌شود.
# محل آن کنار خود فایل پایتون است، نه داخل پوشه خروجی.
# همه VPNهای سالم و دارای IP یکتا را بر اساس بهترین عملکرد Polymarket
# مرتب می‌کند و تا پایان همان اجرای برنامه دیگر هرگز ویرایش نمی‌شود.
STARTUP_SORTED_VPN_SNAPSHOT_FILE_NAME = "active_vpns_startup_sorted.txt"

# حافظه دائمی نتیجه تست VPNها، کنار خود فایل پایتون.
# تا وقتی این فایل وجود دارد، لینک‌هایی که دقیقاً بدون تغییر باقی مانده‌اند دوباره
# بنچمارک کامل اولیه نمی‌شوند. با حذف این فایل، همه VPNها از اول تست می‌شوند.
VPN_STARTUP_TEST_MEMORY_FILE_NAME = "vpn_startup_test_memory.json"
VPN_STARTUP_TEST_MEMORY_VERSION = 2


class SystemCpuUsageSampler:
    """Dependency-free total-system CPU sampler for Windows and Linux."""

    def __init__(self) -> None:
        self._mode = ""
        self._last_idle = 0
        self._last_total = 0
        self._windows_get_system_times = None
        self._windows_filetime_type = None

        if os.name == "nt":
            try:
                import ctypes

                class FILETIME(ctypes.Structure):
                    _fields_ = [
                        ("dwLowDateTime", ctypes.c_uint32),
                        ("dwHighDateTime", ctypes.c_uint32),
                    ]

                get_system_times = ctypes.windll.kernel32.GetSystemTimes
                get_system_times.argtypes = [
                    ctypes.POINTER(FILETIME),
                    ctypes.POINTER(FILETIME),
                    ctypes.POINTER(FILETIME),
                ]
                get_system_times.restype = ctypes.c_int
                self._windows_get_system_times = get_system_times
                self._windows_filetime_type = FILETIME
                self._mode = "windows"
            except Exception:
                self._mode = ""

        if not self._mode and Path("/proc/stat").exists():
            self._mode = "proc"

        counters = self._read_counters()
        if counters is not None:
            self._last_idle, self._last_total = counters

    @property
    def available(self) -> bool:
        return bool(self._mode)

    @staticmethod
    def _filetime_value(value: Any) -> int:
        return (int(value.dwHighDateTime) << 32) | int(value.dwLowDateTime)

    def _read_counters(self) -> tuple[int, int] | None:
        if self._mode == "windows":
            try:
                import ctypes

                filetime_type = self._windows_filetime_type
                get_system_times = self._windows_get_system_times
                if filetime_type is None or get_system_times is None:
                    return None
                idle = filetime_type()
                kernel = filetime_type()
                user = filetime_type()
                if not get_system_times(
                    ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)
                ):
                    return None
                idle_value = self._filetime_value(idle)
                total_value = self._filetime_value(kernel) + self._filetime_value(user)
                return idle_value, total_value
            except Exception:
                return None

        if self._mode == "proc":
            try:
                first_line = Path("/proc/stat").read_text(
                    encoding="utf-8"
                ).splitlines()[0]
                values = [int(value) for value in first_line.split()[1:]]
                if len(values) < 4:
                    return None
                idle_value = values[3] + (values[4] if len(values) > 4 else 0)
                return idle_value, sum(values)
            except Exception:
                return None

        return None

    def sample_percent(self) -> float | None:
        counters = self._read_counters()
        if counters is None:
            return None
        idle, total = counters
        idle_delta = idle - self._last_idle
        total_delta = total - self._last_total
        self._last_idle, self._last_total = idle, total
        if total_delta <= 0:
            return None
        busy_delta = max(total_delta - idle_delta, 0)
        return min(max(busy_delta / total_delta * 100.0, 0.0), 100.0)


class CpuPeakMonitor:
    """Samples CPU in the background and tracks each auto-tune window."""

    def __init__(self, sample_interval_seconds: float = 1.0) -> None:
        self.sample_interval_seconds = max(float(sample_interval_seconds), 0.2)
        self.sampler = SystemCpuUsageSampler()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._current = 0.0
        self._window_peak = 0.0
        self._window_sum = 0.0
        self._window_samples = 0
        self._all_time_peak = 0.0

    @property
    def available(self) -> bool:
        return self.sampler.available

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="cpu-peak-monitor",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=3)

    def _run(self) -> None:
        while not self._stop_event.wait(self.sample_interval_seconds):
            value = self.sampler.sample_percent()
            if value is None:
                continue
            with self._lock:
                self._current = value
                self._window_peak = max(self._window_peak, value)
                self._window_sum += value
                self._window_samples += 1
                self._all_time_peak = max(self._all_time_peak, value)

    def snapshot(self) -> dict[str, float | int]:
        with self._lock:
            average = (
                self._window_sum / self._window_samples
                if self._window_samples
                else 0.0
            )
            return {
                "current": self._current,
                "window_peak": self._window_peak,
                "window_average": average,
                "window_samples": self._window_samples,
                "all_time_peak": self._all_time_peak,
            }

    def consume_window(self) -> dict[str, float | int]:
        with self._lock:
            average = (
                self._window_sum / self._window_samples
                if self._window_samples
                else 0.0
            )
            result = {
                "current": self._current,
                "window_peak": self._window_peak,
                "window_average": average,
                "window_samples": self._window_samples,
                "all_time_peak": self._all_time_peak,
            }
            self._window_peak = 0.0
            self._window_sum = 0.0
            self._window_samples = 0
            return result


def _cpu_autotune_next_limit(
    current_limit: int,
    average_percent: float,
    natural_cap: int,
) -> tuple[int, str]:
    """Return the next runtime limit using only the latest completed CPU window.

    Exact rules:
      * average < 70%: increase by VPN_AUTO_TUNE_UP_STEP
      * 70% <= average < 85%: hold
      * average >= 85%: decrease by exactly one

    There is no queued/stale downscale signal and no network/progress override.
    The only bounds are the natural range 1..number of currently working VPNs.
    """
    cap = max(1, int(natural_cap))
    current = max(1, int(current_limit))
    if current > cap:
        return cap, "clamp-to-available-vpns"

    average = float(average_percent)
    if average >= float(CPU_AUTO_TUNE_HIGH_PERCENT):
        next_limit = max(1, current - 1)
        return next_limit, (
            "down-cpu-current-window" if next_limit < current else "hold-natural-floor"
        )

    if average < float(CPU_AUTO_TUNE_LOW_PERCENT):
        up_step = max(1, int(VPN_AUTO_TUNE_UP_STEP))
        next_limit = min(cap, current + up_step)
        return next_limit, (
            f"up-cpu-current-window-step-{up_step}"
            if next_limit > current
            else "hold-all-working-vpns-active"
        )

    return current, "hold-stable-band"


class SystemMemoryUsageSampler:
    """Reads total physical RAM usage without requiring psutil."""

    def __init__(self) -> None:
        self._mode: str | None = None
        self._windows_status_type: Any = None
        self._windows_global_memory_status_ex: Any = None

        if os.name == "nt":
            try:
                import ctypes
                from ctypes import wintypes

                class MEMORYSTATUSEX(ctypes.Structure):
                    _fields_ = [
                        ("dwLength", wintypes.DWORD),
                        ("dwMemoryLoad", wintypes.DWORD),
                        ("ullTotalPhys", ctypes.c_ulonglong),
                        ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong),
                        ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong),
                        ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                    ]

                function = ctypes.windll.kernel32.GlobalMemoryStatusEx
                function.argtypes = [ctypes.POINTER(MEMORYSTATUSEX)]
                function.restype = wintypes.BOOL
                self._windows_status_type = MEMORYSTATUSEX
                self._windows_global_memory_status_ex = function
                self._mode = "windows"
            except Exception:
                self._mode = None
        elif Path("/proc/meminfo").exists():
            self._mode = "proc"

    @property
    def available(self) -> bool:
        return bool(self._mode)

    def sample_percent(self) -> float | None:
        if self._mode == "windows":
            try:
                import ctypes

                status_type = self._windows_status_type
                function = self._windows_global_memory_status_ex
                if status_type is None or function is None:
                    return None
                status = status_type()
                status.dwLength = ctypes.sizeof(status)
                if not function(ctypes.byref(status)):
                    return None
                return min(max(float(status.dwMemoryLoad), 0.0), 100.0)
            except Exception:
                return None

        if self._mode == "proc":
            try:
                values: dict[str, int] = {}
                for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
                    if ":" not in line:
                        continue
                    key, raw_value = line.split(":", 1)
                    match = re.search(r"(\d+)", raw_value)
                    if match:
                        values[key] = int(match.group(1))
                total = int(values.get("MemTotal", 0))
                available = int(values.get("MemAvailable", values.get("MemFree", 0)))
                if total <= 0:
                    return None
                used = max(total - available, 0)
                return min(max(used / total * 100.0, 0.0), 100.0)
            except Exception:
                return None

        return None


class RamSafetyMonitor:
    """Samples RAM every second and trips after a full rolling minute above the limit."""

    def __init__(
        self,
        sample_interval_seconds: float,
        window_seconds: float,
        stop_percent: float,
    ) -> None:
        self.sample_interval_seconds = max(float(sample_interval_seconds), 0.2)
        self.window_seconds = max(float(window_seconds), self.sample_interval_seconds)
        self.stop_percent = float(stop_percent)
        self.required_samples = max(
            1,
            int(math.ceil(self.window_seconds / self.sample_interval_seconds)),
        )
        self.sampler = SystemMemoryUsageSampler()
        self._samples: deque[float] = deque(maxlen=self.required_samples)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._trip_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._current = 0.0
        self._average = 0.0
        self._peak = 0.0
        self._trip_average = 0.0
        self._trip_current = 0.0
        self._trip_time = 0.0

    @property
    def available(self) -> bool:
        return self.sampler.available

    @property
    def tripped(self) -> bool:
        return self._trip_event.is_set()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="ram-safety-monitor",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=3)

    def _run(self) -> None:
        while not self._stop_event.wait(self.sample_interval_seconds):
            value = self.sampler.sample_percent()
            if value is None:
                continue
            with self._lock:
                self._current = value
                self._peak = max(self._peak, value)
                self._samples.append(value)
                self._average = sum(self._samples) / len(self._samples)
                if (
                    len(self._samples) >= self.required_samples
                    and self._average > self.stop_percent
                    and not self._trip_event.is_set()
                ):
                    self._trip_average = self._average
                    self._trip_current = value
                    self._trip_time = time.time()
                    self._trip_event.set()

    def snapshot(self) -> dict[str, float | int | bool]:
        with self._lock:
            return {
                "current": self._current,
                "average": self._average,
                "peak": self._peak,
                "samples": len(self._samples),
                "required_samples": self.required_samples,
                "ready": len(self._samples) >= self.required_samples,
                "tripped": self._trip_event.is_set(),
                "trip_average": self._trip_average,
                "trip_current": self._trip_current,
                "trip_time": self._trip_time,
            }


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
    temp = path.with_name(
        f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    temp.write_text(text, encoding="utf-8")
    last_error: OSError | None = None
    for attempt in range(6):
        try:
            os.replace(temp, path)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(min(0.05 * (2 ** attempt), 0.8))
    try:
        if path.exists():
            path.chmod(path.stat().st_mode | stat.S_IWRITE)
        path.write_text(text, encoding="utf-8")
    except OSError:
        if last_error is not None:
            raise last_error
        raise
    finally:
        temp.unlink(missing_ok=True)


class ManagerInstanceLock:
    """One operating-system lock per output root; released automatically on exit."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None
        self.acquired = False

    def acquire(self) -> bool:
        ensure_dir(self.path.parent)
        self.path.touch(exist_ok=True)
        handle = self.path.open("r+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError):
            handle.close()
            return False
        payload = f"pid={os.getpid()} build={BUILD_ID} started={_log_timestamp()}\n".encode(
            "utf-8"
        )
        handle.seek(0)
        handle.write(payload)
        handle.truncate()
        handle.flush()
        self.handle = handle
        self.acquired = True
        return True

    def release(self) -> None:
        handle = self.handle
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            handle.close()
            self.handle = None
            self.acquired = False


def _pid_is_running(pid: int) -> bool:
    try:
        normalized_pid = int(pid)
    except (TypeError, ValueError):
        return False
    if normalized_pid <= 0:
        return False
    if normalized_pid == os.getpid():
        return True
    try:
        os.kill(normalized_pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if getattr(exc, "errno", None) == errno.ESRCH or getattr(
            exc, "winerror", None
        ) in {87, 1168}:
            return False
        return True


def _terminate_pid(pid: int) -> str:
    """Request immediate termination without waiting for a helper process.

    On Windows, ``os.kill(..., SIGTERM)`` calls TerminateProcess.  The old code
    launched one ``taskkill`` command per PID and waited up to eight seconds for
    each command, so a few dozen stale rows could freeze startup for minutes.
    Workers and Xray processes are both registered independently in the queue,
    so terminating every recorded PID does not require ``taskkill /T``.
    """
    try:
        normalized_pid = int(pid)
    except (TypeError, ValueError):
        return "invalid"
    if normalized_pid <= 0:
        return "invalid"
    if normalized_pid == os.getpid():
        return "self"
    try:
        os.kill(normalized_pid, signal.SIGTERM)
        return "signaled"
    except ProcessLookupError:
        return "not-running"
    except PermissionError:
        return "permission-denied"
    except OSError as exc:
        # Windows uses ERROR_INVALID_PARAMETER / ERROR_NOT_FOUND when the PID
        # no longer exists; POSIX reports ESRCH.
        if getattr(exc, "errno", None) == errno.ESRCH or getattr(
            exc, "winerror", None
        ) in {87, 1168}:
            return "not-running"
        return f"os-error:{type(exc).__name__}:{exc}"
    except Exception as exc:
        return f"error:{type(exc).__name__}:{exc}"


class GlobalQueueState:
    """SQLite WAL queue with one serialized connection and retry-safe transactions."""

    _RETRYABLE_SQLITE_ERRORS = (
        "database is locked",
        "database table is locked",
        "database schema is locked",
        "cannot start a transaction within a transaction",
        "not an error",
        "busy",
    )

    def __init__(self, path: Path) -> None:
        ensure_dir(path.parent)
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, timeout=60.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("PRAGMA busy_timeout=60000")
            self.conn.execute("PRAGMA foreign_keys=ON")
        self._schema()

    def _is_retryable_sqlite_error(self, exc: BaseException) -> bool:
        message = str(exc).strip().lower()
        return isinstance(exc, sqlite3.OperationalError) and any(
            token in message for token in self._RETRYABLE_SQLITE_ERRORS
        )

    def _rollback_quietly(self) -> None:
        try:
            if self.conn.in_transaction:
                self.conn.rollback()
        except sqlite3.Error:
            pass

    def _begin_immediate_with_retry(self, attempts: int = 12) -> None:
        last_error: BaseException | None = None
        for attempt in range(max(1, int(attempts))):
            try:
                self._rollback_quietly()
                self.conn.execute("BEGIN IMMEDIATE")
                return
            except sqlite3.OperationalError as exc:
                last_error = exc
                self._rollback_quietly()
                if not self._is_retryable_sqlite_error(exc) or attempt + 1 >= attempts:
                    raise
                time.sleep(min(0.05 * (2 ** attempt), 1.0))
        if last_error is not None:
            raise last_error

    def _schema(self) -> None:
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS wallet_queue (
                    wallet TEXT PRIMARY KEY,
                    seed_json TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    bucket INTEGER NOT NULL,
                    legacy_dir TEXT NOT NULL DEFAULT '',
                    work_dir TEXT NOT NULL DEFAULT '',
                    resume_fallback_dir TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    assigned_node INTEGER,
                    batch_id TEXT,
                    retry_avoid_node INTEGER,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    unexpected_failures INTEGER NOT NULL DEFAULT 0,
                    retry_after INTEGER NOT NULL DEFAULT 0,
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
                self.conn.execute(
                    "ALTER TABLE wallet_queue ADD COLUMN active INTEGER NOT NULL DEFAULT 1"
                )
            except sqlite3.OperationalError:
                pass
            try:
                self.conn.execute(
                    "ALTER TABLE wallet_queue ADD COLUMN retry_avoid_node INTEGER"
                )
            except sqlite3.OperationalError:
                pass
            for statement in (
                "ALTER TABLE wallet_queue ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE wallet_queue ADD COLUMN unexpected_failures INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE wallet_queue ADD COLUMN retry_after INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE wallet_queue ADD COLUMN work_dir TEXT NOT NULL DEFAULT ''",
                "ALTER TABLE wallet_queue ADD COLUMN resume_fallback_dir TEXT NOT NULL DEFAULT ''",
            ):
                try:
                    self.conn.execute(statement)
                except sqlite3.OperationalError:
                    pass
            self.conn.commit()

    def seed(self, seeds: list[WalletSeed]) -> None:
        now = int(time.time())
        with self.lock, self.conn:
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
            self.conn.execute(
                "INSERT OR REPLACE INTO queue_meta(key,value) VALUES('current_wallets_json', ?)",
                (json.dumps(sorted(allowed)),),
            )
            # v41 guarantees complete coverage. Any active wallet that an older
            # version marked failed_final is revived and retried indefinitely.
            if bool(GLOBAL_QUEUE_COMPLETE_ALL_WALLETS) or int(GLOBAL_QUEUE_MAX_ATTEMPTS_PER_WALLET) <= 0:
                self.conn.execute(
                    """
                    UPDATE wallet_queue
                    SET status='pending', assigned_node=NULL, batch_id=NULL,
                        retry_avoid_node=NULL, retry_after=0,
                        last_error=CASE
                            WHEN last_error='' THEN 'revived by complete-all mode'
                            ELSE last_error
                        END,
                        updated_at=?
                    WHERE active=1 AND status='failed_final'
                    """,
                    (now,),
                )

            # Old v44 heavy rows may still carry a two-minute deferred timestamp.
            # Clamp only those old waits into a short staggered startup window;
            # retry counts and all checkpoint data remain untouched.
            heavy_after = max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES))
            release_window = max(
                0, int(float(GLOBAL_QUEUE_STARTUP_HEAVY_RELEASE_WINDOW_SECONDS))
            )
            deferred_rows = list(
                self.conn.execute(
                    """
                    SELECT wallet, COALESCE(retry_after,0) AS retry_after
                    FROM wallet_queue
                    WHERE active=1 AND status='pending'
                      AND COALESCE(retry_count,0)>=?
                      AND COALESCE(retry_after,0)>?
                    """,
                    (heavy_after, now),
                )
            )
            for row in deferred_rows:
                wallet = str(row["wallet"])
                stagger = (
                    int(hashlib.sha1(wallet.encode("utf-8")).hexdigest()[:8], 16)
                    % (release_window + 1)
                    if release_window > 0
                    else 0
                )
                new_due = now + stagger
                if int(row["retry_after"] or 0) > new_due:
                    self.conn.execute(
                        "UPDATE wallet_queue SET retry_after=?, updated_at=? WHERE wallet=?",
                        (new_due, now, wallet),
                    )

    def current_wallets(self) -> set[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT value FROM queue_meta WHERE key='current_wallets_json'"
            ).fetchone()
        if not row:
            return set()
        try:
            return set(json.loads(str(row[0])))
        except Exception:
            return set()

    def begin_or_resume_refresh_cycle(
        self,
        fetch_version: str,
        enabled: bool,
    ) -> dict[str, Any]:
        """Start one durable refresh generation or resume it after interruption."""
        if not enabled:
            return {
                "cycle_id": 0,
                "started_at_ms": 0,
                "started_new": False,
                "resumed": False,
                "requeued": 0,
                "fetch_version": fetch_version,
            }

        now_ms = epoch_milliseconds()
        now_seconds = int(time.time())
        with self.lock, self.conn:
            meta = {
                str(row[0]): str(row[1])
                for row in self.conn.execute(
                    "SELECT key,value FROM queue_meta WHERE key IN "
                    "('refresh_cycle_id','refresh_started_at_ms',"
                    "'refresh_fetch_version','refresh_status')"
                )
            }
            unresolved_row = self.conn.execute(
                """
                SELECT COUNT(*) FROM wallet_queue
                WHERE active=1 AND status!='done'
                """
            ).fetchone()
            unresolved = int(unresolved_row[0] or 0) if unresolved_row else 0
            stored_started = int(safe_float(meta.get("refresh_started_at_ms"), 0))
            stored_cycle_id = int(safe_float(meta.get("refresh_cycle_id"), 0))
            can_resume = bool(
                meta.get("refresh_status") == "running"
                and meta.get("refresh_fetch_version") == fetch_version
                and stored_started > 0
                and unresolved > 0
            )
            if can_resume:
                return {
                    "cycle_id": stored_cycle_id,
                    "started_at_ms": stored_started,
                    "started_new": False,
                    "resumed": True,
                    "requeued": 0,
                    "fetch_version": fetch_version,
                }

            cycle_id = stored_cycle_id + 1
            cursor = self.conn.execute(
                """
                UPDATE wallet_queue
                SET status='pending', attempts=0, assigned_node=NULL,
                    batch_id=NULL, retry_avoid_node=NULL, retry_count=0,
                    unexpected_failures=0, retry_after=0, last_error='',
                    completed_at=NULL, updated_at=?
                WHERE active=1
                """,
                (now_seconds,),
            )
            values = {
                "refresh_cycle_id": str(cycle_id),
                "refresh_started_at_ms": str(now_ms),
                "refresh_fetch_version": fetch_version,
                "refresh_status": "running",
            }
            self.conn.executemany(
                "INSERT OR REPLACE INTO queue_meta(key,value) VALUES(?,?)",
                values.items(),
            )
            return {
                "cycle_id": cycle_id,
                "started_at_ms": now_ms,
                "started_new": True,
                "resumed": False,
                "requeued": int(cursor.rowcount or 0),
                "fetch_version": fetch_version,
            }

    def finish_refresh_cycle(self, started_at_ms: int) -> bool:
        """Mark the active refresh generation complete only after every wallet is done."""
        if int(started_at_ms) <= 0:
            return False
        with self.lock, self.conn:
            row = self.conn.execute(
                """
                SELECT
                    SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) AS total,
                    SUM(CASE WHEN active=1 AND status='done' THEN 1 ELSE 0 END) AS done
                FROM wallet_queue
                """
            ).fetchone()
            total = int(row[0] or 0) if row else 0
            done = int(row[1] or 0) if row else 0
            current = self.conn.execute(
                "SELECT value FROM queue_meta WHERE key='refresh_started_at_ms'"
            ).fetchone()
            current_started = int(safe_float(current[0], 0)) if current else 0
            if total <= 0 or done < total or current_started != int(started_at_ms):
                return False
            self.conn.executemany(
                "INSERT OR REPLACE INTO queue_meta(key,value) VALUES(?,?)",
                (
                    ("refresh_status", "completed"),
                    ("refresh_completed_at_ms", str(epoch_milliseconds())),
                ),
            )
            return True

    def kill_and_clear_stale_processes(
        self,
        max_seconds: float = STALE_PID_CLEANUP_TOTAL_TIMEOUT_SECONDS,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Terminate stale recorded processes and wait within one total budget."""
        with self.lock:
            rows = list(
                self.conn.execute(
                    "SELECT pid, kind, started_at FROM runtime_processes ORDER BY started_at, pid"
                )
            )

        started = time.monotonic()
        deadline = started + max(0.0, float(max_seconds))
        summary: dict[str, Any] = {
            "tracked": len(rows),
            "processed": 0,
            "signaled": 0,
            "not_running": 0,
            "self_skipped": 0,
            "invalid": 0,
            "permission_denied": 0,
            "failed": 0,
            "deadline_skipped": 0,
            "remaining_alive": 0,
            "failures": [],
            "elapsed_seconds": 0.0,
        }

        for index, row in enumerate(rows, start=1):
            if time.monotonic() >= deadline:
                summary["deadline_skipped"] = len(rows) - index + 1
                break

            pid = int(row[0])
            kind = str(row[1] or "unknown")
            status = _terminate_pid(pid)
            summary["processed"] += 1
            if status == "signaled":
                summary["signaled"] += 1
            elif status == "not-running":
                summary["not_running"] += 1
            elif status == "self":
                summary["self_skipped"] += 1
            elif status == "invalid":
                summary["invalid"] += 1
            elif status == "permission-denied":
                summary["permission_denied"] += 1
            else:
                summary["failed"] += 1
                failures = summary["failures"]
                if isinstance(failures, list) and len(failures) < 10:
                    failures.append(f"pid={pid} kind={kind} status={status}")

            if progress_callback is not None and (
                index == len(rows)
                or (
                    int(STALE_PID_CLEANUP_PROGRESS_EVERY) > 0
                    and index % int(STALE_PID_CLEANUP_PROGRESS_EVERY) == 0
                )
            ):
                summary["elapsed_seconds"] = max(0.0, time.monotonic() - started)
                progress_callback(dict(summary))

        # Give every signaled process the remainder of the same global budget to
        # release Windows file handles. This is one bounded wait for all PIDs,
        # never the old eight-second wait multiplied by the number of rows.
        signaled_pids = [
            int(row[0])
            for row in rows[: int(summary["processed"])]
            if int(row[0]) != os.getpid()
        ]
        while signaled_pids and time.monotonic() < deadline:
            signaled_pids = [pid for pid in signaled_pids if _pid_is_running(pid)]
            if signaled_pids:
                time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        summary["remaining_alive"] = sum(
            1 for pid in signaled_pids if _pid_is_running(pid)
        )

        # Runtime rows describe only the previous manager run.  Clear all rows,
        # including deadline-skipped ones, so a damaged table cannot stall every
        # future launch. next_free_local_port() still avoids any surviving port.
        with self.lock, self.conn:
            self.conn.execute("DELETE FROM runtime_processes")

        summary["elapsed_seconds"] = max(0.0, time.monotonic() - started)
        return summary

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
        with self.lock, self.conn:
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
        with self.lock, self.conn:
            for wallet in wallets:
                cursor = self.conn.execute(
                    """
                    UPDATE wallet_queue
                    SET status='done', assigned_node=NULL, batch_id=NULL,
                        retry_avoid_node=NULL,
                        completed_at=COALESCE(completed_at, ?), updated_at=?
                    WHERE wallet=? AND active=1 AND status!='done'
                    """,
                    (now, now, wallet),
                )
                changed += int(cursor.rowcount or 0)
        return changed

    def reconcile_with_test_memory(
        self,
        completed: set[str],
        *,
        reset_incomplete: bool = False,
    ) -> dict[str, int]:
        """Make durable CSV memory authoritative over stale queue ``done`` rows."""
        now = int(time.time())
        completed = {str(wallet).strip().lower() for wallet in completed if wallet}
        newly_done = 0
        reopened = 0
        reset_pending = 0
        with self.lock, self.conn:
            rows = list(
                self.conn.execute(
                    "SELECT wallet,status FROM wallet_queue WHERE active=1"
                )
            )
            for row in rows:
                wallet = str(row["wallet"] or "").lower()
                status = str(row["status"] or "pending")
                if wallet in completed:
                    if status != "done":
                        self.conn.execute(
                            """
                            UPDATE wallet_queue
                            SET status='done', assigned_node=NULL, batch_id=NULL,
                                retry_avoid_node=NULL, retry_after=0,
                                completed_at=COALESCE(completed_at, ?), updated_at=?
                            WHERE wallet=? AND active=1
                            """,
                            (now, now, wallet),
                        )
                        newly_done += 1
                    continue
                if status == "done" or reset_incomplete:
                    self.conn.execute(
                        """
                        UPDATE wallet_queue
                        SET status='pending', assigned_node=NULL, batch_id=NULL,
                            attempts=CASE WHEN ? THEN 0 ELSE attempts END,
                            retry_avoid_node=NULL,
                            retry_count=CASE WHEN ? THEN 0 ELSE retry_count END,
                            unexpected_failures=CASE WHEN ? THEN 0 ELSE unexpected_failures END,
                            retry_after=0, completed_at=NULL,
                            last_error=CASE WHEN ? THEN 'wallet_test_memory deleted; fresh run requested'
                                            ELSE last_error END,
                            updated_at=?
                        WHERE wallet=? AND active=1
                        """,
                        (
                            int(reset_incomplete),
                            int(reset_incomplete),
                            int(reset_incomplete),
                            int(reset_incomplete),
                            now,
                            wallet,
                        ),
                    )
                    if status == "done":
                        reopened += 1
                    if reset_incomplete:
                        reset_pending += 1
        return {
            "completed": len(completed),
            "newly_done": newly_done,
            "reopened": reopened,
            "reset_pending": reset_pending,
        }

    def set_legacy_dir(self, wallets: set[str], directory: Path) -> int:
        if not wallets:
            return 0
        changed = 0
        with self.lock, self.conn:
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
        preferred_lane: str | None = None,
    ) -> tuple[str, int, str, list[WalletSeed], str, int, str, str] | None:
        """Claim one queue batch, optionally restricted to normal or heavy work.

        preferred_lane:
          - None: legacy priority order (fresh -> retry -> heavy)
          - "normal": fresh/retry only
          - "fresh", "retry", or "heavy": that exact lane only
        """
        excluded = sorted(int(value) for value in busy_buckets)
        now_epoch = int(time.time())
        requested_lane = (
            str(preferred_lane).strip().lower()
            if preferred_lane is not None
            else ""
        )
        if requested_lane not in {"", "normal", "fresh", "retry", "heavy"}:
            raise ValueError(f"Unsupported queue lane: {preferred_lane!r}")

        with self.lock:
            current = self.current_wallets()
            if not current:
                return None
            self._begin_immediate_with_retry()
            try:
                heavy_after = max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES))
                base_query = (
                    "SELECT wallet,bucket,legacy_dir,work_dir,resume_fallback_dir,"
                    "retry_count,unexpected_failures "
                    "FROM wallet_queue WHERE status='pending' AND active=1 "
                    "AND COALESCE(retry_after,0) <= ?"
                )
                base_params: list[Any] = [now_epoch]
                if excluded:
                    base_query += " AND bucket NOT IN (" + ",".join("?" for _ in excluded) + ")"
                    base_params.extend(excluded)

                if requested_lane == "normal":
                    base_query += f" AND COALESCE(retry_count,0)<{heavy_after}"
                elif requested_lane == "fresh":
                    base_query += " AND COALESCE(retry_count,0)=0"
                elif requested_lane == "retry":
                    base_query += (
                        " AND COALESCE(retry_count,0)>0"
                        f" AND COALESCE(retry_count,0)<{heavy_after}"
                    )
                elif requested_lane == "heavy":
                    base_query += f" AND COALESCE(retry_count,0)>={heavy_after}"

                if requested_lane:
                    order_sql = " ORDER BY priority LIMIT 1"
                else:
                    order_sql = (
                        " ORDER BY CASE "
                        "WHEN COALESCE(retry_count,0)=0 THEN 0 "
                        f"WHEN COALESCE(retry_count,0)<{heavy_after} THEN 1 "
                        "ELSE 2 END, priority LIMIT 1"
                    )

                query = (
                    base_query
                    + " AND (retry_avoid_node IS NULL OR retry_avoid_node != ?)"
                    + order_sql
                )
                first = self.conn.execute(
                    query, [*base_params, int(node_index)]
                ).fetchone()
                avoided_same_node = first is not None
                if first is None:
                    first = self.conn.execute(
                        base_query + order_sql, base_params
                    ).fetchone()
                if first is None:
                    self.conn.commit()
                    return None

                bucket = int(first["bucket"])
                legacy_dir = str(first["legacy_dir"] or "")
                work_dir = str(first["work_dir"] or "")
                resume_fallback_dir = str(first["resume_fallback_dir"] or "")
                first_retry_count = int(first["retry_count"] or 0)
                first_unexpected_failures = int(first["unexpected_failures"] or 0)
                if first_retry_count <= 0:
                    lane = "fresh"
                elif first_retry_count < heavy_after:
                    lane = "retry"
                else:
                    lane = "heavy"

                if first_unexpected_failures >= max(
                    1, int(GLOBAL_QUEUE_ISOLATE_AFTER_ATTEMPTS)
                ):
                    adaptive_batch_size = 1
                elif lane == "fresh":
                    adaptive_batch_size = max(1, int(GLOBAL_QUEUE_BATCH_SIZE))
                elif lane == "retry":
                    adaptive_batch_size = max(
                        1, int(GLOBAL_QUEUE_RETRY_BATCH_SIZE)
                    )
                else:
                    adaptive_batch_size = max(
                        1, int(GLOBAL_QUEUE_HEAVY_BATCH_SIZE)
                    )

                rows_query = (
                    "SELECT wallet,seed_json FROM wallet_queue "
                    "WHERE status='pending' AND active=1 AND bucket=? AND legacy_dir=? "
                    "AND COALESCE(retry_after,0) <= ?"
                )
                rows_params: list[Any] = [bucket, legacy_dir, now_epoch]
                if lane == "fresh":
                    rows_query += " AND COALESCE(retry_count,0)=0"
                elif lane == "retry":
                    rows_query += (
                        " AND COALESCE(retry_count,0)>0"
                        f" AND COALESCE(retry_count,0)<{heavy_after}"
                    )
                else:
                    rows_query += f" AND COALESCE(retry_count,0)>={heavy_after}"
                if avoided_same_node:
                    rows_query += (
                        " AND (retry_avoid_node IS NULL OR retry_avoid_node != ?)"
                    )
                    rows_params.append(int(node_index))
                rows_query += " ORDER BY priority LIMIT ?"
                rows_params.append(adaptive_batch_size)
                rows = list(self.conn.execute(rows_query, rows_params))

                if not rows and avoided_same_node:
                    fallback_query = (
                        "SELECT wallet,seed_json FROM wallet_queue "
                        "WHERE status='pending' AND active=1 AND bucket=? AND legacy_dir=? "
                        "AND COALESCE(retry_after,0) <= ?"
                    )
                    fallback_params: list[Any] = [bucket, legacy_dir, now_epoch]
                    if lane == "fresh":
                        fallback_query += " AND COALESCE(retry_count,0)=0"
                    elif lane == "retry":
                        fallback_query += (
                            " AND COALESCE(retry_count,0)>0"
                            f" AND COALESCE(retry_count,0)<{heavy_after}"
                        )
                    else:
                        fallback_query += (
                            f" AND COALESCE(retry_count,0)>={heavy_after}"
                        )
                    fallback_query += " ORDER BY priority LIMIT ?"
                    fallback_params.append(adaptive_batch_size)
                    rows = list(
                        self.conn.execute(fallback_query, fallback_params)
                    )
                if not rows:
                    self.conn.commit()
                    return None

                wallets = [str(row["wallet"]) for row in rows]
                batch_id = (
                    f"{int(time.time())}-{node_index}-"
                    f"{hashlib.sha1('|'.join(wallets).encode()).hexdigest()[:10]}"
                )
                placeholders = ",".join("?" for _ in wallets)
                self.conn.execute(
                    f"""
                    UPDATE wallet_queue
                    SET status='running', attempts=attempts+1, assigned_node=?,
                        batch_id=?, retry_avoid_node=NULL, updated_at=?
                    WHERE wallet IN ({placeholders}) AND active=1 AND status='pending'
                    """,
                    (node_index, batch_id, now_epoch, *wallets),
                )
                self.conn.commit()
                return (
                    batch_id,
                    bucket,
                    legacy_dir,
                    [_seed_from_json(str(row["seed_json"])) for row in rows],
                    lane,
                    first_retry_count,
                    work_dir,
                    resume_fallback_dir,
                )
            except Exception:
                self._rollback_quietly()
                raise

    def claim_spillover_batch(
        self,
        node_index: int,
        busy_buckets: set[int],
        busy_output_dirs: set[str],
        cache_root: Path,
        preferred_lane: str = "heavy",
    ) -> tuple[str, int, str, list[WalletSeed], str, int, str, str] | None:
        """Claim one ready wallet whose normal bucket is blocked.

        The wallet keeps its logical bucket for ordering, but receives a stable
        wallet-specific work directory. Its previous work directory becomes a
        read-only resume fallback, so all Activity/Market/Current checkpoints are
        retained while file appenders remain single-writer.
        """
        if not bool(HEAVY_TAIL_BUCKET_SPILLOVER_ENABLED):
            return None

        requested_lane = str(preferred_lane or "heavy").strip().lower()
        if requested_lane not in {"fresh", "retry", "heavy", "normal"}:
            return None

        now_epoch = int(time.time())
        heavy_after = max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES))
        busy_bucket_values = sorted(int(value) for value in busy_buckets)
        normalized_busy_dirs = {
            os.path.normcase(os.path.abspath(value))
            for value in busy_output_dirs
            if str(value).strip()
        }

        with self.lock:
            self._begin_immediate_with_retry()
            try:
                query = (
                    "SELECT wallet,bucket,legacy_dir,work_dir,resume_fallback_dir,"
                    "retry_count,seed_json,priority,retry_avoid_node "
                    "FROM wallet_queue WHERE status='pending' AND active=1 "
                    "AND COALESCE(retry_after,0) <= ?"
                )
                params: list[Any] = [now_epoch]
                if requested_lane == "normal":
                    query += f" AND COALESCE(retry_count,0)<{heavy_after}"
                elif requested_lane == "fresh":
                    query += " AND COALESCE(retry_count,0)=0"
                elif requested_lane == "retry":
                    query += (
                        " AND COALESCE(retry_count,0)>0"
                        f" AND COALESCE(retry_count,0)<{heavy_after}"
                    )
                else:
                    query += f" AND COALESCE(retry_count,0)>={heavy_after}"

                # Spillover is a collision escape hatch, not a second scheduler.
                # Prefer rows whose logical bucket is currently busy.
                if busy_bucket_values and bool(
                    HEAVY_TAIL_BUCKET_SPILLOVER_ONLY_WHEN_BLOCKED
                ):
                    query += " AND bucket IN (" + ",".join(
                        "?" for _ in busy_bucket_values
                    ) + ")"
                    params.extend(busy_bucket_values)
                query += (
                    " ORDER BY CASE WHEN retry_avoid_node IS NULL OR retry_avoid_node != ? "
                    "THEN 0 ELSE 1 END, priority LIMIT 256"
                )
                params.append(int(node_index))
                candidates = list(self.conn.execute(query, params))

                selected: sqlite3.Row | None = None
                selected_work_dir = ""
                selected_resume_fallback = ""
                for row in candidates:
                    wallet = str(row["wallet"])
                    bucket = int(row["bucket"])
                    current_work_dir = str(row["work_dir"] or "").strip()
                    canonical_dir = cache_root / f"bucket_{bucket:03d}"
                    current_output = (
                        Path(current_work_dir)
                        if current_work_dir
                        else canonical_dir
                    )
                    current_key = os.path.normcase(
                        os.path.abspath(str(current_output))
                    )

                    if current_work_dir and current_key not in normalized_busy_dirs:
                        selected = row
                        selected_work_dir = str(current_output)
                        selected_resume_fallback = str(
                            row["resume_fallback_dir"] or ""
                        )
                        break

                    if not current_work_dir:
                        wallet_tag = hashlib.sha1(
                            wallet.encode("utf-8")
                        ).hexdigest()[:16]
                        spill_dir = cache_root / (
                            f"bucket_{bucket:03d}_"
                            f"{HEAVY_TAIL_BUCKET_SPILLOVER_PREFIX}_{wallet_tag}"
                        )
                        spill_key = os.path.normcase(
                            os.path.abspath(str(spill_dir))
                        )
                        if spill_key in normalized_busy_dirs:
                            continue
                        selected = row
                        selected_work_dir = str(spill_dir.resolve())
                        selected_resume_fallback = str(canonical_dir.resolve())
                        break

                if selected is None:
                    self.conn.commit()
                    return None

                wallet = str(selected["wallet"])
                bucket = int(selected["bucket"])
                legacy_dir = str(selected["legacy_dir"] or "")
                retry_count = int(selected["retry_count"] or 0)
                lane = (
                    "fresh"
                    if retry_count <= 0
                    else "retry"
                    if retry_count < heavy_after
                    else "heavy"
                )
                batch_id = (
                    f"{int(time.time())}-{node_index}-"
                    f"{hashlib.sha1(wallet.encode()).hexdigest()[:10]}"
                )
                self.conn.execute(
                    """
                    UPDATE wallet_queue
                    SET status='running', attempts=attempts+1, assigned_node=?,
                        batch_id=?, retry_avoid_node=NULL, work_dir=?,
                        resume_fallback_dir=?, updated_at=?
                    WHERE wallet=? AND active=1 AND status='pending'
                    """,
                    (
                        int(node_index),
                        batch_id,
                        selected_work_dir,
                        selected_resume_fallback,
                        now_epoch,
                        wallet,
                    ),
                )
                self.conn.commit()
                return (
                    batch_id,
                    bucket,
                    legacy_dir,
                    [_seed_from_json(str(selected["seed_json"]))],
                    lane,
                    retry_count,
                    selected_work_dir,
                    selected_resume_fallback,
                )
            except Exception:
                self._rollback_quietly()
                raise

    def finish_batch(
        self,
        wallets: set[str],
        completed: set[str],
        error: str,
        retry_avoid_node: int | None = None,
        failure_kind: str = "normal",
    ) -> tuple[int, int]:
        now = int(time.time())
        done = 0
        requeued = 0
        with self.lock, self.conn:
            for wallet in wallets:
                if wallet in completed:
                    cursor = self.conn.execute(
                        """
                        UPDATE wallet_queue
                        SET status='done', assigned_node=NULL, batch_id=NULL,
                            retry_avoid_node=NULL, retry_after=0,
                            completed_at=COALESCE(completed_at, ?), updated_at=?
                        WHERE wallet=? AND active=1
                        """,
                        (now, now, wallet),
                    )
                    done += int(cursor.rowcount or 0)
                    continue
                row = self.conn.execute(
                    "SELECT attempts,retry_count,unexpected_failures FROM wallet_queue WHERE wallet=?",
                    (wallet,),
                ).fetchone()
                attempts = int(row["attempts"] or 0) if row else 0
                retry_count = int(row["retry_count"] or 0) if row else 0
                unexpected_failures = int(row["unexpected_failures"] or 0) if row else 0
                final = bool(
                    not bool(GLOBAL_QUEUE_COMPLETE_ALL_WALLETS)
                    and GLOBAL_QUEUE_MAX_ATTEMPTS_PER_WALLET > 0
                    and attempts >= GLOBAL_QUEUE_MAX_ATTEMPTS_PER_WALLET
                )

                next_retry_count = retry_count
                next_unexpected = unexpected_failures
                retry_after = 0
                if failure_kind in ("retry76", "stall"):
                    next_retry_count += 1
                    if next_retry_count >= max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES)):
                        backoff = float(GLOBAL_QUEUE_HEAVY_RETRY_BACKOFF_SECONDS)
                    else:
                        backoff = min(
                            float(WORKER_RETRY_BACKOFF_MAX_SECONDS),
                            float(WORKER_RETRY_BACKOFF_BASE_SECONDS)
                            * (2 ** min(max(0, next_retry_count - 1), 5)),
                        )
                    jitter_cap = max(
                        0, int(float(GLOBAL_QUEUE_HEAVY_RETRY_JITTER_SECONDS))
                    )
                    jitter = (
                        int(hashlib.sha1(wallet.encode("utf-8")).hexdigest()[:8], 16)
                        % (jitter_cap + 1)
                        if jitter_cap > 0
                        and next_retry_count >= max(
                            1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES)
                        )
                        else 0
                    )
                    retry_after = now + int(max(1.0, backoff + jitter))
                elif failure_kind == "unexpected":
                    next_retry_count += 1
                    next_unexpected += 1
                    backoff = (
                        float(GLOBAL_QUEUE_HEAVY_RETRY_BACKOFF_SECONDS)
                        if next_retry_count >= max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES))
                        else float(WORKER_RETRY_BACKOFF_BASE_SECONDS)
                    )
                    jitter_cap = max(0, int(float(GLOBAL_QUEUE_HEAVY_RETRY_JITTER_SECONDS)))
                    jitter = (
                        int(hashlib.sha1(wallet.encode("utf-8")).hexdigest()[:8], 16)
                        % (jitter_cap + 1)
                        if jitter_cap > 0
                        and next_retry_count >= max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES))
                        else 0
                    )
                    retry_after = now + int(max(1.0, backoff + jitter))
                elif failure_kind == "proxy75":
                    next_retry_count += 1
                    backoff = (
                        float(GLOBAL_QUEUE_HEAVY_RETRY_BACKOFF_SECONDS)
                        if next_retry_count >= max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES))
                        else float(WORKER_RETRY_BACKOFF_BASE_SECONDS)
                    )
                    jitter_cap = max(0, int(float(GLOBAL_QUEUE_HEAVY_RETRY_JITTER_SECONDS)))
                    jitter = (
                        int(hashlib.sha1(wallet.encode("utf-8")).hexdigest()[:8], 16)
                        % (jitter_cap + 1)
                        if jitter_cap > 0
                        and next_retry_count >= max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES))
                        else 0
                    )
                    retry_after = now + int(max(1.0, backoff + jitter))

                self.conn.execute(
                    """
                    UPDATE wallet_queue
                    SET status=?, assigned_node=NULL, batch_id=NULL,
                        retry_avoid_node=?, retry_count=?, unexpected_failures=?,
                        retry_after=?, last_error=?, updated_at=?
                    WHERE wallet=?
                    """,
                    (
                        "failed_final" if final else "pending",
                        None if final else retry_avoid_node,
                        next_retry_count,
                        next_unexpected,
                        0 if final else retry_after,
                        error[:4000],
                        now,
                        wallet,
                    ),
                )
                if not final:
                    requeued += 1
        return done, requeued

    def sync_completed_from_dirs(
        self,
        directories: list[Path],
        min_tested_at_ms: int = 0,
    ) -> int:
        completed: set[str] = set()
        for directory in directories:
            completed |= load_test_memory_from_directory(
                directory,
                min_tested_at_ms=min_tested_at_ms,
            )
        return self.mark_done(completed)

    def retry_stats(self) -> dict[str, int]:
        """Return mutually exclusive fresh, ordinary-retry, and heavy counts."""
        now = int(time.time())
        heavy_after = max(1, int(GLOBAL_QUEUE_HEAVY_AFTER_RETRIES))
        with self.lock:
            row = self.conn.execute(
                """
                SELECT
                    SUM(CASE
                        WHEN status='pending' AND active=1
                         AND COALESCE(retry_count,0)=0
                        THEN 1 ELSE 0 END
                    ) AS fresh_pending,
                    SUM(CASE
                        WHEN status='pending' AND active=1
                         AND COALESCE(retry_count,0)>0
                         AND COALESCE(retry_count,0)<?
                         AND COALESCE(retry_after,0)<=?
                        THEN 1 ELSE 0 END
                    ) AS retry_ready,
                    SUM(CASE
                        WHEN status='pending' AND active=1
                         AND COALESCE(retry_count,0)>0
                         AND COALESCE(retry_count,0)<?
                         AND COALESCE(retry_after,0)>?
                        THEN 1 ELSE 0 END
                    ) AS retry_deferred,
                    SUM(CASE
                        WHEN status='pending' AND active=1
                         AND COALESCE(retry_count,0)>=?
                         AND COALESCE(retry_after,0)<=?
                        THEN 1 ELSE 0 END
                    ) AS heavy_ready,
                    SUM(CASE
                        WHEN status='pending' AND active=1
                         AND COALESCE(retry_count,0)>=?
                         AND COALESCE(retry_after,0)>?
                        THEN 1 ELSE 0 END
                    ) AS heavy_deferred,
                    MAX(COALESCE(retry_count,0)) AS max_retry_count
                FROM wallet_queue
                """,
                (
                    heavy_after,
                    now,
                    heavy_after,
                    now,
                    heavy_after,
                    now,
                    heavy_after,
                    now,
                ),
            ).fetchone()
        return {
            "fresh_pending": int(row["fresh_pending"] or 0),
            "retry_ready": int(row["retry_ready"] or 0),
            "retry_deferred": int(row["retry_deferred"] or 0),
            "heavy_ready": int(row["heavy_ready"] or 0),
            "heavy_deferred": int(row["heavy_deferred"] or 0),
            "max_retry_count": int(row["max_retry_count"] or 0),
        }

    def counts(self) -> dict[str, int]:
        counts = {"done": 0, "pending": 0, "running": 0, "failed": 0, "total": 0}
        with self.lock:
            rows = list(
                self.conn.execute(
                    "SELECT status, COUNT(*) FROM wallet_queue WHERE active=1 GROUP BY status"
                )
            )
        for row in rows:
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
        if bool(GLOBAL_QUEUE_COMPLETE_ALL_WALLETS):
            return counts["total"] > 0 and counts["done"] >= counts["total"]
        return counts["total"] > 0 and counts["done"] + counts["failed"] >= counts["total"]

    def close(self) -> None:
        with self.lock:
            self._rollback_quietly()
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
            for table in (
                "activity_state",
                "activity_scan_state",
                "activity_markets",
                "activity_trades",
                "closed_market_rows",
            ):
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
            result.extend(
                sorted(
                    path.resolve()
                    for pattern in ("bucket_*", f"{HEAVY_TAIL_BUCKET_SPILLOVER_PREFIX}_*")
                    for path in cache_root.glob(pattern)
                    if path.is_dir()
                )
            )
    # preserve order, remove duplicates
    seen: set[str] = set()
    unique: list[Path] = []
    for path in result:
        key = str(path).lower()
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _import_old_state(
    queue: GlobalQueueState,
    root: Path,
    fallback: Path | None,
    logger: RunLogRouter,
    min_tested_at_ms: int = 0,
    authoritative_memory_path: Path | None = None,
    reset_incomplete: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> list[Path]:
    sources = _discover_resume_sources(root, fallback)
    completed: set[str] = set()
    if authoritative_memory_path is not None:
        completed |= load_test_memory(
            authoritative_memory_path,
            min_tested_at_ms=min_tested_at_ms,
        )
    if progress_callback is not None:
        progress_callback(
            f"sources_discovered={len(sources)} scanned=0 "
            f"authoritative_completed={len(completed)}"
        )
    fallback_resolved = fallback.resolve() if fallback is not None else None
    for index, directory in enumerate(sources, start=1):
        completed_here = load_test_memory_from_directory(
            directory,
            min_tested_at_ms=min_tested_at_ms,
        )
        completed |= completed_here
        # Canonical bucket directories are deterministic from the wallet hash and
        # need no 109GB SQLite discovery scan. Only genuinely old part/fallback
        # layouts need wallet-to-directory migration.
        scan_legacy_cache = bool(
            directory.name.startswith("part_")
            or (
                fallback_resolved is not None
                and directory.resolve() == fallback_resolved
            )
        )
        cache_wallets = (
            _wallets_in_complete_cache(directory / COMPLETE_FETCH_CACHE_DB_FILE_NAME)
            if scan_legacy_cache
            else set()
        )
        if cache_wallets:
            queue.set_legacy_dir(cache_wallets, directory)
        if progress_callback is not None and (
            index == 1
            or index == len(sources)
            or (
                int(STARTUP_SCAN_PROGRESS_EVERY_SOURCES) > 0
                and index % int(STARTUP_SCAN_PROGRESS_EVERY_SOURCES) == 0
            )
        ):
            progress_callback(
                f"sources={index}/{len(sources)} completed_seen={len(completed)} "
                f"source_completed={len(completed_here)} cache_wallets={len(cache_wallets)} "
                f"cache_scan={'legacy' if scan_legacy_cache else 'skipped-canonical'} "
                f"current={directory.name}"
            )
    reconciliation = queue.reconcile_with_test_memory(
        completed,
        reset_incomplete=reset_incomplete,
    )
    logger.log(
        f"resume import sources={len(sources)} completed_seen={len(completed)} "
        f"reconciliation={reconciliation} min_tested_at_ms={min_tested_at_ms} "
        f"reset_incomplete={reset_incomplete}",
        source="QUEUE",
    )
    return sources


def _adaptive_heavy_share(queue_stats: dict[str, Any]) -> float:
    heavy_pending = int(queue_stats.get("heavy_ready", 0) or 0) + int(
        queue_stats.get("heavy_deferred", 0) or 0
    )
    normal_pending = (
        int(queue_stats.get("fresh_pending", 0) or 0)
        + int(queue_stats.get("retry_ready", 0) or 0)
        + int(queue_stats.get("retry_deferred", 0) or 0)
    )
    if heavy_pending <= 0:
        return 0.0
    if normal_pending <= 0:
        return 1.0
    low = max(0.0, min(1.0, float(GLOBAL_QUEUE_HEAVY_WORKER_SHARE_MIN)))
    high = max(low, min(1.0, float(GLOBAL_QUEUE_HEAVY_WORKER_SHARE_MAX)))
    backlog = max(1, int(GLOBAL_QUEUE_HEAVY_HIGH_BACKLOG))
    pressure = min(1.0, heavy_pending / float(backlog))
    return low + (high - low) * pressure


def _adaptive_heavy_worker_target(
    worker_limit: int,
    queue_stats: dict[str, Any],
) -> tuple[int, float]:
    worker_limit = max(1, int(worker_limit))
    share = _adaptive_heavy_share(queue_stats)
    if share <= 0.0:
        return 0, 0.0
    target = min(
        worker_limit,
        max(
            int(GLOBAL_QUEUE_HEAVY_MIN_WORKERS),
            int(math.ceil(worker_limit * share)),
        ),
    )
    return target, share


def _is_heavy_tail_mode(
    queue_counts: dict[str, Any],
    queue_stats: dict[str, Any],
) -> bool:
    unresolved = max(
        0,
        int(queue_counts.get("total", 0) or 0)
        - int(queue_counts.get("done", 0) or 0)
        - int(queue_counts.get("failed", 0) or 0),
    )
    normal_pending = (
        int(queue_stats.get("fresh_pending", 0) or 0)
        + int(queue_stats.get("retry_ready", 0) or 0)
        + int(queue_stats.get("retry_deferred", 0) or 0)
    )
    heavy_total = (
        int(queue_stats.get("heavy_ready", 0) or 0)
        + int(queue_stats.get("heavy_deferred", 0) or 0)
        + int(queue_counts.get("running", 0) or 0)
    )
    return bool(
        unresolved > 0
        and unresolved <= max(1, int(HEAVY_TAIL_TRIGGER_REMAINING))
        and normal_pending <= 0
        and heavy_total > 0
    )


def _heavy_tail_next_limit(
    current_limit: int,
    cpu_average: float,
    retry76_events: int,
) -> tuple[int, str]:
    current = max(int(HEAVY_TAIL_MIN_WORKERS), int(current_limit))
    current = min(current, int(HEAVY_TAIL_MAX_WORKERS))
    average = float(cpu_average)
    if average >= float(HEAVY_TAIL_CPU_HIGH_PERCENT):
        new_limit = max(
            int(HEAVY_TAIL_MIN_WORKERS),
            current - max(1, int(HEAVY_TAIL_SCALE_DOWN_STEP)),
        )
        return new_limit, f"down-{current-new_limit}"
    if (
        average < float(HEAVY_TAIL_CPU_LOW_PERCENT)
        and int(retry76_events) <= 0
    ):
        new_limit = min(
            int(HEAVY_TAIL_MAX_WORKERS),
            current + max(1, int(HEAVY_TAIL_SCALE_UP_STEP)),
        )
        return new_limit, f"up+{new_limit-current}"
    if average < float(HEAVY_TAIL_CPU_LOW_PERCENT) and int(retry76_events) > 0:
        return current, "hold-network-retries"
    return current, "hold"


def _heavy_vpn_rank_key(node: dict[str, Any]) -> tuple[float, float, int]:
    """Prefer real Polymarket activity/closed latency and penalize runtime failures."""
    activity_ms = safe_float(node.get("position_test_activity_ms"), float("inf"))
    closed_ms = safe_float(node.get("position_test_closed_ms"), float("inf"))
    base_ms = safe_float(node.get("speed_score_ms"), float("inf"))
    if not math.isfinite(activity_ms) or activity_ms <= 0:
        activity_ms = base_ms
    if not math.isfinite(closed_ms) or closed_ms <= 0:
        closed_ms = base_ms
    retry76 = int(node.get("runtime_retry76", 0) or 0)
    stalls = int(node.get("runtime_stalls", 0) or 0)
    unexpected = int(node.get("runtime_unexpected_exits", 0) or 0)
    runs = max(1, int(node.get("runtime_worker_runs", 0) or 0))
    done = int(node.get("runtime_wallet_done", 0) or 0)
    failure_rate = (retry76 + stalls + unexpected) / float(runs)
    success_bonus = min(done / float(runs), 1.0) * 250.0
    effective = (
        activity_ms * 0.70
        + closed_ms * 0.30
        + retry76 * 2500.0
        + stalls * 6000.0
        + unexpected * 10000.0
        + failure_rate * 3000.0
        - success_bonus
    )
    return effective, base_ms, int(node.get("source_index", 0) or 0)


def _vpn_rank_key(node: dict[str, Any]) -> tuple[float, int, int]:
    speed_score = safe_float(node.get("speed_score_ms"), float("inf"))
    failures = int(node.get("speed_failures", 0) or 0)
    source_index = int(node.get("source_index", 0) or 0)
    return speed_score, failures, source_index



def _vpn_startup_memory_key(link: str) -> str:
    return hashlib.sha256(str(link or "").strip().encode("utf-8")).hexdigest()


def _load_vpn_startup_test_memory(
    script_dir: Path,
) -> tuple[dict[str, dict[str, Any]], Path, str]:
    """Load exact-link startup-test results from the file beside this script."""
    path = script_dir / VPN_STARTUP_TEST_MEMORY_FILE_NAME
    if not path.exists():
        return {}, path, ""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("memory root is not a JSON object")
        if int(payload.get("version", -1)) != int(VPN_STARTUP_TEST_MEMORY_VERSION):
            raise ValueError(
                f"unsupported memory version={payload.get('version')!r}"
            )

        raw_entries = payload.get("entries")
        if not isinstance(raw_entries, dict):
            raise ValueError("memory entries is not an object")

        entries: dict[str, dict[str, Any]] = {}
        for key, row in raw_entries.items():
            if not isinstance(row, dict):
                continue
            link = str(row.get("link") or "").strip()
            expected_key = _vpn_startup_memory_key(link)
            if not link or str(key) != expected_key:
                continue
            entries[expected_key] = dict(row)
        return entries, path, ""
    except Exception as exc:
        # Corrupt/incompatible memory is ignored; all links are tested again.
        return {}, path, repr(exc)


def _memory_number(value: Any) -> float | None:
    number = safe_float(value, float("inf"))
    return number if math.isfinite(number) else None


def _vpn_startup_memory_record(node: dict[str, Any]) -> dict[str, Any]:
    link = str(node.get("link") or "").strip()
    working = bool(node.get("healthy") or node.get("standby"))
    return {
        "link": link,
        "linkHash": _vpn_startup_memory_key(link),
        "status": "working" if working else "failed",
        "everWorked": bool(node.get("ever_worked") or working),
        "rankingCandidate": bool(node.get("ranking_candidate") or node.get("ever_worked") or working),
        "rankingCyclePass": bool(node.get("ranking_cycle_pass", working)),
        "rankingFailures": int(node.get("ranking_failures", 0) or 0),
        "sourceIndex": int(node.get("source_index", 0) or 0),
        "name": str(node.get("name") or ""),
        "protocol": str(node.get("protocol") or ""),
        "ip": str(node.get("ip") or ""),
        "speedAvgMs": _memory_number(node.get("speed_avg_ms")),
        "speedScoreMs": _memory_number(node.get("speed_score_ms")),
        "speedSuccesses": int(node.get("speed_successes", 0) or 0),
        "speedFailures": int(node.get("speed_failures", 0) or 0),
        "speedTestedAt": str(node.get("speed_tested_at") or ""),
        "positionClosedMs": _memory_number(node.get("position_test_closed_ms")),
        "positionOpenMs": _memory_number(node.get("position_test_open_ms")),
        "positionActivityMs": _memory_number(node.get("position_test_activity_ms")),
        "lastError": str(node.get("last_error") or ""),
        "savedAt": _log_timestamp(),
    }


def _write_vpn_startup_test_memory(
    script_dir: Path,
    nodes: list[dict[str, Any]],
) -> Path:
    """Atomically replace memory with results for the links in the current list."""
    entries: dict[str, dict[str, Any]] = {}
    for node in nodes:
        link = str(node.get("link") or "").strip()
        if not link:
            continue
        key = _vpn_startup_memory_key(link)
        entries[key] = _vpn_startup_memory_record(node)

    payload = {
        "version": int(VPN_STARTUP_TEST_MEMORY_VERSION),
        "updatedAt": _log_timestamp(),
        "instructions": (
            "Delete this file to force a complete VPN startup retest. "
            "Unchanged exact links reuse their saved startup result."
        ),
        "entryCount": len(entries),
        "entries": entries,
    }
    path = script_dir / VPN_STARTUP_TEST_MEMORY_FILE_NAME
    _atomic_write_text(
        path,
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
        + "\n",
    )
    return path


def _apply_vpn_startup_memory(
    node: dict[str, Any],
    row: dict[str, Any],
) -> bool:
    """Apply one exact-link cached result without starting Xray or benchmarking."""
    link = str(node.get("link") or "").strip()
    if not link or str(row.get("link") or "").strip() != link:
        return False
    if str(row.get("linkHash") or "") != _vpn_startup_memory_key(link):
        return False

    status = str(row.get("status") or "").strip().lower()
    if status not in {"working", "failed"}:
        return False

    node["startup_tested"] = True
    ever_worked = (
        bool(row.get("everWorked"))
        or status == "working"
        or int(safe_float(row.get("speedSuccesses"), 0)) > 0
    )
    node["ever_worked"] = ever_worked
    node["ranking_candidate"] = bool(row.get("rankingCandidate")) or ever_worked
    node["ranking_cycle_pass"] = bool(
        row.get("rankingCyclePass", status == "working")
    )
    node["ranking_failures"] = int(safe_float(row.get("rankingFailures"), 0))
    node["ranking_testing"] = False
    node["ip"] = str(row.get("ip") or "")
    node["speed_avg_ms"] = safe_float(
        row.get("speedAvgMs"),
        float("inf"),
    )
    node["speed_score_ms"] = safe_float(
        row.get("speedScoreMs"),
        float("inf"),
    )
    node["speed_successes"] = int(safe_float(row.get("speedSuccesses"), 0))
    node["speed_failures"] = int(safe_float(row.get("speedFailures"), 0))
    node["speed_tested_at"] = str(row.get("speedTestedAt") or "")
    node["position_test_closed_ms"] = safe_float(
        row.get("positionClosedMs"), 0.0
    )
    node["position_test_open_ms"] = safe_float(
        row.get("positionOpenMs"), 0.0
    )
    node["position_test_activity_ms"] = safe_float(
        row.get("positionActivityMs"), 0.0
    )
    node["xray_proc"] = None
    node["xray_handle"] = None
    node["busy_batch"] = None
    node["retire_after_batch"] = False
    node["health_failures"] = 0

    if status == "working":
        # Saved-working nodes begin parked. Promotion performs a lightweight
        # real connection/IP check but skips the full Polymarket speed benchmark.
        node["healthy"] = False
        node["standby"] = True
        node["ranking_cycle_pass"] = True
        node["last_error"] = ""
        node["next_recheck"] = 0.0
    else:
        node["healthy"] = False
        node["standby"] = False
        node["last_error"] = str(
            row.get("lastError") or "failed in saved startup test"
        )
        node["next_recheck"] = (
            time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
        )
    return True


def _write_startup_sorted_vpn_snapshot(
    script_dir: Path,
    nodes: list[dict[str, Any]],
) -> Path:
    """Write one immutable-per-run snapshot of startup-working VPN links.

    The caller invokes this exactly once after startup testing and duplicate-IP
    elimination. The normal dynamic active_vpns.txt remains unchanged and can
    continue to reflect recoveries/failures during the run.
    """
    usable = sorted(
        [
            node
            for node in nodes
            if (node.get("healthy") or node.get("standby"))
            and str(node.get("link") or "").strip()
        ],
        key=_vpn_rank_key,
    )

    lines = [
        "# Generated once after the initial VPN test.",
        "# Sorted from best to worst by real wallet-position API score.",
        "# This file is not edited again until the program is started another time.",
        f"# Working unique VPNs: {len(usable)}",
        "",
    ]
    lines.extend(str(node["link"]).strip() for node in usable)

    snapshot_path = script_dir / STARTUP_SORTED_VPN_SNAPSHOT_FILE_NAME
    _atomic_write_text(snapshot_path, "\n".join(lines) + "\n")
    return snapshot_path


def _write_active_vpn_file(root: Path, nodes: list[dict[str, Any]]) -> None:
    # Every currently usable VPN is written:
    # ACTIVE  = Xray is running and may have a wallet batch.
    # STANDBY = it passed the Polymarket speed test but Xray is parked.
    active = [node for node in nodes if node.get("healthy")]
    standby = [node for node in nodes if node.get("standby")]
    usable = sorted(active + standby, key=_vpn_rank_key)
    unavailable_count = max(0, len(nodes) - len(usable))

    lines = [
        f"Updated: {_log_timestamp()}",
        f"Total configurations: {len(nodes)}",
        f"Working VPNs: {len(usable)}",
        f"Currently active/running: {len(active)}",
        f"Working standby: {len(standby)}",
        f"Unavailable/duplicate/not-yet-recovered: {unavailable_count}",
        "Ranking: lower wallet-position score_ms is better; each attempt requires closed-positions, positions and activity.",
        "",
    ]

    for rank, node in enumerate(usable, start=1):
        is_active = bool(node.get("healthy"))
        status = "ACTIVE" if is_active else "STANDBY"
        busy = node.get("busy_batch") or "idle"
        xray_state = "running" if is_active else "stopped"
        score = safe_float(node.get("speed_score_ms"), float("inf"))
        avg_ms = safe_float(node.get("speed_avg_ms"), float("inf"))
        score_text = f"{score:.1f}" if math.isfinite(score) else "unknown"
        avg_text = f"{avg_ms:.1f}" if math.isfinite(avg_ms) else "unknown"
        closed_ms = safe_float(node.get("position_test_closed_ms"), 0.0)
        open_ms = safe_float(node.get("position_test_open_ms"), 0.0)
        activity_ms = safe_float(node.get("position_test_activity_ms"), 0.0)
        lines.append(
            f"{rank}. status={status} node={node.get('source_index')} "
            f"protocol={node.get('protocol')} name={node.get('name')} "
            f"ip={node.get('ip')} score_ms={score_text} avg_ms={avg_text} "
            f"closed_ms={closed_ms:.1f} open_ms={open_ms:.1f} "
            f"activity_ms={activity_ms:.1f} "
            f"speed_ok={int(node.get('speed_successes', 0) or 0)} "
            f"speed_fail={int(node.get('speed_failures', 0) or 0)} "
            f"local_port={node.get('port')} xray={xray_state} work={busy}"
        )

    if not usable:
        lines.append(
            "No work-eligible VPN is available. Startup-never-working nodes are not continuously retested."
        )

    _atomic_write_text(root / ACTIVE_VPN_FILE_NAME, "\n".join(lines) + "\n")


def _deduplicated_paths(paths: Any) -> list[Path]:
    result: list[Path] = []
    seen: set[str] = set()
    for value in paths:
        path = Path(value)
        key = os.path.normcase(os.path.abspath(str(path)))
        if key in seen:
            continue
        seen.add(key)
        result.append(path)
    return result


def _collect_global_score_rows(
    root: Path,
    sources: list[Path],
    refresh_since_ms: int = 0,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, tuple[str, int]],
    list[Path],
]:
    """Collect the latest durable score rows without writing any output file."""
    current_statuses: dict[str, tuple[str, int]] = {}
    memory_sources = _deduplicated_paths(
        [root / TEST_MEMORY_FILE_NAME]
        + [
            path
            for directory in sources
            for path in test_memory_paths_for_directory(directory)
        ]
    )
    for memory_source in memory_sources:
        try:
            status_rows = load_test_memory_statuses(
                memory_source,
                min_tested_at_ms=refresh_since_ms,
            )
        except (OSError, csv.Error):
            continue
        for wallet, status_row in status_rows.items():
            previous = current_statuses.get(wallet)
            if previous is None or status_row[1] >= previous[1]:
                current_statuses[wallet] = status_row
    current_scored_wallets = {
        wallet
        for wallet, (status, _tested_at) in current_statuses.items()
        if status == "scored"
    }

    score_rows_by_wallet: dict[str, dict[str, Any]] = {}
    # Read the previously published root CSV first. Bucket journals then
    # override it, so a new durable score is never hidden by an older checkpoint.
    for directory in _deduplicated_paths([root, *sources]):
        score_sources = [directory / "edge_scores_progress.csv"]
        score_sources.extend(
            directory / name for name in LEGACY_SCORE_JOURNAL_FILE_NAMES
        )
        score_sources.append(directory / SCORE_JOURNAL_FILE_NAME)
        for score_source in score_sources:
            try:
                rows = load_progress_scores(score_source)
            except (OSError, csv.Error):
                # A Worker may be flushing the last journal line. The next live
                # checkpoint retries; already published rows stay untouched.
                continue
            for row in rows:
                wallet = str(row.get("proxyWallet") or "").lower()
                if wallet and wallet in current_scored_wallets:
                    score_rows_by_wallet[wallet] = row

    return score_rows_by_wallet, current_statuses, memory_sources


def _write_global_live_score_outputs(
    root: Path,
    sources: list[Path],
    *,
    refresh_since_ms: int = 0,
    write_csv: bool = True,
    write_xlsx: bool = True,
) -> dict[str, Any]:
    """Publish root score files while the global queue is still running."""
    score_rows_by_wallet, _statuses, _memory_sources = _collect_global_score_rows(
        root,
        sources,
        refresh_since_ms=refresh_since_ms,
    )

    result: dict[str, Any] = {
        "scored_rows": len(score_rows_by_wallet),
        "csv_method": "not-due",
        "xlsx_method": "not-due",
        "csv_error": "",
        "xlsx_error": "",
    }
    fieldnames = get_score_fieldnames()
    if write_csv:
        try:
            result["csv_method"] = write_sorted_scores_csv(
                score_rows_by_wallet.values(),
                root / "edge_scores_progress.csv",
                fieldnames,
            )
        except Exception as exc:
            result["csv_error"] = f"{type(exc).__name__}: {exc}"
    if write_xlsx:
        try:
            result["xlsx_method"] = write_scores_xlsx(
                score_rows_by_wallet.values(),
                root / "edge_scores.xlsx",
                fieldnames,
            )
        except Exception as exc:
            result["xlsx_error"] = f"{type(exc).__name__}: {exc}"
    return result


def _merge_global_outputs(
    root: Path,
    sources: list[Path],
    universe_file: Path,
    refresh_since_ms: int = 0,
) -> None:
    # Internal buckets and legacy parts are implementation details. User-facing files are written
    # directly into the single root folder.
    score_rows_by_wallet, current_statuses, memory_sources = _collect_global_score_rows(
        root,
        sources,
        refresh_since_ms=refresh_since_ms,
    )

    fieldnames = get_score_fieldnames()
    write_sorted_scores_csv(
        score_rows_by_wallet.values(),
        root / "edge_scores_progress.csv",
        fieldnames,
    )
    write_all_score_outputs(
        score_rows_by_wallet.values(),
        root / "edge_scores.xlsx",
        root,
        fieldnames,
    )

    merge_test_memory_files(
        memory_sources,
        root / TEST_MEMORY_FILE_NAME,
        min_tested_at_ms=refresh_since_ms,
    )
    merge_position_completeness_summaries(
        sources,
        root / POSITION_COMPLETENESS_LOG_FILE_NAME,
    )
    completed: set[str] = set(current_statuses)
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
        "refreshSinceMs": int(refresh_since_ms),
        "refreshFetchVersion": COMPLETE_FETCH_VERSION,
    }
    (root / "merge_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def run_global_queue_manager(args: argparse.Namespace) -> int:
    try:
        cpu_low_percent = float(CPU_AUTO_TUNE_LOW_PERCENT)
        cpu_high_percent = float(CPU_AUTO_TUNE_HIGH_PERCENT)
        cpu_window_seconds = float(CPU_AUTO_TUNE_WINDOW_SECONDS)
        cpu_sample_seconds = float(CPU_AUTO_TUNE_SAMPLE_INTERVAL_SECONDS)
        ram_stop_percent = float(RAM_SAFETY_STOP_PERCENT)
        ram_window_seconds = float(RAM_SAFETY_WINDOW_SECONDS)
        ram_sample_seconds = float(RAM_SAFETY_SAMPLE_INTERVAL_SECONDS)
    except (TypeError, ValueError) as exc:
        print(f"Invalid CPU auto-tune setting: {exc}", file=sys.stderr)
        return 2

    if not (0.0 <= cpu_low_percent < cpu_high_percent <= 100.0):
        print(
            "Invalid CPU thresholds: require "
            "0 <= CPU_AUTO_TUNE_LOW_PERCENT < "
            "CPU_AUTO_TUNE_HIGH_PERCENT <= 100.",
            file=sys.stderr,
        )
        return 2
    if cpu_window_seconds <= 0.0:
        print("CPU_AUTO_TUNE_WINDOW_SECONDS must be greater than 0.", file=sys.stderr)
        return 2
    if cpu_sample_seconds <= 0.0:
        print("CPU_AUTO_TUNE_SAMPLE_INTERVAL_SECONDS must be greater than 0.", file=sys.stderr)
        return 2
    if not (0.0 < ram_stop_percent <= 100.0):
        print("RAM_SAFETY_STOP_PERCENT must be in (0, 100].", file=sys.stderr)
        return 2
    if ram_window_seconds <= 0.0:
        print("RAM_SAFETY_WINDOW_SECONDS must be greater than 0.", file=sys.stderr)
        return 2
    if ram_sample_seconds <= 0.0:
        print("RAM_SAFETY_SAMPLE_INTERVAL_SECONDS must be greater than 0.", file=sys.stderr)
        return 2

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
    all_log_path = root / ALL_LOG_FILE_NAME
    error_log_path = root / ERROR_LOG_FILE_NAME
    diagnostic_path = root / DIAGNOSTIC_LOG_FILE_NAME
    worker_crash_path = root / WORKER_CRASH_LOG_FILE_NAME

    def existing_file_size(path: Path) -> int:
        try:
            return path.stat().st_size
        except OSError:
            return 0

    # Diagnostics must begin at this run's first newly appended byte. Defining
    # these paths and cursors before StartupReporter also prevents the v51
    # bootstrap NameError that caused a silent early exit.
    run_all_log_start_offset = existing_file_size(all_log_path)
    run_error_log_start_offset = existing_file_size(error_log_path)
    run_worker_crash_start_offset = existing_file_size(worker_crash_path)

    root_universe = root / "wallet_universe.csv"
    if universe_file.resolve() != root_universe.resolve():
        shutil.copy2(universe_file, root_universe)

    console_stdout = sys.stdout
    console_stderr = sys.stderr
    logger = RunLogRouter(all_log_path, error_log_path)
    logger.log(
        f"[build] id={BUILD_ID} script={Path(__file__).resolve()}",
        source="SYSTEM",
    )
    logger.log(
        f"[vpn-list] file={vpn_file} parsed_links={len(links)}",
        source="SYSTEM",
    )
    logger.log(
        f"[logging] all_logs={all_log_path} contains all normal lines and all errors; "
        f"errors={error_log_path} is the error-only subset",
        source="SYSTEM",
    )
    logger.log(
        f"[diagnostics] summary_file={diagnostic_path} "
        f"crash_file={worker_crash_path} "
        f"interval={DIAGNOSTIC_LOG_INTERVAL_SECONDS:g}s",
        source="SYSTEM",
    )
    logger.log(
        f"[console] live_in_place_dashboard=compact-4-lines refresh={CONSOLE_STATUS_INTERVAL_SECONDS:g}s "
        f"fallback_refresh={CONSOLE_FALLBACK_STATUS_INTERVAL_SECONDS:g}s",
        source="SYSTEM",
    )
    if CLEAN_CONSOLE_DASHBOARD:
        sys.stdout = RoutedLogStream(logger, "MANAGER")
        sys.stderr = RoutedLogStream(logger, "STDERR", force_error=True)

    startup = StartupReporter(
        logger,
        console_stdout,
        heartbeat_seconds=STARTUP_HEARTBEAT_SECONDS,
        summary_path=diagnostic_path,
        build_id=BUILD_ID,
    )
    startup.emit(
        "bootstrap",
        "ready",
        f"script={Path(__file__).resolve()} vpn_links={len(links)} "
        f"all_logs={all_log_path} errors={error_log_path} crashes={worker_crash_path} "
        + wallet_inclusion_policy_text(
            max_wallets=setting(args.max_wallets, MAX_WALLETS_TO_SCORE),
            min_positions=setting(args.min_positions, MIN_RESOLVED_POSITIONS),
            min_losses=setting(args.min_losses, MIN_LOSING_POSITIONS),
            min_pnl=setting(args.min_pnl, MIN_CLOSED_REALIZED_PNL),
        ),
    )

    queue_db_path = root / GLOBAL_QUEUE_DB_FILE_NAME
    with startup.step("queue_database", f"path={queue_db_path}") as startup_state:
        queue = GlobalQueueState(queue_db_path)
        startup_state["detail"] = f"path={queue_db_path} opened=true"

    manager_lock = ManagerInstanceLock(root / GLOBAL_MANAGER_LOCK_FILE_NAME)
    with startup.step("single_manager_lock", f"file={manager_lock.path}") as startup_state:
        if not manager_lock.acquire():
            startup_state["detail"] = (
                "acquired=false another_manager_is_already_running=true"
            )
            startup.emit(
                "single_manager_lock",
                "failed",
                "another Manager is already using this output folder; second run aborted",
            )
            queue.close()
            if CLEAN_CONSOLE_DASHBOARD:
                sys.stdout = console_stdout
                sys.stderr = console_stderr
            console_stderr.write(
                "Another polymarket Manager is already running for this output folder. "
                "Close it before starting a second copy.\n"
            )
            console_stderr.flush()
            logger.close()
            return 3
        startup_state["detail"] = f"acquired=true pid={os.getpid()}"

    with startup.step("persistent_wallet_memory") as startup_state:
        persistent_memory_state = initialize_persistent_test_memory(root)
        startup_state["detail"] = (
            f"file={persistent_memory_state['memory_path']} "
            f"reset_after_ms={persistent_memory_state['reset_after_ms']} "
            f"reset_requested={persistent_memory_state['reset_requested']} "
            f"first_migration={persistent_memory_state['first_migration']} "
            f"legacy_state_migration={persistent_memory_state.get('legacy_state_migration', False)} "
            "resume_policy=until-visible-memory-is-deleted"
        )
    memory_sync_state: dict[str, Any] = {
        "dirty": False,
        "last_sync": time.monotonic(),
        "rows": 0,
    }
    live_output_state: dict[str, Any] = {
        "csv_dirty": True,
        "xlsx_dirty": True,
        "last_csv_sync_monotonic": 0.0,
        "last_xlsx_sync_monotonic": 0.0,
        "last_csv_sync_epoch": 0.0,
        "last_xlsx_sync_epoch": 0.0,
        "last_attempt_epoch": 0.0,
        "scored_rows": 0,
        "csv_method": "not-run",
        "xlsx_method": "not-run",
        "last_error": "",
    }

    def pid_cleanup_text(summary: dict[str, Any]) -> str:
        return (
            f"tracked={int(summary.get('tracked', 0))} "
            f"processed={int(summary.get('processed', 0))} "
            f"signaled={int(summary.get('signaled', 0))} "
            f"already_gone={int(summary.get('not_running', 0))} "
            f"failed={int(summary.get('failed', 0)) + int(summary.get('permission_denied', 0))} "
            f"deadline_skipped={int(summary.get('deadline_skipped', 0))} "
            f"remaining_alive={int(summary.get('remaining_alive', 0))} "
            f"budget={float(STALE_PID_CLEANUP_TOTAL_TIMEOUT_SECONDS):g}s"
        )

    with startup.step(
        "stale_pid_cleanup",
        f"budget={float(STALE_PID_CLEANUP_TOTAL_TIMEOUT_SECONDS):g}s "
        "mode=os-kill-one-global-release-wait",
    ) as startup_state:
        def show_pid_cleanup_progress(summary: dict[str, Any]) -> None:
            detail = pid_cleanup_text(summary)
            startup_state["progress"] = detail
            startup.emit(
                "stale_pid_cleanup",
                "progress",
                detail,
                step_started=float(startup_state["started_monotonic"]),
            )

        stale_pid_summary = queue.kill_and_clear_stale_processes(
            max_seconds=STALE_PID_CLEANUP_TOTAL_TIMEOUT_SECONDS,
            progress_callback=show_pid_cleanup_progress,
        )
        startup_state["detail"] = pid_cleanup_text(stale_pid_summary)
        failures = stale_pid_summary.get("failures") or []
        if failures:
            logger.log(
                "[stale-pid-cleanup-warnings] " + " | ".join(map(str, failures)),
                source="STARTUP",
                force_error=True,
            )

    with startup.step("wallet_universe_load", f"file={universe_file}") as startup_state:
        wallets = sorted(
            load_wallet_universe(universe_file).values(),
            key=lambda item: item.best_pnl,
            reverse=True,
        )
        loaded_wallets = len(wallets)
        max_wallets = setting(args.max_wallets, MAX_WALLETS_TO_SCORE)
        if (
            not FULL_WALLET_INCLUSION_MODE
            and FILTER_MAX_WALLETS_TO_SCORE
            and max_wallets
        ):
            wallets = wallets[: int(max_wallets)]
        startup_state["detail"] = (
            f"file={universe_file} loaded={loaded_wallets} selected={len(wallets)}"
        )

    with startup.step("vpn_test_sample", f"wallets={len(wallets)}") as startup_state:
        position_test_wallets = [
            str(seed.proxy_wallet).strip().lower()
            for seed in wallets
            if str(seed.proxy_wallet).strip()
        ][: max(1, int(VPN_POSITION_TEST_SAMPLE_WALLETS))]
        startup_state["detail"] = f"sample_wallets={len(position_test_wallets)}"
    if not position_test_wallets:
        startup.emit(
            "vpn_test_sample",
            "failed",
            "reason=no-wallet-available",
        )
        print(
            "No wallet is available for the VPN wallet-position test.",
            file=sys.stderr,
        )
        queue.close()
        if CLEAN_CONSOLE_DASHBOARD:
            sys.stdout = console_stdout
            sys.stderr = console_stderr
        manager_lock.release()
        logger.close()
        return 2

    with startup.step("queue_seed", f"wallets={len(wallets)}") as startup_state:
        queue.seed(wallets)
        startup_state["detail"] = f"wallets={len(wallets)} seeded=true"

    with startup.step("interrupted_work_recovery") as startup_state:
        recovered_count = queue.recover_interrupted()
        startup_state["detail"] = f"requeued={recovered_count}"

    with startup.step(
        "refresh_cycle",
        f"enabled={bool(REFRESH_EXISTING_WALLETS_ON_EACH_RUN)} cycle_version={REFRESH_CYCLE_VERSION}",
    ) as startup_state:
        refresh_cycle = queue.begin_or_resume_refresh_cycle(
            REFRESH_CYCLE_VERSION,
            enabled=bool(REFRESH_EXISTING_WALLETS_ON_EACH_RUN),
        )
        startup_state["detail"] = (
            f"cycle={refresh_cycle.get('cycle_id')} "
            f"started_new={refresh_cycle.get('started_new')} "
            f"resumed={refresh_cycle.get('resumed')} "
            f"requeued={refresh_cycle.get('requeued')} "
            "automatic_refresh_disabled=true"
        )
    # This cutoff changes only when the visible wallet_test_memory.csv is
    # deleted. Reopening the script otherwise keeps exactly the same generation.
    refresh_since_ms = int(persistent_memory_state.get("reset_after_ms", 0) or 0)
    if GLOBAL_QUEUE_IMPORT_OLD_PARTS:
        with startup.step(
            "resume_state_import",
            f"refresh_since_ms={refresh_since_ms}",
        ) as startup_state:
            def show_resume_progress(detail: str) -> None:
                startup_state["progress"] = detail
                startup.emit(
                    "resume_state_import",
                    "progress",
                    detail,
                    step_started=float(startup_state["started_monotonic"]),
                )

            legacy_sources = _import_old_state(
                queue,
                root,
                fallback,
                logger,
                min_tested_at_ms=refresh_since_ms,
                authoritative_memory_path=(root / TEST_MEMORY_FILE_NAME),
                reset_incomplete=bool(
                    persistent_memory_state.get("reset_requested")
                ),
                progress_callback=show_resume_progress,
            )
            startup_memory_sources = [root / TEST_MEMORY_FILE_NAME]
            startup_memory_sources.extend(
                path
                for directory in legacy_sources
                for path in test_memory_paths_for_directory(directory)
            )
            compacted_memory_rows = merge_test_memory_files(
                startup_memory_sources,
                root / TEST_MEMORY_FILE_NAME,
                min_tested_at_ms=refresh_since_ms,
            )
            startup_state["detail"] = (
                f"sources={len(legacy_sources)} import_complete=true "
                f"persistent_rows={compacted_memory_rows}"
            )
    else:
        legacy_sources = []
        queue.reconcile_with_test_memory(
            load_test_memory(
                root / TEST_MEMORY_FILE_NAME,
                min_tested_at_ms=refresh_since_ms,
            ),
            reset_incomplete=bool(persistent_memory_state.get("reset_requested")),
        )
        startup.emit("resume_state_import", "skipped", "setting=false")

    startup.emit(
        "queue_ready",
        "done",
        f"wallets={len(wallets)} persistent_memory_rows={queue.counts().get('done', 0)} "
        f"reset_after_ms={refresh_since_ms}",
    )

    logger.log("=" * 80, source="SYSTEM")
    logger.log(
        f"global queue run started wallets={len(wallets)} "
        f"interrupted_requeued={recovered_count} "
        f"persistent_memory={root / TEST_MEMORY_FILE_NAME} "
        f"memory_reset_after_ms={refresh_since_ms} "
        f"memory_reset_requested={persistent_memory_state.get('reset_requested')} "
        "resume_until_memory_deleted=true",
        source="SYSTEM",
    )
    console_stdout.write(
        f"Global queue ready | Build: {BUILD_ID}\n"
        f"  All logs:    {root / ALL_LOG_FILE_NAME}\n"
        f"  Errors:      {root / ERROR_LOG_FILE_NAME}\n"
        f"  Diagnostics: {root / DIAGNOSTIC_LOG_FILE_NAME}\n"
        f"  Active VPNs: {root / ACTIVE_VPN_FILE_NAME}\n"
        f"Startup settings\n"
        f"  VPN test workers: {VPN_STARTUP_TEST_WORKERS}\n"
        f"  Starting active VPNs: {VPN_MAX_ACTIVE_NODES} | Scale-up step: +{VPN_AUTO_TUNE_UP_STEP}\n"
        f"  CPU auto tune: {'ON' if CPU_AUTO_TUNE_ENABLED else 'OFF'} | "
        f"every {CPU_AUTO_TUNE_WINDOW_SECONDS:g}s: +{VPN_AUTO_TUNE_UP_STEP} when avg CPU "
        f"< {CPU_AUTO_TUNE_LOW_PERCENT:.1f}%, stable from {CPU_AUTO_TUNE_LOW_PERCENT:.1f}% "
        f"to below {CPU_AUTO_TUNE_HIGH_PERCENT:.1f}%, -1 when avg >= {CPU_AUTO_TUNE_HIGH_PERCENT:.1f}%\n"
        f"  Natural cap: number of VPNs that actually pass testing\n"
        f"  Dead recheck: {'ON' if VPN_DEAD_RECHECK_ENABLED else 'OFF'} | "
        f"Continuous sort: {'ON' if VPN_CONTINUOUS_SORT_ENABLED else 'OFF'}\n"
        f"  Reality quarantine: {XRAY_REALITY_CERT_ERROR_THRESHOLD} errors / "
        f"{XRAY_REALITY_CERT_ERROR_WINDOW_SECONDS:g}s -> remove for this run\n"
        f"  RAM safety: {'ON' if RAM_SAFETY_STOP_ENABLED else 'OFF'} | "
        f"avg {RAM_SAFETY_WINDOW_SECONDS:g}s > {RAM_SAFETY_STOP_PERCENT:.1f}% -> safe stop\n"
        f"  Complete-all: {'ON' if GLOBAL_QUEUE_COMPLETE_ALL_WALLETS else 'OFF'} | "
        f"retry batch={GLOBAL_QUEUE_RETRY_BATCH_SIZE} | heavy after {GLOBAL_QUEUE_HEAVY_AFTER_RETRIES} retries\n"
        f"  Wallet resume: PERSISTENT | completed={queue.counts().get('done', 0)} | "
        f"reset_after_ms={refresh_since_ms or 'none'} | reset only by deleting {TEST_MEMORY_FILE_NAME}\n"
        f"  Heavy lane: batch={GLOBAL_QUEUE_HEAVY_BATCH_SIZE} | "
        f"adaptive heavy share={GLOBAL_QUEUE_HEAVY_WORKER_SHARE_MIN:.0%}-"
        f"{GLOBAL_QUEUE_HEAVY_WORKER_SHARE_MAX:.0%} | unlimited attempts\n"
        f"  Heavy tail: trigger<={HEAVY_TAIL_TRIGGER_REMAINING} unresolved | "
        f"workers={HEAVY_TAIL_INITIAL_WORKERS} adaptive "
        f"{HEAVY_TAIL_MIN_WORKERS}-{HEAVY_TAIL_MAX_WORKERS} | "
        f"bucket_spillover={HEAVY_TAIL_BUCKET_SPILLOVER_ENABLED} | "
        f"per-heavy HTTP={HEAVY_CLOSED_FETCH_WORKERS}+"
        f"{HEAVY_ACTIVITY_FETCH_WORKERS}+{HEAVY_ACTIVITY_WINDOW_WORKERS}\n"
        f"  Wallet VPN test: {'ON' if VPN_SPEED_RANKING_ENABLED else 'OFF'} | "
        f"samples={len(position_test_wallets)} endpoints=closed/positions/activity\n"
    )
    console_stdout.flush()

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    script_path = Path(__file__).resolve()
    nodes: list[dict[str, Any]] = []
    node_runtime_locks: list[threading.RLock] = []
    worker_states: dict[int, dict[str, Any]] = {}
    output_threads: list[threading.Thread] = []
    continuous_sort_stop = threading.Event()
    continuous_sort_thread: threading.Thread | None = None
    continuous_sort_events: deque[dict[str, Any]] = deque()
    continuous_sort_events_lock = threading.Lock()
    continuous_sort_progress_lock = threading.Lock()
    continuous_sort_apply_lock = threading.RLock()
    continuous_sort_progress: dict[str, Any] = {
        "running": False,
        "cycle": 0,
        "tested": 0,
        "total": 0,
        "passed": 0,
        "failed": 0,
    }
    # Dead-node retesting is completely detached from the wallet-manager loop.
    # The main queue can launch and finish wallet workers while these tests run.
    dead_recheck_stop = threading.Event()
    dead_recheck_thread: threading.Thread | None = None
    dead_recheck_events: deque[dict[str, Any]] = deque()
    dead_recheck_events_lock = threading.Lock()
    dead_recheck_progress_lock = threading.Lock()
    dead_recheck_progress: dict[str, Any] = {
        "running": False,
        "cycle": 0,
        "tested": 0,
        "total": 0,
        "passed": 0,
        "failed": 0,
    }
    # Active VPN wallet-position health checks also run outside the manager loop.
    active_health_stop = threading.Event()
    active_health_thread: threading.Thread | None = None
    active_health_events: deque[dict[str, Any]] = deque()
    active_health_events_lock = threading.Lock()
    reality_error_events: dict[int, deque[float]] = {}
    reality_error_lock = threading.Lock()
    reality_quarantine_pending: set[int] = set()
    last_dashboard = 0.0
    last_error_notice = time.monotonic()
    last_active_file = 0.0
    last_console_width = 0
    console_output_lock = threading.RLock()

    def _enable_virtual_terminal_processing(stream: Any) -> bool:
        """Enable ANSI cursor movement on modern Windows consoles; safely fall back."""
        try:
            if not bool(stream.isatty()):
                return False
        except Exception:
            return False
        if os.name != "nt":
            return True
        try:
            import ctypes
            import msvcrt

            handle = msvcrt.get_osfhandle(stream.fileno())
            mode = ctypes.c_uint()
            kernel32 = ctypes.windll.kernel32
            if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
                return False
            enable_virtual_terminal_processing = 0x0004
            return bool(
                kernel32.SetConsoleMode(
                    handle,
                    mode.value | enable_virtual_terminal_processing,
                )
            )
        except Exception:
            return False

    live_dashboard_enabled = _enable_virtual_terminal_processing(console_stdout)
    live_dashboard_rendered = False
    live_dashboard_lines: list[str] = []
    live_dashboard_rate_samples: deque[tuple[float, int]] = deque()

    def _format_eta_minutes(total_minutes: float | None) -> str:
        if total_minutes is None or not math.isfinite(float(total_minutes)):
            return "calculating"
        minutes = max(0, int(math.ceil(float(total_minutes))))
        days, remainder = divmod(minutes, 24 * 60)
        hours, mins = divmod(remainder, 60)
        return f"{days}d {hours}h {mins}m"

    def _fit_dashboard_line(value: str) -> str:
        try:
            columns = int(shutil.get_terminal_size((180, 40)).columns)
        except Exception:
            columns = 180
        width = max(50, columns - 1)
        value = str(value).replace("\r", " ").replace("\n", " ")
        if len(value) <= width:
            return value
        return value[: max(1, width - 3)] + "..."

    def _clear_live_dashboard_locked() -> None:
        nonlocal live_dashboard_rendered
        if not live_dashboard_enabled or not live_dashboard_rendered:
            return
        line_count = max(1, len(live_dashboard_lines))
        console_stdout.write("\r")
        if line_count > 1:
            console_stdout.write(f"\x1b[{line_count - 1}A")
        for index in range(line_count):
            console_stdout.write("\x1b[2K")
            if index < line_count - 1:
                console_stdout.write("\n")
        if line_count > 1:
            console_stdout.write(f"\x1b[{line_count - 1}A")
        console_stdout.write("\r")
        live_dashboard_rendered = False

    def _render_live_dashboard_locked(lines: list[str]) -> None:
        nonlocal live_dashboard_rendered, live_dashboard_lines
        fitted = [_fit_dashboard_line(line) for line in lines]
        if not fitted:
            return
        if not live_dashboard_enabled:
            console_stdout.write("\n".join(fitted) + "\n")
            console_stdout.flush()
            live_dashboard_lines = fitted
            return

        previous_count = max(1, len(live_dashboard_lines))
        if live_dashboard_rendered:
            console_stdout.write("\r")
            if previous_count > 1:
                console_stdout.write(f"\x1b[{previous_count - 1}A")
        for index, line in enumerate(fitted):
            console_stdout.write("\x1b[2K\r" + line)
            if index < len(fitted) - 1:
                console_stdout.write("\n")
        live_dashboard_lines = fitted
        live_dashboard_rendered = True
        console_stdout.flush()

    def _write_console_message_locked(message: str) -> None:
        saved_lines = list(live_dashboard_lines)
        had_dashboard = bool(live_dashboard_enabled and live_dashboard_rendered)
        if had_dashboard:
            _clear_live_dashboard_locked()
        payload = str(message).rstrip("\r\n")
        if payload:
            console_stdout.write(payload + "\n")
        else:
            console_stdout.write("\n")
        if had_dashboard and saved_lines:
            _render_live_dashboard_locked(saved_lines)
        else:
            console_stdout.flush()

    def _finish_live_dashboard() -> None:
        nonlocal live_dashboard_rendered
        with console_output_lock:
            if live_dashboard_enabled and live_dashboard_rendered:
                console_stdout.write("\n")
                console_stdout.flush()
                live_dashboard_rendered = False

    diagnostic_lock = threading.RLock()
    diagnostic_started_monotonic = time.monotonic()
    diagnostic_last_snapshot = 0.0
    diagnostic_previous_snapshot = diagnostic_started_monotonic
    with startup.step("initial_queue_snapshot") as startup_state:
        diagnostic_previous_counts = queue.counts()
        startup_state["detail"] = (
            f"total={diagnostic_previous_counts.get('total', 0)} "
            f"pending={diagnostic_previous_counts.get('pending', 0)} "
            f"done={diagnostic_previous_counts.get('done', 0)}"
        )
    diagnostic_last_progress = diagnostic_started_monotonic
    diagnostic_previous_data_bytes = 0
    diagnostic_error_path = error_log_path
    diagnostic_all_log_offset = int(run_all_log_start_offset)
    diagnostic_error_offset = int(run_error_log_start_offset)
    diagnostic_worker_crash_offset = int(run_worker_crash_start_offset)
    # Separate cursor for 30-second network diagnostics shown beside CPU tuning.
    # These counters never alter the runtime limit and never interfere with the
    # 3-minute diagnostic reader.
    cpu_guard_error_offset = diagnostic_error_offset

    # Runtime values always move together. The source-code settings above remain
    # the starting point for every new program run.
    # وقتی بررسی مجدد VPN مرده خاموش است، تعداد Workerهای dead-recheck نباید
    # ظرفیت VPNهای سالم را محدود کند.
    initial_parallelism_setting = (
        min(int(VPN_MAX_ACTIVE_NODES), int(VPN_DEAD_RECHECK_WORKERS))
        if VPN_DEAD_RECHECK_ENABLED
        else int(VPN_MAX_ACTIVE_NODES)
    )
    # عدد VPN_MAX_ACTIVE_NODES فقط نقطه شروع است. Auto Tune هیچ حداقل/حداکثر
    # قابل‌تنظیمی ندارد؛ فقط از محدوده طبیعی 1 تا تعداد VPNهای موجود خارج نمی‌شود.
    cpu_parallelism = max(1, int(initial_parallelism_setting))
    heavy_tail_parallelism = max(
        int(HEAVY_TAIL_MIN_WORKERS),
        min(
            int(HEAVY_TAIL_INITIAL_WORKERS),
            int(HEAVY_TAIL_MAX_WORKERS),
            int(cpu_parallelism),
        ),
    )
    cpu_monitor = CpuPeakMonitor(CPU_AUTO_TUNE_SAMPLE_INTERVAL_SECONDS)
    ram_monitor = RamSafetyMonitor(
        RAM_SAFETY_SAMPLE_INTERVAL_SECONDS,
        RAM_SAFETY_WINDOW_SECONDS,
        RAM_SAFETY_STOP_PERCENT,
    )
    ram_safety_triggered = False
    ram_safety_message = ""
    next_cpu_tune = 0.0
    last_cpu_window_peak = 0.0
    last_cpu_window_average = 0.0
    last_cpu_all_time_peak = 0.0
    retry76_events_since_cpu_tune = 0
    # Completion recency is diagnostic-only. CPU auto-tune never uses it to
    # block an increase or schedule a delayed decrease.
    last_durable_completion_monotonic = diagnostic_started_monotonic
    durable_completion_seen_this_run = False
    last_worker_stall_check = 0.0

    def _diagnostic_append(lines: list[str]) -> None:
        try:
            ensure_dir(diagnostic_path.parent)
            payload = "\n".join(lines).rstrip() + "\n"
            with diagnostic_lock:
                with diagnostic_path.open("a", encoding="utf-8") as file:
                    file.write(payload)
                    file.flush()
        except Exception:
            logger.log(
                "[diagnostic:write-failed]\n" + traceback.format_exc(),
                source="DIAG",
                force_error=True,
            )

    def _diagnostic_error_summary() -> tuple[dict[str, int], list[str], int, bool]:
        nonlocal diagnostic_error_offset
        categories = {
            "total_lines": 0,
            "timeout": 0,
            "connection_aborted_10053": 0,
            "connection_reset_or_closed": 0,
            "ssl_tls": 0,
            "dns": 0,
            "http_429_rate_limit": 0,
            "http_403_blocked": 0,
            "worker_exit_75": 0,
            "worker_retry_76": 0,
            "worker_exit_other": 0,
            "promotion_failed": 0,
            "proxy_dead": 0,
            "duplicate_ip": 0,
            "sqlite": 0,
            "fatal": 0,
            "worker_traceback": 0,
            "xray_dial": 0,
            "output_write": 0,
        }
        samples: deque[str] = deque(
            maxlen=max(1, int(DIAGNOSTIC_ERROR_SAMPLE_LINES))
        )
        truncated = False
        try:
            size = diagnostic_error_path.stat().st_size
            if size < diagnostic_error_offset:
                diagnostic_error_offset = 0
            max_read = 8 * 1024 * 1024
            start = diagnostic_error_offset
            if size - start > max_read:
                start = size - max_read
                truncated = True
            with diagnostic_error_path.open("rb") as file:
                file.seek(start)
                raw = file.read()
                diagnostic_error_offset = file.tell()
            lines = raw.decode("utf-8", errors="replace").splitlines()
        except OSError:
            lines = []

        for line in lines:
            cleaned = line.strip()
            if not cleaned:
                continue
            lower = cleaned.lower()
            categories["total_lines"] += 1
            if "timeout" in lower or "timed out" in lower or "i/o timeout" in lower:
                categories["timeout"] += 1
            if "10053" in lower or "connectionabortederror" in lower:
                categories["connection_aborted_10053"] += 1
            if any(token in lower for token in (
                "connection reset", "connection refused", "connection closed",
                "remote end closed", "broken pipe", "protocolerror('connection aborted",
            )):
                categories["connection_reset_or_closed"] += 1
            if any(token in lower for token in (
                "ssl", "tls", "certificate", "handshake", "curvepreferences",
            )):
                categories["ssl_tls"] += 1
            if any(token in lower for token in (
                "no such host", "name or service not known", "temporary failure in name resolution",
                "getaddrinfo", "lookup ", "dns",
            )):
                categories["dns"] += 1
            if " 429" in lower or "status=429" in lower or "rate limit" in lower or "too many requests" in lower:
                categories["http_429_rate_limit"] += 1
            if " 403" in lower or "status=403" in lower or "forbidden" in lower or "cloudflare" in lower:
                categories["http_403_blocked"] += 1
            if "worker:finish" in lower and "exit=75" in lower:
                categories["worker_exit_75"] += 1
            elif (
                ("worker:finish" in lower and "exit=76" in lower)
                or "[worker:retry-required]" in lower
                or "[worker:retry-reroute]" in lower
            ):
                categories["worker_retry_76"] += 1
            elif "worker:finish" in lower and "exit=" in lower and "exit=0" not in lower:
                categories["worker_exit_other"] += 1
            if "proxy:promotion-failed" in lower:
                categories["promotion_failed"] += 1
            if "proxy:dead" in lower:
                categories["proxy_dead"] += 1
            if "duplicate outbound ip" in lower:
                categories["duplicate_ip"] += 1
            if "sqlite" in lower or "database is locked" in lower or "not an error" in lower:
                categories["sqlite"] += 1
            if "[fatal]" in lower:
                categories["fatal"] += 1
            elif "traceback" in lower:
                categories["worker_traceback"] += 1
            if "failed to dial" in lower or "deadxray" in lower or "sortxray" in lower:
                categories["xray_dial"] += 1
            if "[output:checkpoint-failed]" in lower:
                categories["output_write"] += 1
            samples.append(cleaned[:700])
        return categories, list(samples), len(lines), truncated

    def _diagnostic_runtime_summary(
        self_contained_limit: int = DIAGNOSTIC_RUNTIME_SAMPLE_LINES,
    ) -> tuple[dict[str, int], dict[str, int], list[str], int, bool]:
        """Summarize non-error progress so diagnostics alone shows where work went."""
        nonlocal diagnostic_all_log_offset
        categories = {
            "wallet_started": 0,
            "wallet_resume_skipped": 0,
            "fetch_failed": 0,
            "positions_direct_pages": 0,
            "positions_complete": 0,
            "current_position_pages": 0,
            "current_position_retries": 0,
            "activity_progress": 0,
            "activity_retries": 0,
            "market_batch_progress": 0,
            "market_batch_retries": 0,
            "worker_finish_ok": 0,
            "worker_finish_nonzero": 0,
            "worker_stall_reroutes": 0,
            "worker_retry_reroutes": 0,
            "proxy_promotions": 0,
            "proxy_dead": 0,
            "cpu_tunes": 0,
            "output_checkpoints": 0,
            "output_checkpoint_failures": 0,
        }
        source_counts: dict[str, int] = {}
        recent: deque[str] = deque(maxlen=max(1, int(self_contained_limit)))
        truncated = False
        try:
            size = all_log_path.stat().st_size
            if size < diagnostic_all_log_offset:
                diagnostic_all_log_offset = 0
            max_read = max(1024, int(DIAGNOSTIC_MAX_INCREMENTAL_READ_BYTES))
            start = diagnostic_all_log_offset
            if size - start > max_read:
                start = size - max_read
                truncated = True
            with all_log_path.open("rb") as file:
                file.seek(start)
                raw = file.read()
                diagnostic_all_log_offset = file.tell()
            lines = raw.decode("utf-8", errors="replace").splitlines()
        except OSError:
            lines = []

        significant_tokens = (
            "[startup]",
            "[closed]",
            "[skip]",
            "[fetch-failed]",
            "[positions:done]",
            "[positions-complete]",
            "[positions:fallback]",
            "[current-positions:done]",
            "[current-positions:incomplete]",
            "[trade-markets:done]",
            "[trades-window:retry]",
            "[trade-set-verify]",
            "[activity-reconstruct]",
            "[market-batch:retry]",
            "[worker:finish]",
            "[worker:crash-captured]",
            "[worker:stall-reroute]",
            "[worker:retry-reroute]",
            "[proxy:promotion]",
            "[proxy:dead]",
            "[cpu:auto-tune]",
            "[output:checkpoint]",
            "[output:checkpoint-failed]",
            "[queue:summary]",
            "[fatal]",
        )
        for line in lines:
            cleaned = line.strip()
            if not cleaned:
                continue
            lower = cleaned.lower()
            source_match = re.match(r"^\[[^\]]+\]\s+\[([^\]]+)\]", cleaned)
            if source_match:
                source = source_match.group(1)
                source_counts[source] = source_counts.get(source, 0) + 1
            if "[closed]" in lower:
                categories["wallet_started"] += 1
            if "[skip]" in lower:
                categories["wallet_resume_skipped"] += 1
            if "[fetch-failed]" in lower:
                categories["fetch_failed"] += 1
            if "[positions-direct]" in lower:
                categories["positions_direct_pages"] += 1
            if "[positions:done]" in lower or "[positions-complete]" in lower:
                categories["positions_complete"] += 1
            if "[current-positions]" in lower or "[current-positions:done]" in lower:
                categories["current_position_pages"] += 1
            if "[current-positions:retry]" in lower:
                categories["current_position_retries"] += 1
            if "[activity-" in lower or "[trade-markets" in lower or "[trade-set-verify]" in lower:
                categories["activity_progress"] += 1
            if "[activity-window:retry]" in lower or "[trades-window:retry]" in lower:
                categories["activity_retries"] += 1
            if "[market-batch" in lower or "[market-batches" in lower:
                categories["market_batch_progress"] += 1
            if "[market-batch:retry]" in lower:
                categories["market_batch_retries"] += 1
            if "[worker:finish]" in lower:
                if "exit=0" in lower:
                    categories["worker_finish_ok"] += 1
                else:
                    categories["worker_finish_nonzero"] += 1
            if "[worker:stall-reroute]" in lower:
                categories["worker_stall_reroutes"] += 1
            if "[worker:retry-reroute]" in lower:
                categories["worker_retry_reroutes"] += 1
            if "[proxy:promotion" in lower and "failed" not in lower:
                categories["proxy_promotions"] += 1
            if "[proxy:dead]" in lower:
                categories["proxy_dead"] += 1
            if "[cpu:auto-tune]" in lower:
                categories["cpu_tunes"] += 1
            if "[output:checkpoint]" in lower:
                categories["output_checkpoints"] += 1
            if "[output:checkpoint-failed]" in lower:
                categories["output_checkpoint_failures"] += 1
            if any(token in lower for token in significant_tokens):
                recent.append(cleaned[:900])
        return categories, source_counts, list(recent), len(lines), truncated

    def _diagnostic_worker_crash_summary() -> tuple[int, list[str], bool]:
        """Count newly appended crash blocks and retain their identities/causes."""
        nonlocal diagnostic_worker_crash_offset
        truncated = False
        try:
            size = worker_crash_path.stat().st_size
            if size < diagnostic_worker_crash_offset:
                diagnostic_worker_crash_offset = 0
            max_read = max(1024, int(DIAGNOSTIC_MAX_INCREMENTAL_READ_BYTES))
            start = diagnostic_worker_crash_offset
            if size - start > max_read:
                start = size - max_read
                truncated = True
            with worker_crash_path.open("rb") as file:
                file.seek(start)
                raw = file.read()
                diagnostic_worker_crash_offset = file.tell()
            text = raw.decode("utf-8", errors="replace")
        except OSError:
            text = ""
        blocks = [
            block.strip()
            for block in text.split("=" * 100)
            if "WORKER CRASH" in block
        ]
        summaries: deque[str] = deque(maxlen=5)
        for block in blocks:
            lines = [line.strip() for line in block.splitlines() if line.strip()]
            identity = " | ".join(
                line
                for line in lines
                if line.startswith("[")
                or line.startswith("node=")
                or line.startswith("batch=")
            )
            cause = ""
            for line in reversed(lines):
                if re.search(r"[A-Za-z_][\w.]*(?:Error|Exception):", line):
                    cause = line
                    break
            summaries.append(
                (identity + (" | cause=" + cause if cause else ""))[:1200]
            )
        return len(blocks), list(summaries), truncated

    def _cpu_autotune_network_guard() -> dict[str, int]:
        """Read only new error-log lines and detect network/socket pressure.

        CPU can remain low while HTTP threads are blocked in sockets. Without
        this guard, low CPU incorrectly causes more VPN workers to be added,
        multiplying connections and making Windows 10053 failures worse.
        """
        nonlocal cpu_guard_error_offset
        counts = {
            "lines": 0,
            "abort_10053": 0,
            "timeouts": 0,
            "worker_exits": 0,
            "connection_closed": 0,
        }
        try:
            size = diagnostic_error_path.stat().st_size
            if size < cpu_guard_error_offset:
                cpu_guard_error_offset = 0
            max_read = 2 * 1024 * 1024
            start = cpu_guard_error_offset
            if size - start > max_read:
                start = size - max_read
            with diagnostic_error_path.open("rb") as file:
                file.seek(start)
                raw = file.read()
                cpu_guard_error_offset = file.tell()
            lines = raw.decode("utf-8", errors="replace").splitlines()
        except OSError:
            lines = []

        for line in lines:
            lower = line.lower()
            if not lower.strip():
                continue
            counts["lines"] += 1
            if "10053" in lower or "connectionabortederror" in lower:
                counts["abort_10053"] += 1
            if "timeout" in lower or "timed out" in lower or "i/o timeout" in lower:
                counts["timeouts"] += 1
            if "worker:finish" in lower and "exit=" in lower and "exit=0" not in lower:
                counts["worker_exits"] += 1
            if any(token in lower for token in (
                "connection reset", "connection refused", "connection closed",
                "remote end closed", "broken pipe", "protocolerror('connection aborted",
            )):
                counts["connection_closed"] += 1
        return counts

    def _diagnostic_data_footprint() -> tuple[int, int]:
        total_bytes = 0
        files_count = 0
        wanted_suffixes = {".jsonl", ".sqlite3", ".csv", ".xlsx"}
        try:
            for directory, _subdirs, filenames in os.walk(root):
                for filename in filenames:
                    path = Path(directory) / filename
                    if path.suffix.lower() not in wanted_suffixes:
                        continue
                    try:
                        total_bytes += path.stat().st_size
                        files_count += 1
                    except OSError:
                        pass
        except OSError:
            pass
        return total_bytes, files_count

    def _diagnostic_tcp_summary(pids: set[int]) -> dict[str, int]:
        if os.name != "nt" or not pids:
            return {}
        try:
            result = subprocess.run(
                ["netstat", "-ano", "-p", "tcp"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=20.0,
                creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0),
            )
        except Exception:
            return {"netstat_error": 1}
        states: dict[str, int] = {}
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) < 5 or parts[0].upper() != "TCP":
                continue
            try:
                pid = int(parts[-1])
            except ValueError:
                continue
            if pid not in pids:
                continue
            state = parts[-2].upper()
            states[state] = states.get(state, 0) + 1
        states["TOTAL"] = sum(value for key, value in states.items() if key != "TOTAL")
        return states

    def write_diagnostic_snapshot(reason: str = "periodic", force: bool = False) -> None:
        nonlocal diagnostic_last_snapshot
        nonlocal diagnostic_previous_snapshot
        nonlocal diagnostic_previous_counts
        nonlocal diagnostic_last_progress
        nonlocal diagnostic_previous_data_bytes

        now = time.monotonic()
        if not force and now - diagnostic_last_snapshot < float(DIAGNOSTIC_LOG_INTERVAL_SECONDS):
            return

        counts = queue.counts()
        interval_seconds = max(0.001, now - diagnostic_previous_snapshot)
        done_delta = int(counts.get("done", 0)) - int(diagnostic_previous_counts.get("done", 0))
        failed_delta = int(counts.get("failed", 0)) - int(diagnostic_previous_counts.get("failed", 0))
        wallets_per_minute = done_delta * 60.0 / interval_seconds
        if done_delta > 0:
            diagnostic_last_progress = now
        no_progress_seconds = now - diagnostic_last_progress

        active_indexes = [
            index for index, node in enumerate(nodes)
            if node.get("healthy") and not node.get("retire_after_batch")
        ]
        retiring_count = sum(
            1 for node in nodes if node.get("healthy") and node.get("retire_after_batch")
        )
        standby_count = sum(1 for node in nodes if node.get("standby"))
        worker_count = len(worker_states)
        running_claimed_wallets = sum(
            len(set(state.get("wallets") or set()))
            for state in worker_states.values()
        )
        lane_worker_counts = {
            lane: sum(
                1
                for state in worker_states.values()
                if str(state.get("queue_lane") or "fresh") == lane
            )
            for lane in ("fresh", "retry", "heavy")
        }
        queue_lane_snapshot = queue.retry_stats()
        heavy_tail_mode_now = _is_heavy_tail_mode(counts, queue_lane_snapshot)
        effective_worker_limit = max(
            1,
            min(
                int(cpu_parallelism),
                int(heavy_tail_parallelism)
                if heavy_tail_mode_now
                else int(cpu_parallelism),
            ),
        )
        heavy_worker_target, heavy_worker_share = _adaptive_heavy_worker_target(
            effective_worker_limit, queue_lane_snapshot
        )
        cpu_state = cpu_monitor.snapshot()
        cpu_current = safe_float(cpu_state.get("current"))
        cpu_avg = safe_float(cpu_state.get("window_average"), last_cpu_window_average)
        cpu_peak = safe_float(cpu_state.get("window_peak"), last_cpu_window_peak)

        worker_rows: list[tuple[float, str]] = []
        stalled_workers = 0
        worker_pids: set[int] = set()
        all_child_pids: set[int] = set()
        for node_index, state in list(worker_states.items()):
            proc = state.get("proc")
            pid = int(getattr(proc, "pid", 0) or 0)
            if pid:
                worker_pids.add(pid)
                all_child_pids.add(pid)
            started = safe_float(state.get("started_at_monotonic"), now)
            age_seconds = max(0.0, now - started)
            wallets_set = set(state.get("wallets") or set())
            try:
                durable_done = len(
                    load_test_memory_from_directory(
                        state["bucket_dir"],
                        min_tested_at_ms=refresh_since_ms,
                    )
                    & wallets_set
                )
            except Exception:
                durable_done = -1
            node = nodes[node_index] if 0 <= node_index < len(nodes) else {}
            xray_proc = node.get("xray_proc")
            xray_pid = int(getattr(xray_proc, "pid", 0) or 0)
            if xray_pid:
                all_child_pids.add(xray_pid)
            worker_alive = proc is not None and proc.poll() is None
            xray_alive = xray_proc is not None and xray_proc.poll() is None
            activity = state.get("activity_state") or {}
            last_output_age = max(
                0.0, now - safe_float(activity.get("last_output_monotonic"), started)
            )
            queue_lane = str(state.get("queue_lane") or "fresh")
            if queue_lane == "heavy":
                diagnostic_progress_timeout = max(
                    float(WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS),
                    float(HEAVY_WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS),
                )
                diagnostic_silence_timeout = max(
                    float(WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS),
                    float(HEAVY_WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS),
                )
            elif queue_lane == "retry":
                diagnostic_progress_timeout = max(
                    float(WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS),
                    float(RETRY_WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS),
                )
                diagnostic_silence_timeout = max(
                    float(WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS),
                    float(RETRY_WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS),
                )
            else:
                diagnostic_progress_timeout = float(
                    WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS
                )
                diagnostic_silence_timeout = float(
                    WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS
                )
            diagnostic_silent_stall = (
                durable_done <= 0
                and age_seconds >= diagnostic_progress_timeout
                and last_output_age >= diagnostic_silence_timeout
            )
            if diagnostic_silent_stall:
                stalled_workers += 1
            last_line = str(activity.get("last_output_line") or "").strip().replace("\r", " ").replace("\n", " ")
            if len(last_line) > 180:
                last_line = last_line[:177] + "..."
            score = safe_float(node.get("speed_score_ms"), float("inf"))
            score_text = f"{score:.0f}" if math.isfinite(score) else "unknown"
            worker_rows.append((
                age_seconds,
                f"node={node_index} pid={pid or '-'} batch={state.get('batch_id')} "
                f"age={age_seconds/60.0:.1f}m lane={state.get('queue_lane', 'normal')} "
                f"durable_done={durable_done}/{len(wallets_set)} "
                f"last_output_age={last_output_age:.1f}s stage={last_line!r} "
                f"worker_alive={worker_alive} xray_alive={xray_alive} "
                f"vpn_score_ms={score_text} ip={node.get('ip') or '-'}",
            ))
        for index in active_indexes:
            xray_proc = nodes[index].get("xray_proc")
            xray_pid = int(getattr(xray_proc, "pid", 0) or 0)
            if xray_pid:
                all_child_pids.add(xray_pid)

        error_categories, error_samples, _error_lines, error_truncated = _diagnostic_error_summary()
        (
            runtime_categories,
            runtime_sources,
            runtime_samples,
            runtime_lines,
            runtime_truncated,
        ) = _diagnostic_runtime_summary()
        new_crash_count, crash_summaries, crash_truncated = (
            _diagnostic_worker_crash_summary()
        )
        data_bytes, data_files = _diagnostic_data_footprint()
        data_growth = data_bytes - diagnostic_previous_data_bytes if diagnostic_previous_data_bytes else 0
        diagnostic_previous_data_bytes = data_bytes

        tcp = _diagnostic_tcp_summary(all_child_pids)
        try:
            disk = shutil.disk_usage(root)
            disk_free_gb = disk.free / (1024 ** 3)
            disk_used_percent = (disk.used / disk.total * 100.0) if disk.total else 0.0
        except OSError:
            disk_free_gb = -1.0
            disk_used_percent = -1.0

        db_path = root / GLOBAL_QUEUE_DB_FILE_NAME
        def _size(path: Path) -> int:
            try:
                return path.stat().st_size
            except OSError:
                return 0
        def _epoch_age_text(value: Any) -> str:
            epoch_value = safe_float(value, 0.0)
            if epoch_value <= 0.0:
                return "never"
            return f"{max(0.0, time.time() - epoch_value):.1f}s"
        db_bytes = _size(db_path)
        wal_bytes = _size(Path(str(db_path) + "-wal"))
        shm_bytes = _size(Path(str(db_path) + "-shm"))
        memory_quality = test_memory_quality_summary(
            root / TEST_MEMORY_FILE_NAME,
            min_tested_at_ms=refresh_since_ms,
        )

        blockers: list[str] = []
        if len(active_indexes) < int(cpu_parallelism) and counts.get("pending", 0):
            blockers.append(
                f"ACTIVE_BELOW_RUNTIME_LIMIT({len(active_indexes)}/{cpu_parallelism})"
            )
        if worker_count < len(active_indexes) and counts.get("pending", 0):
            if heavy_tail_mode_now:
                blockers.append(
                    f"HEAVY_TAIL_WORKER_CAP({worker_count}/{effective_worker_limit}; "
                    f"idle_vpns={len(active_indexes)-worker_count})"
                )
            else:
                blockers.append(f"IDLE_ACTIVE_VPNS({len(active_indexes)-worker_count})")
        if no_progress_seconds >= float(DIAGNOSTIC_STALL_SECONDS) and counts.get("running", 0):
            blockers.append(f"NO_WALLET_COMPLETION_FOR_{no_progress_seconds/60.0:.1f}MIN")
        if stalled_workers:
            blockers.append(f"WORKERS_WITHOUT_DURABLE_PROGRESS({stalled_workers})")
        if error_categories["connection_aborted_10053"]:
            blockers.append(f"WINDOWS_ABORT_10053({error_categories['connection_aborted_10053']})")
        if error_categories["timeout"]:
            blockers.append(f"NETWORK_TIMEOUTS({error_categories['timeout']})")
        if error_categories["http_429_rate_limit"]:
            blockers.append(f"RATE_LIMIT_429({error_categories['http_429_rate_limit']})")
        if error_categories["http_403_blocked"]:
            blockers.append(f"HTTP_403_OR_CLOUDFLARE({error_categories['http_403_blocked']})")
        if error_categories["sqlite"]:
            blockers.append(f"SQLITE_ERRORS({error_categories['sqlite']})")
        if error_categories["worker_retry_76"]:
            blockers.append(f"WALLET_RETRY_REROUTES_76({error_categories['worker_retry_76']})")
        if error_categories["worker_exit_75"] or error_categories["worker_exit_other"]:
            blockers.append(
                f"WORKER_EXITS(75={error_categories['worker_exit_75']},other={error_categories['worker_exit_other']})"
            )
        if error_categories["worker_traceback"]:
            blockers.append(f"WORKER_TRACEBACKS({error_categories['worker_traceback']})")
        if error_categories["fatal"]:
            blockers.append(f"MANAGER_FATAL({error_categories['fatal']})")
        if error_categories["output_write"]:
            blockers.append(f"LIVE_OUTPUT_WRITE_ERRORS({error_categories['output_write']})")
        if tcp.get("SYN_SENT", 0) >= 10:
            blockers.append(f"MANY_TCP_SYN_SENT({tcp.get('SYN_SENT', 0)})")
        if disk_free_gb >= 0 and disk_free_gb < 5.0:
            blockers.append(f"LOW_DISK_FREE({disk_free_gb:.1f}GB)")
        if (
            done_delta == 0
            and data_growth < 1024 * 1024
            and counts.get("running", 0)
            and no_progress_seconds >= 600.0
        ):
            blockers.append("LOW_DATA_GROWTH_WITH_RUNNING_WORK")
        if reason == "startup-ready":
            blockers = ["STARTUP_READY_NO_RUNTIME_SAMPLE_YET"]
        elif not blockers:
            blockers.append("NO_SINGLE_CLEAR_BLOCKER_IN_THIS_INTERVAL")

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        lines = [
            "",
            "=" * 100,
            f"[{timestamp}] DIAGNOSTIC SNAPSHOT reason={reason} build={BUILD_ID}",
            "wallet_inclusion "
            + wallet_inclusion_policy_text(
                max_wallets=setting(args.max_wallets, MAX_WALLETS_TO_SCORE),
                min_positions=setting(args.min_positions, MIN_RESOLVED_POSITIONS),
                min_losses=setting(args.min_losses, MIN_LOSING_POSITIONS),
                min_pnl=setting(args.min_pnl, MIN_CLOSED_REALIZED_PNL),
            ),
            f"versions data_fetch={COMPLETE_FETCH_VERSION} refresh_cycle={REFRESH_CYCLE_VERSION}",
            f"persistent_wallet_memory file={root / TEST_MEMORY_FILE_NAME} "
            f"mirrored_rows={int(memory_sync_state.get('rows', 0))} "
            f"dirty={bool(memory_sync_state.get('dirty'))} "
            f"reset_after_ms={refresh_since_ms} "
            f"resume_until_deleted=True live_traded_required={REQUIRE_LIVE_OFFICIAL_TRADED_FOR_MEMORY}",
            f"memory_verification rows={memory_quality['rows']} "
            f"verified_complete={memory_quality['verified_complete']} "
            f"legacy_missing_fields={memory_quality['missing_verification_fields']}",
            f"live_score_outputs enabled={GLOBAL_QUEUE_LIVE_OUTPUT_SYNC_ENABLED} "
            f"scored_rows={int(live_output_state.get('scored_rows', 0) or 0)} "
            f"csv_file={root / 'edge_scores_progress.csv'} "
            f"csv_size={_size(root / 'edge_scores_progress.csv') / (1024 ** 2):.2f}MB "
            f"csv_dirty={bool(live_output_state.get('csv_dirty'))} "
            f"csv_last_success_age={_epoch_age_text(live_output_state.get('last_csv_sync_epoch'))} "
            f"xlsx_file={root / 'edge_scores.xlsx'} "
            f"xlsx_size={_size(root / 'edge_scores.xlsx') / (1024 ** 2):.2f}MB "
            f"xlsx_dirty={bool(live_output_state.get('xlsx_dirty'))} "
            f"xlsx_last_success_age={_epoch_age_text(live_output_state.get('last_xlsx_sync_epoch'))} "
            f"last_error={str(live_output_state.get('last_error') or 'none')}",
            f"starting_vpn_target={VPN_MAX_ACTIVE_NODES} "
            f"scale_up_step={max(1, int(VPN_AUTO_TUNE_UP_STEP))} "
            f"runtime_limit={cpu_parallelism} effective_worker_limit={effective_worker_limit} "
            f"heavy_tail_mode={heavy_tail_mode_now} heavy_tail_limit={heavy_tail_parallelism} "
            f"cpu_auto_tune={CPU_AUTO_TUNE_ENABLED} "
            f"dead_recheck={VPN_DEAD_RECHECK_ENABLED} continuous_sort={VPN_CONTINUOUS_SORT_ENABLED}",
            f"interval={interval_seconds:.1f}s done_delta={done_delta} failed_delta={failed_delta} "
            f"wallets_per_min={wallets_per_minute:.3f} no_completion_for={no_progress_seconds:.1f}s",
            f"queue total={counts.get('total')} done={counts.get('done')} running_wallets={counts.get('running')} "
            f"pending={counts.get('pending')} failed={counts.get('failed')}",
            f"queue_retry={queue_lane_snapshot}",
            f"vpn active={len(active_indexes)}/{cpu_parallelism} retiring={retiring_count} standby={standby_count} "
            f"worker_processes={worker_count} claimed_wallets={running_claimed_wallets}",
            f"worker_lanes fresh={lane_worker_counts['fresh']} retry={lane_worker_counts['retry']} "
            f"heavy={lane_worker_counts['heavy']}/{heavy_worker_target} "
            f"share={heavy_worker_share:.0%}",
            f"http_concurrency per_worker~={int(HEAVY_CLOSED_FETCH_WORKERS)+int(HEAVY_ACTIVITY_FETCH_WORKERS)+int(HEAVY_ACTIVITY_WINDOW_WORKERS) if heavy_tail_mode_now else int(CLOSED_FETCH_WORKERS)+int(ACTIVITY_FETCH_WORKERS)+int(ACTIVITY_WINDOW_WORKERS)} "
            f"estimated_total~={worker_count * (int(HEAVY_CLOSED_FETCH_WORKERS)+int(HEAVY_ACTIVITY_FETCH_WORKERS)+int(HEAVY_ACTIVITY_WINDOW_WORKERS) if heavy_tail_mode_now else int(CLOSED_FETCH_WORKERS)+int(ACTIVITY_FETCH_WORKERS)+int(ACTIVITY_WINDOW_WORKERS))} "
            f"closed={HEAVY_CLOSED_FETCH_WORKERS if heavy_tail_mode_now else CLOSED_FETCH_WORKERS} "
            f"activity_offsets={HEAVY_ACTIVITY_FETCH_WORKERS if heavy_tail_mode_now else ACTIVITY_FETCH_WORKERS} "
            f"activity_windows={HEAVY_ACTIVITY_WINDOW_WORKERS if heavy_tail_mode_now else ACTIVITY_WINDOW_WORKERS}",
            f"progress_guard completion_seen_this_run={durable_completion_seen_this_run} "
            f"fresh_for={CPU_AUTO_TUNE_PROGRESS_FRESH_SECONDS:.0f}s "
            f"auto_tune_effect=diagnostics-only "
            f"worker_watchdog=fresh({WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS:.0f}s+"
            f"{WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS:.0f}s) "
            f"retry({RETRY_WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS:.0f}s+"
            f"{RETRY_WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS:.0f}s) "
            f"heavy({HEAVY_WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS:.0f}s+"
            f"{HEAVY_WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS:.0f}s)",
            f"cpu current={cpu_current:.1f}% window_avg={cpu_avg:.1f}% window_peak={cpu_peak:.1f}% "
            f"threads={threading.active_count()}",
            f"ram current={safe_float(ram_monitor.snapshot().get('current')):.1f}% "
            f"avg_window={safe_float(ram_monitor.snapshot().get('average')):.1f}% "
            f"samples={int(ram_monitor.snapshot().get('samples', 0) or 0)}/"
            f"{int(ram_monitor.snapshot().get('required_samples', 0) or 0)} "
            f"stop_if_avg>{RAM_SAFETY_STOP_PERCENT:.1f}% "
            f"tripped={bool(ram_monitor.snapshot().get('tripped'))}",
            f"data_files={data_files} data_size={data_bytes/(1024**2):.1f}MB "
            f"growth={data_growth/(1024**2):.1f}MB interval",
            f"worker_crash_log={worker_crash_path} exists={worker_crash_path.exists()} "
            f"size={(worker_crash_path.stat().st_size if worker_crash_path.exists() else 0)/1024:.1f}KB",
            f"queue_db={db_bytes/(1024**2):.2f}MB wal={wal_bytes/(1024**2):.2f}MB "
            f"shm={shm_bytes/(1024**2):.2f}MB",
            f"disk_free={disk_free_gb:.1f}GB disk_used={disk_used_percent:.1f}%",
            f"tcp_for_worker_and_xray_pids={tcp or {'unavailable': 1}}",
            f"recent_error_categories={error_categories} truncated_input={error_truncated}",
            f"runtime_event_summary lines={runtime_lines} categories={runtime_categories} "
            f"sources={runtime_sources} truncated_input={runtime_truncated}",
            f"new_worker_crashes={new_crash_count} truncated_input={crash_truncated}",
            "LIKELY_BLOCKERS=" + " | ".join(blockers),
            "OLDEST_WORKERS:",
        ]
        worker_rows.sort(key=lambda item: item[0], reverse=True)
        if worker_rows:
            lines.extend(
                "  " + row
                for _age, row in worker_rows[: max(1, int(DIAGNOSTIC_OLDEST_WORKERS))]
            )
        else:
            lines.append("  none")
        lines.append("RECENT_RUNTIME_EVENTS:")
        if runtime_samples:
            lines.extend("  " + sample for sample in runtime_samples)
        else:
            lines.append("  none")
        lines.append("RECENT_ERROR_SAMPLES:")
        if error_samples:
            lines.extend("  " + sample for sample in error_samples)
        else:
            lines.append("  none")
        lines.append("NEW_WORKER_CRASH_SUMMARIES:")
        if crash_summaries:
            lines.extend("  " + summary for summary in crash_summaries)
        else:
            lines.append("  none")
        lines.append("=" * 100)
        _diagnostic_append(lines)

        diagnostic_last_snapshot = now
        diagnostic_previous_snapshot = now
        diagnostic_previous_counts = counts

    def console_line(text: str, newline: bool = False) -> None:
        nonlocal last_console_width
        if not CLEAN_CONSOLE_DASHBOARD:
            return
        with console_output_lock:
            _write_console_message_locked(text if text else "")
        last_console_width = 0

    def update_active_file(force: bool = False) -> None:
        nonlocal last_active_file
        now = time.monotonic()
        if force or now - last_active_file >= ACTIVE_VPN_UPDATE_INTERVAL_SECONDS:
            with continuous_sort_apply_lock:
                _write_active_vpn_file(root, nodes)
            last_active_file = now

    def persist_vpn_test_memory(
        reason: str,
        *,
        log_success: bool = False,
    ) -> Path | None:
        try:
            memory_path = _write_vpn_startup_test_memory(
                script_path.parent,
                nodes,
            )
            if log_success:
                logger.log(
                    f"[proxy:test-memory-saved] file={memory_path} "
                    f"entries={len(nodes)} reason={reason}",
                    source="PROXY",
                )
            return memory_path
        except Exception:
            logger.log(
                f"[proxy:test-memory-write-failed] reason={reason}\n"
                + traceback.format_exc(),
                source="PROXY",
                force_error=True,
            )
            return None

    def show_dashboard(force: bool = False) -> None:
        nonlocal last_dashboard
        now = time.monotonic()
        refresh_interval = (
            float(CONSOLE_STATUS_INTERVAL_SECONDS)
            if live_dashboard_enabled
            else float(CONSOLE_FALLBACK_STATUS_INTERVAL_SECONDS)
        )
        if not force and last_dashboard > 0 and now - last_dashboard < refresh_interval:
            return

        counts = queue.counts()
        retry_state = queue.retry_stats()
        heavy_pending = int(retry_state.get("heavy_ready", 0)) + int(
            retry_state.get("heavy_deferred", 0)
        )
        active = sum(
            1 for node in nodes
            if node.get("healthy") and not node.get("retire_after_batch")
        )
        retiring = sum(
            1 for node in nodes if node.get("healthy") and node.get("retire_after_batch")
        )
        standby = sum(1 for node in nodes if node.get("standby"))
        available_working = active + retiring + standby
        effective_limit = min(max(1, int(cpu_parallelism)), max(1, available_working))
        heavy_tail_mode_dashboard = _is_heavy_tail_mode(counts, retry_state)
        effective_worker_limit_dashboard = max(
            1,
            min(
                int(effective_limit),
                int(heavy_tail_parallelism)
                if heavy_tail_mode_dashboard
                else int(effective_limit),
            ),
        )
        percent = counts["done"] / counts["total"] * 100.0 if counts["total"] else 100.0

        cpu_state = cpu_monitor.snapshot()
        cpu_current = safe_float(cpu_state.get("current"))
        cpu_peak = max(safe_float(cpu_state.get("window_peak")), last_cpu_window_peak)
        cpu_average_display = safe_float(
            cpu_state.get("window_average"), last_cpu_window_average
        )

        session_elapsed_minutes = max(0.001, (now - runtime_session_start_monotonic) / 60.0)
        session_completed = max(0, int(counts["done"]) - int(runtime_session_start_done))
        session_wpm = session_completed / session_elapsed_minutes
        remaining_wallets = max(0, int(counts["total"]) - int(counts["done"]))
        eta_minutes = (remaining_wallets / session_wpm) if session_wpm > 0 else None
        eta_text = _format_eta_minutes(eta_minutes)

        with continuous_sort_progress_lock:
            sort_running = bool(continuous_sort_progress.get("running"))
            sort_cycle = int(continuous_sort_progress.get("cycle", 0) or 0)
            sort_tested = int(continuous_sort_progress.get("tested", 0) or 0)
            sort_total = int(continuous_sort_progress.get("total", 0) or 0)
        sort_text = f"C{sort_cycle} {sort_tested}/{sort_total}" if sort_running else "OFF"

        with dead_recheck_progress_lock:
            dead_running = bool(dead_recheck_progress.get("running"))
            dead_cycle = int(dead_recheck_progress.get("cycle", 0) or 0)
            dead_tested = int(dead_recheck_progress.get("tested", 0) or 0)
            dead_total = int(dead_recheck_progress.get("total", 0) or 0)
        dead_text = (
            f"C{dead_cycle} {dead_tested}/{dead_total}"
            if dead_running
            else ("IDLE" if VPN_DEAD_RECHECK_ENABLED else "OFF")
        )

        no_progress_seconds = max(0.0, now - last_durable_completion_monotonic)
        if cpu_average_display >= float(CPU_AUTO_TUNE_HIGH_PERCENT):
            tune_text = "DOWN -1"
        elif cpu_average_display < float(CPU_AUTO_TUNE_LOW_PERCENT):
            tune_text = f"UP +{max(1, int(VPN_AUTO_TUNE_UP_STEP))}"
        else:
            tune_text = "STABLE"

        workers = len(worker_states)
        heavy_workers = sum(
            1
            for state in worker_states.values()
            if str(state.get("queue_lane") or "fresh") == "heavy"
        )
        normal_workers = max(0, workers - heavy_workers)
        normal_http_per_worker = max(
            1,
            int(CLOSED_FETCH_WORKERS + ACTIVITY_FETCH_WORKERS + ACTIVITY_WINDOW_WORKERS),
        )
        heavy_http_per_worker = max(
            1,
            int(
                HEAVY_CLOSED_FETCH_WORKERS
                + HEAVY_ACTIVITY_FETCH_WORKERS
                + HEAVY_ACTIVITY_WINDOW_WORKERS
            ),
        )
        http_total = (
            normal_workers * normal_http_per_worker
            + heavy_workers * heavy_http_per_worker
        )
        ram_state = ram_monitor.snapshot()
        ram_current = safe_float(ram_state.get("current"))
        ram_average = safe_float(ram_state.get("average"))
        ram_samples = int(ram_state.get("samples", 0) or 0)
        ram_required = int(ram_state.get("required_samples", 0) or 0)
        ram_text = (
            f"RAM={ram_current:.0f}/{ram_average:.0f}%"
            if ram_samples >= ram_required and ram_required > 0
            else f"RAM={ram_current:.0f}%({ram_samples}/{ram_required})"
        )
        timestamp = datetime.now().strftime("%H:%M:%S")
        last_done_text = (
            f"{no_progress_seconds:.0f}s ago"
            if durable_completion_seen_this_run
            else "not yet this run"
        )

        lines = [
            (
                f"[{timestamp}] VPN={active}/{effective_limit} fin={retiring} av={available_working} +"
                f"{max(1, int(VPN_AUTO_TUNE_UP_STEP))} | WCap={effective_worker_limit_dashboard} "
                f"Tail={'ON' if heavy_tail_mode_dashboard else 'OFF'} | "
                f"CPU={cpu_current:.1f}/{cpu_average_display:.1f}% {tune_text}"
            ),
            (
                f"Run={counts['running']} pend={counts['pending']} H={heavy_pending}/{heavy_workers} "
                f"fail={counts['failed']} | W={workers}/{effective_worker_limit_dashboard} "
                f"HTTP~{http_total} | {ram_text}"
            ),
            (
                f"Err={logger.total_errors} retry76={retry76_events_since_cpu_tune} | "
                f"Sort={sort_text} Dead={dead_text}"
            ),
            (
                f"{counts['done']}/{counts['total']} ({percent:.2f}%) | "
                f"Speed={session_wpm:.2f} WPM | ETA={eta_text}"
            ),
        ]
        with console_output_lock:
            _render_live_dashboard_locked(lines)
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
            with console_output_lock:
                _write_console_message_locked(
                    "\n"
                    f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: {count} new "
                    f"entr{'y' if count == 1 else 'ies'} -> {ERROR_LOG_FILE_NAME}"
                    "\n"
                )

    def stop_node_xray(node: dict[str, Any]) -> None:
        node_index = int(node.get("manager_index", -1) or -1)
        lock = (
            node_runtime_locks[node_index]
            if 0 <= node_index < len(node_runtime_locks)
            else threading.RLock()
        )
        with lock:
            proc = node.get("xray_proc")
            if proc is not None:
                queue.unregister_pid(getattr(proc, "pid", None))
                stop_process(proc)
            handle = node.get("xray_handle")
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
            node["xray_proc"] = None
            node["xray_handle"] = None

    def _park_node_unlocked(node: dict[str, Any]) -> None:
        """Stop Xray but keep the successfully tested node available as standby."""
        stop_node_xray(node)
        node["healthy"] = False
        node["standby"] = True
        node["health_failures"] = 0
        node["last_error"] = ""
        node["next_recheck"] = 0.0

    def park_node(node: dict[str, Any]) -> None:
        index = int(node.get("manager_index", -1) or -1)
        lock = (
            node_runtime_locks[index]
            if 0 <= index < len(node_runtime_locks)
            else threading.RLock()
        )
        with lock:
            _park_node_unlocked(node)

    def _wallet_position_test_urls(wallet: str) -> tuple[tuple[str, str], ...]:
        now_ts = max(1, int(time.time()))
        closed_query = urllib.parse.urlencode(
            {
                "user": wallet,
                "limit": 1,
                "offset": 0,
                "sortBy": "TIMESTAMP",
                "sortDirection": "ASC",
            }
        )
        positions_query = urllib.parse.urlencode(
            {
                "user": wallet,
                "sizeThreshold": 0,
                "limit": 1,
                "offset": 0,
                "sortBy": "TITLE",
                "sortDirection": "ASC",
            }
        )
        activity_query = urllib.parse.urlencode(
            {
                "user": wallet,
                "type": "TRADE",
                "start": 1,
                "end": now_ts,
                "sortBy": "TIMESTAMP",
                "sortDirection": "ASC",
                "limit": 1,
                "offset": 0,
            }
        )
        return (
            ("closed-positions", f"{BASE_URL}/closed-positions?{closed_query}"),
            ("positions", f"{BASE_URL}/positions?{positions_query}"),
            ("activity", f"{BASE_URL}/activity?{activity_query}"),
        )

    def _run_wallet_position_attempt(
        proxy_url: str,
        wallet: str,
    ) -> tuple[float, dict[str, float]]:
        """Require all real wallet-position endpoints to work through this VPN."""
        attempt_started = time.perf_counter()
        endpoint_ms: dict[str, float] = {}
        for endpoint_name, url in _wallet_position_test_urls(wallet):
            endpoint_started = time.perf_counter()
            payload = proxy_json_request(
                str(proxy_url),
                url,
                timeout=float(VPN_SPEED_TEST_TIMEOUT_SECONDS),
            )
            if not isinstance(payload, list):
                raise RuntimeError(
                    f"Unexpected /{endpoint_name} response for wallet {wallet}: "
                    f"{type(payload).__name__}"
                )
            endpoint_ms[endpoint_name] = (
                time.perf_counter() - endpoint_started
            ) * 1000.0
        return (time.perf_counter() - attempt_started) * 1000.0, endpoint_ms

    def measure_polymarket_proxy(
        proxy_url: str,
        *,
        attempts_override: int | None = None,
        wallet_offset: int = 0,
    ) -> dict[str, Any]:
        # Even when ranking is disabled, the VPN must still pass the real
        # wallet-position endpoints. The switch only controls use of latency
        # as a ranking score; it never downgrades validation to an IP-only test.
        attempts = max(
            1,
            int(
                VPN_SPEED_TEST_ATTEMPTS
                if attempts_override is None
                else attempts_override
            ),
        )
        durations_ms: list[float] = []
        failures = 0
        last_error = ""
        endpoint_totals = {
            "closed-positions": 0.0,
            "positions": 0.0,
            "activity": 0.0,
        }

        for attempt_index in range(attempts):
            wallet = position_test_wallets[
                (int(wallet_offset) + attempt_index) % len(position_test_wallets)
            ]
            try:
                duration_ms, endpoint_ms = _run_wallet_position_attempt(
                    str(proxy_url), wallet
                )
                durations_ms.append(duration_ms)
                for key, value in endpoint_ms.items():
                    endpoint_totals[key] = endpoint_totals.get(key, 0.0) + value
            except Exception as exc:
                failures += 1
                last_error = f"wallet={wallet} error={exc!r}"

        required_successes = min(
            attempts,
            max(1, int(VPN_POSITION_TEST_MIN_SUCCESSFUL_ATTEMPTS)),
        )
        if len(durations_ms) < required_successes:
            raise RuntimeError(
                f"Wallet-position VPN test passed only {len(durations_ms)}/{attempts}; "
                f"required={required_successes}; last={last_error}"
            )

        average_ms = sum(durations_ms) / len(durations_ms)
        endpoint_averages = {
            key: value / len(durations_ms)
            for key, value in endpoint_totals.items()
        }
        return {
            "speed_avg_ms": average_ms,
            "speed_score_ms": (
                average_ms + failures * float(VPN_SPEED_FAILURE_PENALTY_MS)
                if VPN_SPEED_RANKING_ENABLED
                else 0.0
            ),
            "speed_successes": len(durations_ms),
            "speed_failures": failures,
            "speed_tested_at": _log_timestamp(),
            "position_test_closed_ms": endpoint_averages["closed-positions"],
            "position_test_open_ms": endpoint_averages["positions"],
            "position_test_activity_ms": endpoint_averages["activity"],
        }

    def quick_wallet_position_proxy_check(
        proxy_url: str,
        *,
        wallet_offset: int = 0,
    ) -> None:
        measure_polymarket_proxy(
            str(proxy_url),
            attempts_override=max(1, int(VPN_POSITION_QUICK_TEST_ATTEMPTS)),
            wallet_offset=wallet_offset,
        )

    def apply_speed_measurement(node: dict[str, Any], measurement: dict[str, Any]) -> None:
        for key in (
            "speed_avg_ms",
            "speed_score_ms",
            "speed_successes",
            "speed_failures",
            "speed_tested_at",
            "position_test_closed_ms",
            "position_test_open_ms",
            "position_test_activity_ms",
        ):
            node[key] = measurement.get(key)

    def benchmark_polymarket_proxy(node: dict[str, Any]) -> None:
        apply_speed_measurement(
            node,
            measure_polymarket_proxy(
                str(node["proxy"]),
                wallet_offset=int(node.get("source_index", 0) or 0),
            ),
        )

    def _start_or_restart_node_unlocked(
        node: dict[str, Any],
        enforce_unique_ip: bool = True,
        run_speed_test: bool = False,
    ) -> tuple[bool, str]:
        # Important: proc/handle must be cleaned even when the outbound-IP request
        # raises Timeout/SSL/Connection errors. Without this cleanup, failed tests
        # leave orphan Xray processes running and eventually exhaust Windows commit
        # memory/pagefile.
        stop_node_xray(node)
        node["standby"] = False
        proc: subprocess.Popen | None = None
        handle: Any = None
        registered_pid = False
        try:
            proc, handle = start_xray_node(
                xray_path,
                node["config_path"],
                int(node["port"]),
                node["log_path"],
                logger=logger,
                source=f"XRAY{node['source_index']}",
                line_callback=(
                    lambda line, node_index=int(node.get("manager_index", -1) or -1):
                    handle_active_xray_line(node_index, line)
                ),
            )
            queue.register_pid(proc.pid, "xray")
            registered_pid = True

            proxy_url = str(node["proxy"])
            outbound_ip = (
                proxy_text_request(
                    proxy_url,
                    VLESS_IP_CHECK_URL,
                    timeout=PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
                )
                if VLESS_CHECK_OUTBOUND_IP and not args.skip_ip_check
                else f"unchecked-{node['source_index']}"
            )

            if run_speed_test or not math.isfinite(
                safe_float(node.get("speed_score_ms"), float("inf"))
            ):
                benchmark_polymarket_proxy(node)
            else:
                # A cached/ranked VPN still has to pass the real wallet-position
                # APIs before it receives a wallet batch.
                quick_wallet_position_proxy_check(
                    proxy_url,
                    wallet_offset=int(node.get("source_index", 0) or 0),
                )

            duplicate = any(
                other is not node
                and other.get("healthy")
                and other.get("ip") == outbound_ip
                for other in nodes
            )
            if enforce_unique_ip and duplicate and VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS:
                raise RuntimeError(f"duplicate outbound IP {outbound_ip}")

            node["xray_proc"] = proc
            node["xray_handle"] = handle
            node["ip"] = outbound_ip
            node["healthy"] = True
            node["standby"] = False
            node["ever_worked"] = True
            node["ranking_candidate"] = True
            node["ranking_cycle_pass"] = True
            node["ranking_failures"] = 0
            node["health_failures"] = 0
            node["last_error"] = ""
            node["next_recheck"] = 0.0
            return True, ""

        except Exception as exc:
            # Clean every partially-started Xray before marking the node failed.
            if proc is not None:
                if registered_pid:
                    try:
                        queue.unregister_pid(proc.pid)
                    except Exception:
                        pass
                stop_process(proc)

            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass

            node["xray_proc"] = None
            node["xray_handle"] = None
            node["healthy"] = False
            node["standby"] = False
            node["last_error"] = repr(exc)
            node["next_recheck"] = (
                time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
            )
            return False, repr(exc)

    def start_or_restart_node(
        node: dict[str, Any],
        enforce_unique_ip: bool = True,
        run_speed_test: bool = False,
    ) -> tuple[bool, str]:
        index = int(node.get("manager_index", -1) or -1)
        lock = (
            node_runtime_locks[index]
            if 0 <= index < len(node_runtime_locks)
            else threading.RLock()
        )
        with lock:
            return _start_or_restart_node_unlocked(
                node,
                enforce_unique_ip=enforce_unique_ip,
                run_speed_test=run_speed_test,
            )

    def _mark_node_dead_unlocked(node_index: int, reason: str) -> None:
        node = nodes[node_index]
        if not node.get("healthy") and node.get("next_recheck", 0):
            return
        node["healthy"] = False
        node["standby"] = False
        node["ranking_candidate"] = bool(node.get("ever_worked"))
        node["ranking_cycle_pass"] = False
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
        persist_vpn_test_memory("node-marked-dead")

    def mark_node_dead(node_index: int, reason: str) -> None:
        # Lock order is always ranking-apply -> node-runtime.
        with continuous_sort_apply_lock:
            lock = node_runtime_locks[node_index]
            with lock:
                _mark_node_dead_unlocked(node_index, reason)

    def handle_active_xray_line(node_index: int, line: str) -> None:
        """Quarantine an active Reality VPN after repeated bad handshakes."""
        if not (0 <= int(node_index) < len(nodes)):
            return
        if XRAY_REALITY_CERT_ERROR_MARKER not in str(line).lower():
            return

        now = time.monotonic()
        threshold = max(1, int(XRAY_REALITY_CERT_ERROR_THRESHOLD))
        window = max(1.0, float(XRAY_REALITY_CERT_ERROR_WINDOW_SECONDS))
        with reality_error_lock:
            events = reality_error_events.setdefault(int(node_index), deque())
            cutoff = now - window
            while events and events[0] < cutoff:
                events.popleft()
            events.append(now)
            if len(events) < threshold or int(node_index) in reality_quarantine_pending:
                return
            reality_quarantine_pending.add(int(node_index))
            events.clear()

        def quarantine() -> None:
            try:
                index = int(node_index)
                with node_runtime_locks[index]:
                    nodes[index]["reality_quarantined"] = True
                    nodes[index]["ranking_candidate"] = False
                    nodes[index]["ranking_cycle_pass"] = False
                reason = (
                    f"Reality handshake returned a real certificate {threshold} times "
                    f"within {window:.0f}s; quarantined for this run"
                )
                logger.log(
                    f"[proxy:reality-quarantine] node={index} reason={reason}",
                    source="PROXY",
                    force_error=True,
                )
                mark_node_dead(index, reason)
            finally:
                with reality_error_lock:
                    reality_quarantine_pending.discard(int(node_index))

        # Do not close/join the Xray stream from its own reader thread.
        threading.Thread(
            target=quarantine,
            name=f"reality-quarantine-{node_index}",
            daemon=True,
        ).start()

    # Parse every valid link first. Even startup-failed nodes remain in this list and are retried.
    vpn_parse_started = time.monotonic()
    vpn_parse_failed = 0
    startup.emit(
        "vpn_config_build",
        "started",
        f"links={len(links)} runtime_dir={runtime_dir}",
        step_started=vpn_parse_started,
    )
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
                "manager_index": len(nodes),
                "source_index": source_index,
                "link": link,
                "name": name,
                "protocol": protocol,
                "port": port,
                "proxy": f"http://127.0.0.1:{port}",
                "config_path": config_path,
                "log_path": node_dir / "xray.log",
                "healthy": False,
                "standby": False,
                "startup_tested": False,
                "health_failures": 0,
                "next_recheck": 0.0,
                "busy_batch": None,
                "retire_after_batch": False,
                "ever_worked": False,
                "ranking_candidate": False,
                "ranking_cycle_pass": False,
                "reality_quarantined": False,
                "ranking_testing": False,
                "dead_recheck_in_progress": False,
                "retry_cooldown_until": 0.0,
                "ranking_failures": 0,
                "ip": "",
                "speed_avg_ms": float("inf"),
                "speed_score_ms": float("inf"),
                "speed_successes": 0,
                "speed_failures": 0,
                "speed_tested_at": "",
                "position_test_closed_ms": 0.0,
                "position_test_open_ms": 0.0,
                "position_test_activity_ms": 0.0,
                "xray_proc": None,
                "xray_handle": None,
                "last_error": "",
            }
            nodes.append(node)
            node_runtime_locks.append(threading.RLock())
        except Exception as exc:
            vpn_parse_failed += 1
            logger.log(
                f"[proxy:parse-failed] link_index={source_index} error={exc!r}",
                source="PROXY",
                force_error=True,
            )
        parsed_so_far = source_index + 1
        if (
            parsed_so_far == len(links)
            or parsed_so_far == 1
            or (
                int(STARTUP_VPN_PARSE_PROGRESS_EVERY_LINKS) > 0
                and parsed_so_far % int(STARTUP_VPN_PARSE_PROGRESS_EVERY_LINKS) == 0
            )
        ):
            startup.emit(
                "vpn_config_build",
                "progress",
                f"links={parsed_so_far}/{len(links)} parsed={len(nodes)} failed={vpn_parse_failed}",
                step_started=vpn_parse_started,
            )

    startup.emit(
        "vpn_config_build",
        "done",
        f"links={len(links)} parsed={len(nodes)} failed={vpn_parse_failed}",
        step_started=vpn_parse_started,
    )

    if not nodes:
        if CLEAN_CONSOLE_DASHBOARD:
            sys.stdout = console_stdout
            sys.stderr = console_stderr
        manager_lock.release()
        logger.close()
        queue.close()
        print("No parseable proxy link remains.", file=sys.stderr)
        return 2

    # Load persistent startup-test memory from beside the Python file.
    # Exact unchanged links reuse their saved result; new/changed links are tested now.
    with startup.step("vpn_test_memory_load") as startup_state:
        startup_memory, startup_memory_path, startup_memory_error = (
            _load_vpn_startup_test_memory(script_path.parent)
        )
        startup_state["detail"] = (
            f"file={startup_memory_path} entries={len(startup_memory)} "
            f"valid={not bool(startup_memory_error)}"
        )
    if startup_memory_error:
        logger.log(
            f"[proxy:test-memory-invalid] file={startup_memory_path} "
            f"error={startup_memory_error}; all VPNs will be tested again",
            source="PROXY",
            force_error=True,
        )

    fresh_node_indexes: list[int] = []
    cached_working_count = 0
    cached_failed_count = 0
    with startup.step("vpn_test_memory_apply", f"nodes={len(nodes)}") as startup_state:
        for index, node in enumerate(nodes):
            key = _vpn_startup_memory_key(str(node.get("link") or ""))
            cached_row = startup_memory.get(key)
            if cached_row is None or not _apply_vpn_startup_memory(node, cached_row):
                fresh_node_indexes.append(index)
                continue
            if node.get("standby"):
                cached_working_count += 1
            else:
                cached_failed_count += 1
        startup_state["detail"] = (
            f"nodes={len(nodes)} reused={cached_working_count + cached_failed_count} "
            f"fresh={len(fresh_node_indexes)}"
        )

    cached_count = cached_working_count + cached_failed_count
    logger.log(
        f"[proxy:test-memory] file={startup_memory_path} "
        f"cached={cached_count} cached_working={cached_working_count} "
        f"cached_failed={cached_failed_count} fresh_tests={len(fresh_node_indexes)} "
        f"delete_file_to_retest_all=true",
        source="PROXY",
    )
    if CLEAN_CONSOLE_DASHBOARD:
        console_stdout.write(
            f"VPN test memory: {startup_memory_path} | "
            f"reused={cached_count} | fresh tests={len(fresh_node_indexes)} | "
            "delete this JSON file to retest all VPNs\n"
        )
        console_stdout.flush()

    # Only links absent from memory are benchmarked. At most
    # VPN_STARTUP_TEST_WORKERS temporary Xray processes exist at once.
    startup_workers = (
        max(
            1,
            min(
                len(fresh_node_indexes),
                int(VPN_STARTUP_TEST_WORKERS),
            ),
        )
        if fresh_node_indexes
        else 0
    )
    logger.log(
        f"[proxy:wallet-position-test-config] sample_wallets={position_test_wallets} "
        f"endpoints=closed-positions,positions,activity "
        f"full_attempts={VPN_SPEED_TEST_ATTEMPTS} "
        f"min_full_successes={VPN_POSITION_TEST_MIN_SUCCESSFUL_ATTEMPTS} "
        f"quick_attempts={VPN_POSITION_QUICK_TEST_ATTEMPTS}",
        source="PROXY",
    )
    logger.log(
        f"[proxy:startup-test] total_nodes={len(nodes)} reused={cached_count} "
        f"fresh_nodes={len(fresh_node_indexes)} concurrent_workers={startup_workers} "
        f"max_active_pool_start={cpu_parallelism} cpu_auto_tune={CPU_AUTO_TUNE_ENABLED}",
        source="PROXY",
    )

    def startup_test_node(node: dict[str, Any]) -> tuple[bool, str]:
        ok, error = start_or_restart_node(
            node,
            enforce_unique_ip=False,
            run_speed_test=True,
        )
        node["startup_tested"] = True
        if ok:
            node["ever_worked"] = True
            node["ranking_candidate"] = True
            node["ranking_cycle_pass"] = True
            park_node(node)
            return True, ""
        return False, error

    tested_count = cached_count
    passed_count = cached_working_count
    failed_count = cached_failed_count

    def show_startup_test_dashboard() -> None:
        test_percent = tested_count / len(nodes) * 100.0 if nodes else 100.0
        line = (
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"VPN Test: {tested_count}/{len(nodes)} ({test_percent:.2f}%) | "
            f"Passed: {passed_count} | Failed: {failed_count} | "
            f"From memory: {cached_count} | Testing at once: {startup_workers}"
        )
        with console_output_lock:
            _render_live_dashboard_locked([line])

    vpn_startup_test_started = time.monotonic()
    startup.emit(
        "vpn_startup_test",
        "started",
        f"total={len(nodes)} reused={cached_count} fresh={len(fresh_node_indexes)} workers={startup_workers}",
        step_started=vpn_startup_test_started,
    )
    show_startup_test_dashboard()

    if fresh_node_indexes:
        with ThreadPoolExecutor(max_workers=startup_workers) as executor:
            futures = {
                executor.submit(startup_test_node, nodes[index]): index
                for index in fresh_node_indexes
            }
            for future in as_completed(futures):
                index = futures[future]
                ok, error = future.result()
                tested_count += 1
                if ok:
                    passed_count += 1
                    logger.log(
                        f"[proxy:startup-pass] node={index} "
                        f"protocol={nodes[index]['protocol']} "
                        f"ip={nodes[index]['ip']} "
                        f"speed_score_ms={safe_float(nodes[index].get('speed_score_ms')):.1f} "
                        f"speed_avg_ms={safe_float(nodes[index].get('speed_avg_ms')):.1f} "
                        f"speed_failures={int(nodes[index].get('speed_failures', 0))}",
                        source="PROXY",
                    )
                else:
                    failed_count += 1
                    logger.log(
                        f"[proxy:startup-failed] node={index} error={error}",
                        source="PROXY",
                        force_error=True,
                    )
                if tested_count == 1 or tested_count == len(nodes) or (
                    VPN_STARTUP_PROGRESS_EVERY > 0
                    and tested_count % int(VPN_STARTUP_PROGRESS_EVERY) == 0
                ):
                    show_startup_test_dashboard()

    _finish_live_dashboard()
    startup.emit(
        "vpn_startup_test",
        "done",
        f"tested={tested_count}/{len(nodes)} passed={passed_count} failed={failed_count}",
        step_started=vpn_startup_test_started,
    )

    # For duplicate outbound IPs, keep the fastest Polymarket-tested config.
    vpn_dedup_started = time.monotonic()
    duplicate_nodes_removed = 0
    startup.emit(
        "vpn_duplicate_ip_filter",
        "started",
        f"enabled={VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS} nodes={len(nodes)}",
        step_started=vpn_dedup_started,
    )
    if VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS:
        nodes_by_ip: dict[str, list[int]] = {}
        for index, node in enumerate(nodes):
            if node.get("standby"):
                nodes_by_ip.setdefault(str(node.get("ip") or ""), []).append(index)

        for ip, indexes in nodes_by_ip.items():
            if not ip or len(indexes) <= 1:
                continue
            fastest_index = min(indexes, key=lambda idx: _vpn_rank_key(nodes[idx]))
            for index in indexes:
                if index == fastest_index:
                    continue
                node = nodes[index]
                node["standby"] = False
                node["last_error"] = (
                    f"duplicate outbound IP {ip}; faster_node={fastest_index}"
                )
                node["next_recheck"] = (
                    time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
                )
                duplicate_nodes_removed += 1
                logger.log(
                    f"[proxy:duplicate] node={index} ip={ip} "
                    f"kept_node={fastest_index} "
                    f"kept_score_ms={safe_float(nodes[fastest_index].get('speed_score_ms')):.1f} "
                    f"dropped_score_ms={safe_float(node.get('speed_score_ms')):.1f}",
                    source="PROXY",
                    force_error=True,
                )
    startup.emit(
        "vpn_duplicate_ip_filter",
        "done",
        f"duplicates_removed={duplicate_nodes_removed}",
        step_started=vpn_dedup_started,
    )

    # Save/prune persistent memory only after duplicate-IP elimination.
    # Deleting this JSON beside the Python file forces a complete startup retest.
    with startup.step("vpn_test_memory_save", f"nodes={len(nodes)}") as startup_state:
        saved_startup_memory_path = persist_vpn_test_memory(
            "startup-test-complete",
            log_success=True,
        )
        startup_state["detail"] = f"file={saved_startup_memory_path or 'not-saved'}"
    if CLEAN_CONSOLE_DASHBOARD and saved_startup_memory_path is not None:
        console_stdout.write(
            f"VPN startup-test memory saved: {saved_startup_memory_path}\n"
        )
        console_stdout.flush()

    # Snapshot is intentionally written exactly once per program run.
    # Later health checks, recoveries, dead nodes and worker assignments never edit it.
    with startup.step("vpn_startup_snapshot", f"nodes={len(nodes)}") as startup_state:
        startup_snapshot_path = _write_startup_sorted_vpn_snapshot(
            script_path.parent,
            nodes,
        )
        startup_snapshot_count = sum(
            1
            for node in nodes
            if (node.get("healthy") or node.get("standby"))
            and str(node.get("link") or "").strip()
        )
        startup_state["detail"] = (
            f"file={startup_snapshot_path} working_unique={startup_snapshot_count}"
        )
    logger.log(
        f"[proxy:startup-snapshot] file={startup_snapshot_path} "
        f"working_unique_sorted={startup_snapshot_count} immutable_until_exit=true",
        source="PROXY",
    )
    if CLEAN_CONSOLE_DASHBOARD:
        console_stdout.write(
            f"Startup VPN snapshot saved: {startup_snapshot_path} "
            f"({startup_snapshot_count} working unique VPNs, best first)\n"
        )
        console_stdout.flush()

    logger.log(
        "[queue:nonblocking-vpn-background] healthy VPNs will start wallet workers immediately; "
        "continuous sort and dead recheck run independently in background",
        source="QUEUE",
    )


    def continuous_sort_candidate(index: int) -> dict[str, Any]:
        """Benchmark one previously-working VPN without assigning wallet work to it."""
        node = nodes[index]
        lock = node_runtime_locks[index]
        with lock:
            node["ranking_testing"] = True
            temporary_proc: subprocess.Popen | None = None
            temporary_handle: Any = None
            temporary_pid_registered = False
            active_at_start = bool(
                node.get("healthy")
                and node.get("xray_proc") is not None
                and node.get("xray_proc").poll() is None
            )
            try:
                if active_at_start:
                    proxy_url = str(node["proxy"])
                    outbound_ip = (
                        proxy_text_request(
                            proxy_url,
                            VLESS_IP_CHECK_URL,
                            timeout=PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
                        )
                        if VLESS_CHECK_OUTBOUND_IP and not args.skip_ip_check
                        else str(node.get("ip") or f"unchecked-{node['source_index']}")
                    )
                    measurement = measure_polymarket_proxy(
                        proxy_url,
                        wallet_offset=int(node.get("source_index", 0) or 0),
                    )
                else:
                    # A standby/offline VPN is turned on only for this ranking test.
                    stop_node_xray(node)
                    temporary_proc, temporary_handle = start_xray_node(
                        xray_path,
                        node["config_path"],
                        int(node["port"]),
                        node["log_path"],
                        logger=logger,
                        source=f"SORTXRAY{node['source_index']}",
                    )
                    queue.register_pid(temporary_proc.pid, "xray-sort")
                    temporary_pid_registered = True
                    proxy_url = str(node["proxy"])
                    outbound_ip = (
                        proxy_text_request(
                            proxy_url,
                            VLESS_IP_CHECK_URL,
                            timeout=PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
                        )
                        if VLESS_CHECK_OUTBOUND_IP and not args.skip_ip_check
                        else f"unchecked-{node['source_index']}"
                    )
                    measurement = measure_polymarket_proxy(
                        proxy_url,
                        wallet_offset=int(node.get("source_index", 0) or 0),
                    )

                return {
                    "index": index,
                    "ok": True,
                    "ip": outbound_ip,
                    "measurement": measurement,
                    "active_at_start": active_at_start,
                    "error": "",
                }
            except Exception as exc:
                return {
                    "index": index,
                    "ok": False,
                    "ip": "",
                    "measurement": {},
                    "active_at_start": active_at_start,
                    "error": repr(exc),
                }
            finally:
                if not active_at_start:
                    if temporary_proc is not None:
                        if temporary_pid_registered:
                            try:
                                queue.unregister_pid(temporary_proc.pid)
                            except Exception:
                                pass
                        stop_process(temporary_proc)
                    if temporary_handle is not None:
                        try:
                            temporary_handle.close()
                        except Exception:
                            pass
                    node["xray_proc"] = None
                    node["xray_handle"] = None
                    node["healthy"] = False
                # Inactive/standby nodes remain locked out of the wallet pool until
                # the whole cycle is finished and its ranking is applied atomically.
                if active_at_start:
                    node["ranking_testing"] = False

    def continuous_sort_loop() -> None:
        cycle = 0
        previous_signature = tuple(
            sorted(
                [
                    index
                    for index, node in enumerate(nodes)
                    if (node.get("healthy") or node.get("standby"))
                    and bool(node.get("ranking_cycle_pass", True))
                ],
                key=lambda index: _vpn_rank_key(nodes[index]),
            )
        )

        while not continuous_sort_stop.is_set():
            candidates = [
                index
                for index, node in enumerate(nodes)
                if bool(node.get("ranking_candidate") or node.get("ever_worked"))
                and not bool(node.get("reality_quarantined"))
                and not bool(node.get("dead_recheck_in_progress"))
            ]
            if not candidates:
                continuous_sort_stop.wait(max(float(VPN_CONTINUOUS_SORT_PAUSE_SECONDS), 1.0))
                continue

            cycle += 1
            with continuous_sort_progress_lock:
                continuous_sort_progress.update(
                    {
                        "running": True,
                        "cycle": cycle,
                        "tested": 0,
                        "total": len(candidates),
                        "passed": 0,
                        "failed": 0,
                    }
                )

            results: dict[int, dict[str, Any]] = {}
            workers = max(1, min(len(candidates), int(VPN_CONTINUOUS_SORT_WORKERS)))
            executor = ThreadPoolExecutor(max_workers=workers)
            futures = {
                executor.submit(continuous_sort_candidate, index): index
                for index in candidates
            }
            try:
                for future in as_completed(futures):
                    if continuous_sort_stop.is_set():
                        break
                    index = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = {
                            "index": index,
                            "ok": False,
                            "ip": "",
                            "measurement": {},
                            "active_at_start": bool(nodes[index].get("healthy")),
                            "error": repr(exc),
                        }
                    results[index] = result
                    with continuous_sort_progress_lock:
                        continuous_sort_progress["tested"] = int(
                            continuous_sort_progress.get("tested", 0)
                        ) + 1
                        key = "passed" if result.get("ok") else "failed"
                        continuous_sort_progress[key] = int(
                            continuous_sort_progress.get(key, 0)
                        ) + 1
            finally:
                if continuous_sort_stop.is_set():
                    for future in futures:
                        future.cancel()
                    executor.shutdown(wait=True, cancel_futures=True)
                else:
                    executor.shutdown(wait=True)

            if continuous_sort_stop.is_set():
                break

            successful = [
                index for index, result in results.items() if result.get("ok")
            ]
            successful.sort(
                key=lambda index: (
                    safe_float(
                        results[index].get("measurement", {}).get("speed_score_ms"),
                        float("inf"),
                    ),
                    int(
                        results[index].get("measurement", {}).get(
                            "speed_failures", 0
                        )
                        or 0
                    ),
                    int(nodes[index].get("source_index", 0) or 0),
                )
            )

            # One work-eligible config per outbound IP; duplicates stay ranking candidates
            # but never receive wallet work in this cycle.
            winners: list[int] = []
            seen_ips: set[str] = set()
            duplicate_count = 0
            for index in successful:
                outbound_ip = str(results[index].get("ip") or "")
                if (
                    VLESS_REQUIRE_UNIQUE_OUTBOUND_IPS
                    and outbound_ip
                    and outbound_ip in seen_ips
                ):
                    duplicate_count += 1
                    continue
                if outbound_ip:
                    seen_ips.add(outbound_ip)
                winners.append(index)

            winner_set = set(winners)
            with continuous_sort_apply_lock:
                for index in candidates:
                    with node_runtime_locks[index]:
                        node = nodes[index]
                        result = results.get(index)
                        if result is None:
                            continue
                        if result.get("ok"):
                            apply_speed_measurement(node, result.get("measurement") or {})
                            node["ip"] = str(result.get("ip") or node.get("ip") or "")
                            node["ever_worked"] = True
                            node["ranking_candidate"] = True
                            node["ranking_failures"] = 0
                            node["ranking_cycle_pass"] = index in winner_set
                            if index in winner_set:
                                if not node.get("healthy"):
                                    node["standby"] = True
                                node["last_error"] = ""
                            else:
                                if not node.get("healthy"):
                                    node["standby"] = False
                                node["last_error"] = (
                                    f"continuous-sort duplicate outbound IP {node.get('ip')}"
                                )
                        else:
                            node["ranking_candidate"] = bool(node.get("ever_worked"))
                            node["ranking_cycle_pass"] = False
                            node["ranking_failures"] = int(
                                node.get("ranking_failures", 0) or 0
                            ) + 1
                            node["last_error"] = str(result.get("error") or "ranking test failed")
                            if not node.get("healthy"):
                                node["standby"] = False

                for index in candidates:
                    nodes[index]["ranking_testing"] = False

            signature = tuple(winners)
            compare_limit = max(1, min(len(nodes), int(cpu_parallelism)))
            old_top = previous_signature[:compare_limit]
            new_top = signature[:compare_limit]
            changed = new_top != old_top
            event = {
                "cycle": cycle,
                "tested": len(results),
                "passed": len(successful),
                "failed": max(0, len(results) - len(successful)),
                "duplicates": duplicate_count,
                "eligible": len(winners),
                "changed": changed,
                "old_signature": previous_signature,
                "new_signature": signature,
                "old_top": old_top,
                "new_top": new_top,
            }
            previous_signature = signature
            with continuous_sort_events_lock:
                continuous_sort_events.append(event)
            with continuous_sort_progress_lock:
                continuous_sort_progress["running"] = False

            if continuous_sort_stop.wait(
                max(0.0, float(VPN_CONTINUOUS_SORT_PAUSE_SECONDS))
            ):
                break

        with continuous_sort_apply_lock:
            for node in nodes:
                node["ranking_testing"] = False
        with continuous_sort_progress_lock:
            continuous_sort_progress["running"] = False

    def start_continuous_sorter() -> None:
        nonlocal continuous_sort_thread
        if not VPN_CONTINUOUS_SORT_ENABLED or continuous_sort_thread is not None:
            return
        continuous_sort_thread = threading.Thread(
            target=continuous_sort_loop,
            name="continuous-working-vpn-sort",
            daemon=True,
        )
        continuous_sort_thread.start()
        logger.log(
            f"[proxy:continuous-sort-start] workers={VPN_CONTINUOUS_SORT_WORKERS} "
            f"pause={VPN_CONTINUOUS_SORT_PAUSE_SECONDS}s "
            "scope=previously-working-only; startup-never-working nodes excluded",
            source="PROXY",
        )

    def process_continuous_sort_events() -> None:
        events: list[dict[str, Any]] = []
        with continuous_sort_events_lock:
            while continuous_sort_events:
                events.append(continuous_sort_events.popleft())
        for event in events:
            logger.log(
                f"[proxy:continuous-sort-cycle] cycle={event['cycle']} "
                f"tested={event['tested']} passed={event['passed']} "
                f"failed={event['failed']} duplicates={event['duplicates']} "
                f"eligible={event['eligible']} changed={event['changed']}",
                source="PROXY",
            )
            if event.get("changed"):
                logger.log(
                    f"[proxy:continuous-sort-changed] cycle={event['cycle']} "
                    f"old_top={list(event.get('old_top') or ())} "
                    f"new_top={list(event.get('new_top') or ())}",
                    source="PROXY",
                )
                rebalance_best_active_nodes()
            persist_vpn_test_memory(
                f"continuous-sort-cycle-{event['cycle']}"
            )
            update_active_file(force=True)
            show_dashboard(force=True)


    def background_dead_recheck_candidate(index: int) -> dict[str, Any]:
        """Test one dead VPN with a temporary Xray, never blocking wallet workers."""
        node = nodes[index]
        lock = node_runtime_locks[index]
        with lock:
            # State may have changed while this item was waiting in the executor.
            if (
                node.get("healthy")
                or node.get("standby")
                or node.get("ranking_testing")
                or node.get("dead_recheck_in_progress")
            ):
                return {
                    "index": index,
                    "ok": False,
                    "skipped": True,
                    "ip": "",
                    "measurement": {},
                    "error": "node state changed before background dead recheck",
                }

            node["dead_recheck_in_progress"] = True
            temporary_proc: subprocess.Popen | None = None
            temporary_handle: Any = None
            registered_pid = False
            try:
                stop_node_xray(node)
                temporary_proc, temporary_handle = start_xray_node(
                    xray_path,
                    node["config_path"],
                    int(node["port"]),
                    node["log_path"],
                    logger=logger,
                    source=f"DEADXRAY{node['source_index']}",
                )
                queue.register_pid(temporary_proc.pid, "xray-dead-recheck")
                registered_pid = True
                proxy_url = str(node["proxy"])
                outbound_ip = (
                    proxy_text_request(
                        proxy_url,
                        VLESS_IP_CHECK_URL,
                        timeout=PROXY_HEALTH_CHECK_TIMEOUT_SECONDS,
                    )
                    if VLESS_CHECK_OUTBOUND_IP and not args.skip_ip_check
                    else f"unchecked-{node['source_index']}"
                )
                measurement = measure_polymarket_proxy(
                    proxy_url,
                    wallet_offset=int(node.get("source_index", 0) or 0),
                )
                return {
                    "index": index,
                    "ok": True,
                    "skipped": False,
                    "ip": outbound_ip,
                    "measurement": measurement,
                    "error": "",
                }
            except Exception as exc:
                return {
                    "index": index,
                    "ok": False,
                    "skipped": False,
                    "ip": "",
                    "measurement": {},
                    "error": repr(exc),
                }
            finally:
                if temporary_proc is not None:
                    if registered_pid:
                        try:
                            queue.unregister_pid(temporary_proc.pid)
                        except Exception:
                            pass
                    stop_process(temporary_proc)
                if temporary_handle is not None:
                    try:
                        temporary_handle.close()
                    except Exception:
                        pass
                node["xray_proc"] = None
                node["xray_handle"] = None
                node["dead_recheck_in_progress"] = False
            node["retry_cooldown_until"] = 0.0

    def dead_recheck_loop() -> None:
        """Continuously recheck due dead VPNs in a daemon thread."""
        cycle = 0
        while not dead_recheck_stop.is_set():
            if not VPN_DEAD_RECHECK_ENABLED:
                dead_recheck_stop.wait(1.0)
                continue

            now = time.monotonic()
            due = [
                index
                for index, node in enumerate(nodes)
                if (
                    not node.get("healthy")
                    and not node.get("standby")
                    and not node.get("reality_quarantined")
                    and not node.get("ranking_testing")
                    and not node.get("dead_recheck_in_progress")
                    # When continuous sort is ON, it already owns every VPN that
                    # worked at least once. Dead recheck then handles only configs
                    # that have never worked, preventing duplicate tests/log spam.
                    and (
                        not VPN_CONTINUOUS_SORT_ENABLED
                        or not bool(node.get("ever_worked"))
                    )
                    and now >= float(node.get("next_recheck", 0.0))
                )
            ]
            if not due:
                dead_recheck_stop.wait(0.5)
                continue

            cycle += 1
            with dead_recheck_progress_lock:
                dead_recheck_progress.update(
                    {
                        "running": True,
                        "cycle": cycle,
                        "tested": 0,
                        "total": len(due),
                        "passed": 0,
                        "failed": 0,
                    }
                )

            # CPU auto-tune continues to control the effective concurrency, while
            # VPN_DEAD_RECHECK_WORKERS remains the user-configured hard ceiling.
            workers = max(
                1,
                min(
                    len(due),
                    int(VPN_DEAD_RECHECK_WORKERS),
                    int(cpu_parallelism),
                ),
            )
            executor = ThreadPoolExecutor(max_workers=workers)
            futures = {
                executor.submit(background_dead_recheck_candidate, index): index
                for index in due
            }
            try:
                for future in as_completed(futures):
                    if dead_recheck_stop.is_set():
                        break
                    index = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = {
                            "index": index,
                            "ok": False,
                            "skipped": False,
                            "ip": "",
                            "measurement": {},
                            "error": repr(exc),
                        }
                    with dead_recheck_events_lock:
                        dead_recheck_events.append(
                            {"cycle": cycle, **result}
                        )
                    if not result.get("skipped"):
                        with dead_recheck_progress_lock:
                            dead_recheck_progress["tested"] = int(
                                dead_recheck_progress.get("tested", 0)
                            ) + 1
                            key = "passed" if result.get("ok") else "failed"
                            dead_recheck_progress[key] = int(
                                dead_recheck_progress.get(key, 0)
                            ) + 1
            finally:
                if dead_recheck_stop.is_set():
                    for future in futures:
                        future.cancel()
                    executor.shutdown(wait=True, cancel_futures=True)
                else:
                    executor.shutdown(wait=True)

            with dead_recheck_progress_lock:
                cycle_tested = int(dead_recheck_progress.get("tested", 0) or 0)
                cycle_passed = int(dead_recheck_progress.get("passed", 0) or 0)
                cycle_failed = int(dead_recheck_progress.get("failed", 0) or 0)
                dead_recheck_progress["running"] = False
            with dead_recheck_events_lock:
                dead_recheck_events.append(
                    {
                        "cycle_summary": True,
                        "cycle": cycle,
                        "tested": cycle_tested,
                        "passed": cycle_passed,
                        "failed": cycle_failed,
                    }
                )
            dead_recheck_stop.wait(0.25)

        with dead_recheck_progress_lock:
            dead_recheck_progress["running"] = False

    def start_background_dead_recheck() -> None:
        nonlocal dead_recheck_thread
        if not VPN_DEAD_RECHECK_ENABLED or dead_recheck_thread is not None:
            return
        dead_recheck_thread = threading.Thread(
            target=dead_recheck_loop,
            name="background-dead-vpn-recheck",
            daemon=True,
        )
        dead_recheck_thread.start()
        logger.log(
            f"[proxy:dead-recheck-background-start] "
            f"workers_ceiling={VPN_DEAD_RECHECK_WORKERS} "
            f"interval={PROXY_DEAD_RECHECK_INTERVAL_SECONDS}s "
            f"scope={'startup-never-working-only' if VPN_CONTINUOUS_SORT_ENABLED else 'all-dead'} "
            "wallet_queue_blocking=false",
            source="PROXY",
        )

    def process_dead_recheck_events(allow_rebalance: bool = True) -> None:
        events: list[dict[str, Any]] = []
        with dead_recheck_events_lock:
            while dead_recheck_events:
                events.append(dead_recheck_events.popleft())
        if not events:
            return

        recovered = False
        changed = False
        cycle_summaries = [event for event in events if event.get("cycle_summary")]
        node_events = [event for event in events if not event.get("cycle_summary")]
        with continuous_sort_apply_lock:
            for event in node_events:
                if event.get("skipped"):
                    continue
                index = int(event["index"])
                node = nodes[index]
                with node_runtime_locks[index]:
                    # Never overwrite a node that became active through another path.
                    if node.get("healthy"):
                        continue
                    if event.get("ok"):
                        apply_speed_measurement(
                            node,
                            event.get("measurement") or {},
                        )
                        node["ip"] = str(event.get("ip") or node.get("ip") or "")
                        node["healthy"] = False
                        node["standby"] = True
                        node["ever_worked"] = True
                        node["ranking_candidate"] = True
                        node["ranking_cycle_pass"] = True
                        node["ranking_failures"] = 0
                        node["health_failures"] = 0
                        node["last_error"] = ""
                        node["next_recheck"] = 0.0
                        recovered = True
                        changed = True
                        logger.log(
                            f"[proxy:recovered-standby] node={index} "
                            f"ip={node['ip']} cycle={event['cycle']} "
                            "source=background",
                            source="PROXY",
                        )
                    else:
                        error = str(event.get("error") or "background dead recheck failed")
                        node["healthy"] = False
                        node["standby"] = False
                        node["last_error"] = error
                        node["next_recheck"] = (
                            time.monotonic() + PROXY_DEAD_RECHECK_INTERVAL_SECONDS
                        )
                        changed = True

        for summary in cycle_summaries:
            logger.log(
                f"[proxy:dead-recheck-cycle] cycle={summary['cycle']} "
                f"tested={summary['tested']} recovered={summary['passed']} "
                f"still_dead={summary['failed']} background=true",
                source="PROXY",
            )

        if recovered and allow_rebalance:
            rebalance_best_active_nodes()
        if changed:
            update_active_file(force=True)
            persist_vpn_test_memory("background-dead-node-recheck")
        show_dashboard(force=True)


    def _rebalance_best_active_nodes_unlocked() -> None:
        max_active = max(1, min(len(nodes), int(cpu_parallelism)))
        usable_indexes = [
            index
            for index, node in enumerate(nodes)
            if (node.get("healthy") or node.get("standby"))
            and not bool(node.get("reality_quarantined"))
            and bool(node.get("ranking_cycle_pass", True))
            and not bool(node.get("ranking_testing"))
            and not bool(node.get("dead_recheck_in_progress"))
        ]
        ranked_indexes = sorted(
            usable_indexes,
            key=lambda index: _vpn_rank_key(nodes[index]),
        )
        desired = set(ranked_indexes[:max_active])

        # Slower active nodes that are no longer in the best-N pool are parked.
        for index, node in enumerate(nodes):
            if not node.get("healthy") or node.get("ranking_testing"):
                continue
            if index in desired:
                node["retire_after_batch"] = False
                continue

            if index in worker_states:
                if not node.get("retire_after_batch"):
                    node["retire_after_batch"] = True
                    logger.log(
                        f"[proxy:retire-slower-after-batch] node={index} "
                        f"score_ms={safe_float(node.get('speed_score_ms')):.1f}",
                        source="PROXY",
                    )
            else:
                park_node(node)
                node["retire_after_batch"] = False
                logger.log(
                    f"[proxy:parked-slower] node={index} "
                    f"score_ms={safe_float(node.get('speed_score_ms')):.1f}",
                    source="PROXY",
                )

        active_count = sum(
            1 for node in nodes
            if node.get("healthy") and not node.get("retire_after_batch")
        )
        for index in ranked_indexes:
            if active_count >= max_active:
                break
            node = nodes[index]
            if index not in desired or not node.get("standby") or index in worker_states:
                continue

            ok, error = start_or_restart_node(
                node,
                enforce_unique_ip=True,
                run_speed_test=False,
            )
            if ok:
                active_count += 1
                logger.log(
                    f"[proxy:promoted-best] node={index} ip={node['ip']} "
                    f"rank_score_ms={safe_float(node.get('speed_score_ms')):.1f} "
                    f"active={active_count}/{max_active}",
                    source="PROXY",
                )
            else:
                logger.log(
                    f"[proxy:promotion-failed] node={index} error={error}",
                    source="PROXY",
                    force_error=True,
                )

    def rebalance_best_active_nodes() -> None:
        with continuous_sort_apply_lock:
            _rebalance_best_active_nodes_unlocked()


    def stop_one_excess_worker_for_cpu() -> bool:
        """Immediately remove the slowest excess active node after CPU scale-down."""
        active_indexes = [
            index for index, node in enumerate(nodes) if node.get("healthy")
        ]
        if len(active_indexes) <= cpu_parallelism:
            return False

        active_indexes.sort(
            key=lambda index: _vpn_rank_key(nodes[index]),
            reverse=True,
        )
        node_index = active_indexes[0]
        node = nodes[node_index]
        worker = worker_states.get(node_index)
        if worker is not None:
            worker["forced_stop"] = True
            worker["cpu_scale_down"] = True
            node["retire_after_batch"] = True
            stop_process(worker["proc"])
            logger.log(
                f"[cpu:auto-scale-stop] node={node_index} "
                f"batch={worker.get('batch_id')} new_limit={cpu_parallelism}; "
                "unfinished wallets will return to queue",
                source="CPU",
            )
        else:
            park_node(node)
            node["retire_after_batch"] = False
            logger.log(
                f"[cpu:auto-scale-park] node={node_index} "
                f"new_limit={cpu_parallelism}",
                source="CPU",
            )
        return True

    def stop_excess_heavy_workers_for_tail(target_limit: int) -> int:
        target = max(int(HEAVY_TAIL_MIN_WORKERS), int(target_limit))
        heavy_workers = [
            (node_index, state)
            for node_index, state in worker_states.items()
            if str(state.get("queue_lane") or "") == "heavy"
            and state.get("proc") is not None
            and state["proc"].poll() is None
        ]
        excess = max(0, len(heavy_workers) - target)
        to_stop = min(excess, max(1, int(HEAVY_TAIL_IMMEDIATE_STOP_MAX)))
        if to_stop <= 0:
            return 0

        # Stop the newest Heavy workers first. They have the least in-flight work,
        # and every completed Activity/Market checkpoint remains durable.
        heavy_workers.sort(
            key=lambda item: safe_float(
                item[1].get("started_at_monotonic"),
                0.0,
            ),
            reverse=True,
        )
        stopped = 0
        for node_index, worker in heavy_workers[:to_stop]:
            worker["forced_stop"] = True
            worker["cpu_scale_down"] = True
            worker["tail_scale_down"] = True
            stop_process(worker["proc"])
            stopped += 1
            logger.log(
                f"[heavy-tail:scale-stop] node={node_index} "
                f"batch={worker.get('batch_id')} target={target}; "
                "unfinished wallet returns to queue with checkpoints preserved",
                source="CPU",
            )
        return stopped

    def apply_cpu_auto_tune() -> None:
        nonlocal cpu_parallelism
        nonlocal heavy_tail_parallelism
        nonlocal last_cpu_window_peak
        nonlocal last_cpu_window_average
        nonlocal last_cpu_all_time_peak
        nonlocal retry76_events_since_cpu_tune

        # consume_window() resets the accumulator, so every decision is based only
        # on the newly completed window. No older DOWN decision can remain queued.
        window = cpu_monitor.consume_window()
        samples = int(window.get("window_samples", 0) or 0)
        peak = safe_float(window.get("window_peak"))
        average = safe_float(window.get("window_average"))
        all_time_peak = safe_float(window.get("all_time_peak"))
        last_cpu_window_peak = peak
        last_cpu_window_average = average
        last_cpu_all_time_peak = all_time_peak

        if samples <= 0:
            logger.log(
                f"[cpu:auto-tune-hold] no CPU samples; "
                f"limit remains {cpu_parallelism}",
                source="CPU",
                force_error=True,
            )
            return

        old_limit = cpu_parallelism
        # Network/retry counters are still collected so the diagnostic log can
        # explain API pressure, but they cannot change the runtime limit.
        network = _cpu_autotune_network_guard()
        retry76_pressure = int(retry76_events_since_cpu_tune)
        retry76_events_since_cpu_tune = 0
        network["retry76"] = retry76_pressure

        available_working = sum(
            1 for node in nodes if node.get("healthy") or node.get("standby")
        )
        natural_cap = max(1, int(available_working))
        cpu_parallelism, action = _cpu_autotune_next_limit(
            cpu_parallelism, average, natural_cap
        )

        queue_counts_now = queue.counts()
        queue_stats_now = queue.retry_stats()
        heavy_tail_mode_now = _is_heavy_tail_mode(
            queue_counts_now,
            queue_stats_now,
        )
        old_heavy_tail_limit = int(heavy_tail_parallelism)
        if heavy_tail_mode_now:
            heavy_tail_parallelism, heavy_tail_action = _heavy_tail_next_limit(
                heavy_tail_parallelism,
                average,
                retry76_pressure,
            )
        else:
            heavy_tail_parallelism = max(
                int(HEAVY_TAIL_MIN_WORKERS),
                min(
                    int(HEAVY_TAIL_INITIAL_WORKERS),
                    int(HEAVY_TAIL_MAX_WORKERS),
                    int(cpu_parallelism),
                ),
            )
            heavy_tail_action = "inactive-reset"

        logger.log(
            f"[cpu:auto-tune] window={CPU_AUTO_TUNE_WINDOW_SECONDS:g}s "
            f"samples={samples} peak_window={peak:.2f}% "
            f"avg_window={average:.2f}% all_time_peak={all_time_peak:.2f}% "
            f"rule=current-window-only "
            f"down_if_avg=>={CPU_AUTO_TUNE_HIGH_PERCENT:.1f}% "
            f"up_by={max(1, int(VPN_AUTO_TUNE_UP_STEP))} "
            f"up_if_avg<{CPU_AUTO_TUNE_LOW_PERCENT:.1f}% "
            f"stable_if_avg={CPU_AUTO_TUNE_LOW_PERCENT:.1f}-{CPU_AUTO_TUNE_HIGH_PERCENT:.1f}% "
            f"action={action} "
            f"network_diagnostics_only={network} "
            f"working_available={available_working} natural_cap={natural_cap} "
            f"VPN_MAX_ACTIVE_NODES={cpu_parallelism} "
            f"VPN_DEAD_RECHECK_WORKERS={cpu_parallelism}",
            source="CPU",
        )

        logger.log(
            f"[heavy-tail:auto-tune] active={heavy_tail_mode_now} "
            f"remaining={max(0, int(queue_counts_now.get('total', 0))-int(queue_counts_now.get('done', 0))-int(queue_counts_now.get('failed', 0)))} "
            f"cpu_avg={average:.2f}% retry76_window={retry76_pressure} "
            f"limit={heavy_tail_parallelism} action={heavy_tail_action} "
            f"bounds={HEAVY_TAIL_MIN_WORKERS}-{HEAVY_TAIL_MAX_WORKERS} "
            f"heavy_http={HEAVY_CLOSED_FETCH_WORKERS}+{HEAVY_ACTIVITY_FETCH_WORKERS}+{HEAVY_ACTIVITY_WINDOW_WORKERS}",
            source="CPU",
        )
        if (
            heavy_tail_mode_now
            and heavy_tail_parallelism < old_heavy_tail_limit
        ):
            stop_excess_heavy_workers_for_tail(heavy_tail_parallelism)

        # Exactly one immediate worker is stopped only for a DOWN decision made by
        # this current window. Raising/holding the limit never consumes a stale stop.
        if cpu_parallelism < old_limit and CPU_AUTO_TUNE_IMMEDIATE_SCALE_DOWN:
            stop_one_excess_worker_for_cpu()
        rebalance_best_active_nodes()
        update_active_file(force=True)


    rebalance_best_active_nodes()
    update_active_file(force=True)
    if cpu_monitor.available:
        cpu_monitor.start()
        if CPU_AUTO_TUNE_ENABLED:
            next_cpu_tune = (
                time.monotonic() + float(CPU_AUTO_TUNE_WINDOW_SECONDS)
            )
            logger.log(
                f"[cpu:auto-tune-start] start_limit={cpu_parallelism} "
                f"bounds=natural(1..available_vpns) current_window_only=true "
                f"network_guard=diagnostics-only "
                f"http_threads_per_worker~={CLOSED_FETCH_WORKERS + ACTIVITY_FETCH_WORKERS + ACTIVITY_WINDOW_WORKERS} "
                f"sample_every={CPU_AUTO_TUNE_SAMPLE_INTERVAL_SECONDS}s "
                f"window={CPU_AUTO_TUNE_WINDOW_SECONDS}s "
                f"down_if_avg=>={CPU_AUTO_TUNE_HIGH_PERCENT}% "
                f"up_by={max(1, int(VPN_AUTO_TUNE_UP_STEP))} "
                f"up_if_avg<{CPU_AUTO_TUNE_LOW_PERCENT}% stable_band="
                f"{CPU_AUTO_TUNE_LOW_PERCENT}-{CPU_AUTO_TUNE_HIGH_PERCENT}%",
                source="CPU",
            )
        else:
            logger.log(
                f"[cpu:monitor-start] auto_tune=OFF fixed_limit={cpu_parallelism} "
                f"sample_every={CPU_AUTO_TUNE_SAMPLE_INTERVAL_SECONDS}s "
                f"diagnostics=ON",
                source="CPU",
            )
    else:
        logger.log(
            "[cpu:monitor-unavailable] system CPU counters are unavailable",
            source="CPU",
            force_error=True,
        )

    if RAM_SAFETY_STOP_ENABLED and ram_monitor.available:
        ram_monitor.start()
        logger.log(
            f"[ram:safety-monitor-start] sample_every={RAM_SAFETY_SAMPLE_INTERVAL_SECONDS:g}s "
            f"window={RAM_SAFETY_WINDOW_SECONDS:g}s "
            f"required_samples={ram_monitor.required_samples} "
            f"safe_stop_if_avg>{RAM_SAFETY_STOP_PERCENT:.1f}%",
            source="RAM",
        )
    elif RAM_SAFETY_STOP_ENABLED:
        logger.log(
            "[ram:safety-monitor-unavailable] system RAM counters are unavailable; "
            "automatic RAM stop cannot run",
            source="RAM",
            force_error=True,
        )

    start_continuous_sorter()
    start_background_dead_recheck()

    def all_cache_dirs() -> list[Path]:
        dirs = sorted(
            path
            for pattern in ("bucket_*", f"{HEAVY_TAIL_BUCKET_SPILLOVER_PREFIX}_*")
            for path in cache_root.glob(pattern)
            if path.is_dir()
        )
        return legacy_sources + dirs

    memory_sync_state.update(
        {
            "dirty": False,
            "last_sync": time.monotonic(),
            "rows": int(queue.counts().get("done", 0)),
        }
    )

    def mirror_persistent_memory(*, force: bool = False) -> int:
        now = time.monotonic()
        if not force and not memory_sync_state["dirty"]:
            return int(memory_sync_state["rows"])
        if (
            not force
            and now - float(memory_sync_state["last_sync"])
            < max(0.5, float(TEST_MEMORY_SYNC_SECONDS))
        ):
            return int(memory_sync_state["rows"])
        sources = [root / TEST_MEMORY_FILE_NAME]
        sources.extend(
            path
            for directory in all_cache_dirs()
            for path in test_memory_paths_for_directory(directory)
        )
        try:
            rows = merge_test_memory_files(
                sources,
                root / TEST_MEMORY_FILE_NAME,
                min_tested_at_ms=refresh_since_ms,
            )
            memory_sync_state.update(
                {"dirty": False, "last_sync": now, "rows": rows}
            )
            logger.log(
                f"[memory:mirrored] rows={rows} file={root / TEST_MEMORY_FILE_NAME}",
                source="QUEUE",
            )
            return rows
        except Exception as exc:
            memory_sync_state["dirty"] = True
            logger.log(
                f"[memory:mirror-failed] error={type(exc).__name__}: {exc}",
                source="QUEUE",
                force_error=True,
            )
            return int(memory_sync_state["rows"])

    def sync_completed() -> int:
        # تعداد والت‌هایی را برمی‌گرداند که همین لحظه از حافظه Worker وارد وضعیت done شدند.
        # نسخه قبلی این مقدار را دور می‌ریخت؛ در نتیجه داشبورد تا Refresh دوره‌ای ثابت می‌ماند.
        changed = queue.sync_completed_from_dirs(
            all_cache_dirs(),
            min_tested_at_ms=refresh_since_ms,
        )
        if changed:
            memory_sync_state["dirty"] = True
        mirror_persistent_memory()
        return changed

    def sync_live_score_outputs(
        *,
        force: bool = False,
        force_xlsx: bool = False,
    ) -> dict[str, Any]:
        """Publish user-facing root score files without waiting for final exit."""
        if not GLOBAL_QUEUE_LIVE_OUTPUT_SYNC_ENABLED:
            return dict(live_output_state)
        now = time.monotonic()
        csv_due = bool(
            force
            or (
                live_output_state["csv_dirty"]
                and now - float(live_output_state["last_csv_sync_monotonic"])
                >= max(1.0, float(GLOBAL_QUEUE_LIVE_CSV_SYNC_SECONDS))
            )
        )
        xlsx_due = bool(
            force_xlsx
            or (
                live_output_state["xlsx_dirty"]
                and now - float(live_output_state["last_xlsx_sync_monotonic"])
                >= max(1.0, float(GLOBAL_QUEUE_LIVE_XLSX_SYNC_SECONDS))
            )
        )
        if not csv_due and not xlsx_due:
            return dict(live_output_state)

        started = time.monotonic()
        try:
            result = _write_global_live_score_outputs(
                root,
                all_cache_dirs(),
                refresh_since_ms=refresh_since_ms,
                write_csv=csv_due,
                write_xlsx=xlsx_due,
            )
        except Exception as exc:
            elapsed = max(0.0, time.monotonic() - started)
            live_output_state["last_attempt_epoch"] = time.time()
            live_output_state["last_error"] = f"{type(exc).__name__}: {exc}"
            message = (
                f"[output:checkpoint-failed] stage=collect scored_rows=unknown "
                f"csv_due={csv_due} xlsx_due={xlsx_due} elapsed={elapsed:.2f}s "
                f"error={live_output_state['last_error']}"
            )
            logger.log(message, source="OUTPUT", force_error=True)
            _diagnostic_append(
                [
                    "",
                    f"[{_log_timestamp()}] LIVE OUTPUT CHECKPOINT FAILURE build={BUILD_ID}",
                    message,
                    *traceback.format_exc().rstrip().splitlines()[-20:],
                ]
            )
            return dict(live_output_state)
        elapsed = max(0.0, time.monotonic() - started)
        live_output_state["last_attempt_epoch"] = time.time()
        live_output_state["scored_rows"] = int(result.get("scored_rows", 0) or 0)

        errors: list[str] = []
        csv_error = str(result.get("csv_error") or "")
        if csv_due:
            live_output_state["csv_method"] = str(result.get("csv_method") or "unknown")
            if csv_error:
                errors.append(f"csv={csv_error}")
                live_output_state["csv_dirty"] = True
            else:
                live_output_state["csv_dirty"] = False
                live_output_state["last_csv_sync_monotonic"] = now
                live_output_state["last_csv_sync_epoch"] = time.time()

        xlsx_error = str(result.get("xlsx_error") or "")
        if xlsx_due:
            live_output_state["xlsx_method"] = str(result.get("xlsx_method") or "unknown")
            if xlsx_error:
                errors.append(f"xlsx={xlsx_error}")
                live_output_state["xlsx_dirty"] = True
            else:
                live_output_state["xlsx_dirty"] = False
                live_output_state["last_xlsx_sync_monotonic"] = now
                live_output_state["last_xlsx_sync_epoch"] = time.time()

        live_output_state["last_error"] = " | ".join(errors)
        if errors:
            message = (
                f"[output:checkpoint-failed] scored_rows={live_output_state['scored_rows']} "
                f"csv_due={csv_due} xlsx_due={xlsx_due} elapsed={elapsed:.2f}s "
                f"error={live_output_state['last_error']}"
            )
            logger.log(message, source="OUTPUT", force_error=True)
            _diagnostic_append(
                [
                    "",
                    f"[{_log_timestamp()}] LIVE OUTPUT CHECKPOINT FAILURE build={BUILD_ID}",
                    message,
                ]
            )
        else:
            logger.log(
                f"[output:checkpoint] scored_rows={live_output_state['scored_rows']} "
                f"csv={live_output_state['csv_method'] if csv_due else 'not-due'} "
                f"xlsx={live_output_state['xlsx_method'] if xlsx_due else 'not-due'} "
                f"elapsed={elapsed:.2f}s",
                source="OUTPUT",
            )
        return dict(live_output_state)

    def write_worker_crash_report(
        *,
        node_index: int,
        state: dict[str, Any],
        return_code: int,
    ) -> str:
        """Append one compact but complete crash report; called only on unexpected exits."""
        tail = list(state.get("output_tail") or [])
        last_meaningful = ""
        for line in reversed(tail):
            stripped = str(line).strip()
            if stripped:
                last_meaningful = stripped
                break
        timestamp = _log_timestamp()
        wallets = sorted(str(wallet) for wallet in state.get("wallets") or [])
        lines = [
            "=" * 100,
            f"[{timestamp}] WORKER CRASH",
            f"build={BUILD_ID}",
            f"node={node_index} pid={state['proc'].pid} exit={return_code}",
            f"batch={state.get('batch_id')} bucket={state.get('bucket')}",
            f"wallet_count={len(wallets)} wallets={'|'.join(wallets)}",
            f"worker_age_seconds={max(0.0, time.monotonic() - float(state.get('started_at_monotonic', time.monotonic()))):.1f}",
            f"last_line={last_meaningful}",
            "OUTPUT_TAIL:",
        ]
        lines.extend(tail[-max(20, int(WORKER_OUTPUT_TAIL_LINES)):])
        lines.append("=" * 100)
        with worker_crash_path.open("a", encoding="utf-8") as crash_file:
            crash_file.write("\n".join(lines) + "\n")
            crash_file.flush()
        _diagnostic_append(
            [
                "",
                "=" * 100,
                f"[{timestamp}] IMMEDIATE WORKER CRASH DIGEST build={BUILD_ID}",
                *compact_worker_crash_report_lines(lines),
                "=" * 100,
            ]
        )
        return last_meaningful

    def launch_batch(
        node_index: int,
        preferred_lane: str | None = None,
        *,
        allow_spillover: bool = False,
    ) -> bool:
        node = nodes[node_index]
        busy_buckets = {int(state["bucket"]) for state in worker_states.values()}
        busy_output_dirs = {
            str(Path(state["bucket_dir"]).resolve())
            for state in worker_states.values()
        }
        claim = queue.claim_batch(
            node_index, busy_buckets, preferred_lane=preferred_lane
        )
        used_spillover = False
        if claim is None and allow_spillover:
            claim = queue.claim_spillover_batch(
                node_index,
                busy_buckets,
                busy_output_dirs,
                cache_root,
                preferred_lane=str(preferred_lane or "heavy"),
            )
            used_spillover = claim is not None
        if claim is None:
            return False
        (
            batch_id,
            bucket,
            legacy_dir,
            seeds,
            queue_lane,
            first_retry_count,
            work_dir,
            resume_fallback_dir,
        ) = claim
        canonical_bucket_dir = cache_root / f"bucket_{bucket:03d}"
        bucket_dir = Path(work_dir) if str(work_dir).strip() else canonical_bucket_dir
        ensure_dir(bucket_dir)
        effective_fallback_dir = str(resume_fallback_dir or legacy_dir or "").strip()
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
            "--refresh-since-ms",
            str(refresh_since_ms),
        ]
        if effective_fallback_dir:
            command.extend(["--fallback-out-dir", effective_fallback_dir])
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

        worker_env = dict(env)
        worker_env[_WORKER_LANE_ENV] = str(queue_lane)
        if queue_lane == "heavy":
            worker_env[_WORKER_CLOSED_FETCH_ENV] = str(HEAVY_CLOSED_FETCH_WORKERS)
            worker_env[_WORKER_ACTIVITY_FETCH_ENV] = str(HEAVY_ACTIVITY_FETCH_WORKERS)
            worker_env[_WORKER_ACTIVITY_WINDOW_ENV] = str(HEAVY_ACTIVITY_WINDOW_WORKERS)
        else:
            worker_env[_WORKER_CLOSED_FETCH_ENV] = str(CLOSED_FETCH_WORKERS)
            worker_env[_WORKER_ACTIVITY_FETCH_ENV] = str(ACTIVITY_FETCH_WORKERS)
            worker_env[_WORKER_ACTIVITY_WINDOW_ENV] = str(ACTIVITY_WINDOW_WORKERS)

        proc = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=worker_env,
            cwd=str(script_path.parent),
            creationflags=(subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0),
        )
        queue.register_pid(proc.pid, "worker")
        output_tail: deque[str] = deque(maxlen=max(20, int(WORKER_OUTPUT_TAIL_LINES)))
        activity_state: dict[str, Any] = {
            "last_output_monotonic": time.monotonic(),
            "last_output_line": "",
        }
        thread = threading.Thread(
            target=stream_process_output,
            args=(proc, f"N{node_index}", logger, output_tail, activity_state),
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
            "canonical_bucket_dir": canonical_bucket_dir,
            "spillover": bool(used_spillover or str(work_dir).strip()),
            "resume_fallback_dir": effective_fallback_dir,
            "batch_file": batch_file,
            "wallets": wallets_set,
            "started_at_monotonic": time.monotonic(),
            "started_at_epoch": time.time(),
            "last_durable_progress_monotonic": time.monotonic(),
            "last_durable_done_count": 0,
            "activity_state": activity_state,
            "forced_stop": False,
            "cpu_scale_down": False,
            "stall_timeout": False,
            "output_tail": output_tail,
            "queue_lane": queue_lane,
            "heavy": queue_lane == "heavy",
            "first_retry_count": int(first_retry_count),
        }
        node["busy_batch"] = batch_id
        if used_spillover:
            logger.log(
                f"[tail-backfill:spillover] node={node_index} "
                f"wallet={next(iter(wallets_set), '')} bucket={bucket} "
                f"out={bucket_dir} fallback={effective_fallback_dir or 'none'} "
                f"reason=ready-wallet-blocked-by-busy-bucket",
                source="QUEUE",
            )
        logger.log(
            f"[worker:start] node={node_index} batch={batch_id} bucket={bucket} "
            f"wallets={len(seeds)} lane={queue_lane} retry_count={first_retry_count}",
            source="QUEUE",
        )
        update_active_file(force=True)
        return True

    def recycle_stalled_workers(now: float) -> None:
        nonlocal last_worker_stall_check
        nonlocal retry76_events_since_cpu_tune
        if now - last_worker_stall_check < max(1.0, float(WORKER_STALL_CHECK_INTERVAL_SECONDS)):
            return
        last_worker_stall_check = now
        normal_progress_timeout = max(30.0, float(WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS))
        normal_silence_timeout = max(30.0, float(WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS))
        for node_index, state in list(worker_states.items()):
            queue_lane = str(state.get("queue_lane") or "fresh")
            if queue_lane == "heavy":
                progress_timeout = max(
                    normal_progress_timeout,
                    float(HEAVY_WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS),
                )
                silence_timeout = max(
                    normal_silence_timeout,
                    float(HEAVY_WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS),
                )
            elif queue_lane == "retry":
                progress_timeout = max(
                    normal_progress_timeout,
                    float(RETRY_WORKER_NO_DURABLE_PROGRESS_TIMEOUT_SECONDS),
                )
                silence_timeout = max(
                    normal_silence_timeout,
                    float(RETRY_WORKER_OUTPUT_SILENCE_TIMEOUT_SECONDS),
                )
            else:
                progress_timeout = normal_progress_timeout
                silence_timeout = normal_silence_timeout
            proc = state.get("proc")
            if proc is None or proc.poll() is not None or state.get("stall_timeout"):
                continue
            wallets_set = set(state.get("wallets") or set())
            try:
                durable_done = len(
                    load_test_memory_from_directory(
                        state["bucket_dir"],
                        min_tested_at_ms=refresh_since_ms,
                    )
                    & wallets_set
                )
            except Exception:
                durable_done = int(state.get("last_durable_done_count", 0) or 0)
            previous_done = int(state.get("last_durable_done_count", 0) or 0)
            if durable_done > previous_done:
                state["last_durable_done_count"] = durable_done
                state["last_durable_progress_monotonic"] = now
                continue
            started = float(state.get("started_at_monotonic", now) or now)
            last_progress = float(state.get("last_durable_progress_monotonic", started) or started)
            age = max(0.0, now - started)
            no_progress = max(0.0, now - last_progress)
            activity = state.get("activity_state") or {}
            last_output_age = max(
                0.0, now - float(activity.get("last_output_monotonic", started) or started)
            )
            silent_stall = no_progress >= progress_timeout and last_output_age >= silence_timeout
            if not silent_stall:
                continue
            state["stall_timeout"] = True
            state["forced_stop"] = True
            retry76_events_since_cpu_tune += max(1, int(CPU_AUTO_TUNE_RETRY76_PRESSURE_COUNT))
            last_line = str(activity.get("last_output_line") or "").strip()
            if len(last_line) > 220:
                last_line = last_line[:217] + "..."
            logger.log(
                f"[worker:stall-timeout] node={node_index} batch={state.get('batch_id')} "
                f"reason=silent-no-progress lane={state.get('queue_lane', 'normal')} "
                f"age={age:.1f}s no_durable_progress={no_progress:.1f}s "
                f"durable_done={durable_done}/{len(wallets_set)} "
                f"last_output_age={last_output_age:.1f}s last_output={last_line!r}; "
                f"rerouting unfinished wallets",
                source="QUEUE",
                force_error=True,
            )
            stop_process(proc)

    def active_node_process_health_check(index: int) -> dict[str, Any]:
        """Check only local Xray process liveness; wallet traffic itself proves network health."""
        node = nodes[index]
        lock = node_runtime_locks[index]
        with lock:
            if not node.get("healthy") or node.get("ranking_testing"):
                return {
                    "index": index,
                    "ok": False,
                    "skipped": True,
                    "error": "node state changed before process health check",
                }
            proc = node.get("xray_proc")
            if proc is None:
                return {
                    "index": index,
                    "ok": False,
                    "skipped": False,
                    "error": "active node has no Xray process",
                }
            return_code = proc.poll()
            if return_code is not None:
                return {
                    "index": index,
                    "ok": False,
                    "skipped": False,
                    "error": f"Xray process exited with code {return_code}",
                }
            return {
                "index": index,
                "ok": True,
                "skipped": False,
                "error": "",
            }

    def active_health_loop() -> None:
        # Do not generate extra API traffic for active VPNs. Their real wallet worker
        # requests are the health test; this loop only detects a dead local Xray process.
        while not active_health_stop.wait(
            max(0.5, float(PROXY_HEALTH_CHECK_INTERVAL_SECONDS))
        ):
            indexes = [
                index
                for index, node in enumerate(nodes)
                if node.get("healthy") and not node.get("ranking_testing")
            ]
            for index in indexes:
                if active_health_stop.is_set():
                    break
                result = active_node_process_health_check(index)
                with active_health_events_lock:
                    active_health_events.append(result)

    def process_active_health_events() -> None:
        events: list[dict[str, Any]] = []
        with active_health_events_lock:
            while active_health_events:
                events.append(active_health_events.popleft())
        for result in events:
            if result.get("skipped") or result.get("ok"):
                continue
            index = int(result["index"])
            node = nodes[index]
            if not node.get("healthy"):
                continue
            error = str(result.get("error") or "Xray process health failed")
            node["last_error"] = error
            logger.log(
                f"[proxy:xray-process-dead] node={index} error={error}",
                source="PROXY",
                force_error=True,
            )
            # A dead local Xray process cannot recover by waiting for more failures.
            mark_node_dead(index, error)

    def start_active_health_checker() -> None:
        nonlocal active_health_thread
        if active_health_thread is not None:
            return
        active_health_thread = threading.Thread(
            target=active_health_loop,
            name="active-xray-process-health",
            daemon=True,
        )
        active_health_thread.start()
        logger.log(
            f"[proxy:active-process-health-start] "
            f"interval={PROXY_HEALTH_CHECK_INTERVAL_SECONDS}s "
            f"network_checks=worker-real-wallet-requests",
            source="PROXY",
        )

    start_active_health_checker()

    # VPN startup testing can take many minutes and can generate hundreds of
    # expected failures. Runtime progress/diagnostics must start here, after the
    # working pool is ready, not from the beginning of VPN testing.
    runtime_ready_now = time.monotonic()
    diagnostic_previous_snapshot = runtime_ready_now
    diagnostic_last_progress = runtime_ready_now
    diagnostic_previous_counts = queue.counts()
    runtime_session_start_monotonic = runtime_ready_now
    runtime_session_start_done = int(diagnostic_previous_counts.get("done", 0) or 0)
    diagnostic_previous_data_bytes = _diagnostic_data_footprint()[0]
    try:
        diagnostic_error_offset = diagnostic_error_path.stat().st_size
    except OSError:
        diagnostic_error_offset = 0
    cpu_guard_error_offset = diagnostic_error_offset
    last_durable_completion_monotonic = runtime_ready_now

    # Clamp the configured starting target to the number of VPNs that actually
    # passed startup testing. Example: configured=20, working=15 -> runtime=15.
    startup_working_vpns = sum(
        1 for node in nodes if node.get("healthy") or node.get("standby")
    )
    configured_starting_vpns = max(1, int(VPN_MAX_ACTIVE_NODES))
    natural_start_cap = max(1, int(startup_working_vpns))
    cpu_parallelism = min(configured_starting_vpns, natural_start_cap)
    logger.log(
        f"[proxy:natural-cap] configured_start={configured_starting_vpns} "
        f"working_available={startup_working_vpns} runtime_limit={cpu_parallelism} "
        f"scale_up_step={max(1, int(VPN_AUTO_TUNE_UP_STEP))}",
        source="PROXY",
    )
    rebalance_best_active_nodes()
    update_active_file(force=True)

    # Rebuild the visible root files once before runtime work starts. From this
    # point onward they are checkpointed continuously instead of only at exit.
    sync_live_score_outputs(force=True, force_xlsx=True)
    write_diagnostic_snapshot(reason="startup-ready", force=True)
    exit_code = 0
    try:
        while True:
            if RAM_SAFETY_STOP_ENABLED and ram_monitor.tripped:
                ram_state = ram_monitor.snapshot()
                ram_average = safe_float(ram_state.get("trip_average"))
                ram_current = safe_float(ram_state.get("trip_current"))
                ram_samples = int(ram_state.get("samples", 0) or 0)
                ram_safety_triggered = True
                ram_safety_message = (
                    f"RAM safety stop: {RAM_SAFETY_WINDOW_SECONDS:g}s average "
                    f"{ram_average:.1f}% > {RAM_SAFETY_STOP_PERCENT:.1f}% "
                    f"(current={ram_current:.1f}%, samples={ram_samples})"
                )
                logger.log(
                    f"[ram:safety-stop] {ram_safety_message}; "
                    "stopping workers safely and returning unfinished wallets to pending",
                    source="RAM",
                    force_error=True,
                )
                with console_output_lock:
                    _write_console_message_locked(
                        "\n" + ram_safety_message + "\nSafe shutdown in progress...\n"
                    )
                show_dashboard(force=True)
                write_diagnostic_snapshot(reason="ram-safety-stop", force=True)
                break

            process_continuous_sort_events()
            process_dead_recheck_events()
            process_active_health_events()
            newly_completed = sync_completed()
            if newly_completed:
                last_durable_completion_monotonic = time.monotonic()
                durable_completion_seen_this_run = True
                live_output_state["csv_dirty"] = True
                live_output_state["xlsx_dirty"] = True
                logger.log(
                    f"[progress:update] newly_completed={newly_completed} counts={queue.counts()}",
                    source="QUEUE",
                )
                # با ثبت کامل هر والت در حافظه دائمی، درصد همان لحظه روی CMD تغییر می‌کند.
                show_dashboard(force=True)
            sync_live_score_outputs()

            recycle_stalled_workers(time.monotonic())

            # Collect finished/aborted workers and requeue only wallets that have no durable memory row.
            for node_index, state in list(worker_states.items()):
                proc = state["proc"]
                return_code = proc.poll()
                if return_code is None:
                    continue
                queue.unregister_pid(proc.pid)
                completed = (
                    load_test_memory_from_directory(
                        state["bucket_dir"],
                        min_tested_at_ms=refresh_since_ms,
                    )
                    & state["wallets"]
                )
                cpu_scale_down = bool(state.get("cpu_scale_down"))
                stall_timeout = bool(state.get("stall_timeout") and not cpu_scale_down)
                retry_required = bool(
                    return_code == int(WORKER_RETRY_REQUIRED_EXIT_CODE)
                    and not cpu_scale_down
                    and not stall_timeout
                )
                proxy_failed = bool(return_code == 75 and not cpu_scale_down and not stall_timeout)
                unexpected_exit = bool(
                    return_code not in (0, 75, int(WORKER_RETRY_REQUIRED_EXIT_CODE))
                    and not cpu_scale_down
                    and not stall_timeout
                )
                error = (
                    "worker completed"
                    if return_code == 0
                    else (
                        "worker intentionally stopped by CPU auto-tune"
                        if cpu_scale_down
                        else (
                            "worker recycled after no durable progress"
                            if stall_timeout
                            else (
                                "worker requested wallet retry through a different VPN"
                                if retry_required
                                else (
                                    "worker reported repeated proxy failures"
                                    if proxy_failed
                                    else f"worker exit={return_code} forced_stop={state.get('forced_stop', False)}"
                                )
                            )
                        )
                    )
                )
                failure_kind = (
                    "stall" if stall_timeout else
                    "retry76" if retry_required else
                    "proxy75" if proxy_failed else
                    "unexpected" if unexpected_exit else
                    "normal"
                )
                done_count, requeued = queue.finish_batch(
                    state["wallets"],
                    completed,
                    error,
                    retry_avoid_node=(node_index if (retry_required or stall_timeout) else None),
                    failure_kind=failure_kind,
                )
                runtime_node = nodes[node_index]
                runtime_node["runtime_worker_runs"] = int(
                    runtime_node.get("runtime_worker_runs", 0) or 0
                ) + 1
                runtime_node["runtime_wallet_done"] = int(
                    runtime_node.get("runtime_wallet_done", 0) or 0
                ) + int(done_count)
                runtime_node["runtime_retry76"] = int(
                    runtime_node.get("runtime_retry76", 0) or 0
                ) + int(bool(retry_required))
                runtime_node["runtime_stalls"] = int(
                    runtime_node.get("runtime_stalls", 0) or 0
                ) + int(bool(stall_timeout))
                runtime_node["runtime_unexpected_exits"] = int(
                    runtime_node.get("runtime_unexpected_exits", 0) or 0
                ) + int(bool(unexpected_exit))
                runtime_node["runtime_worker_seconds"] = safe_float(
                    runtime_node.get("runtime_worker_seconds"), 0.0
                ) + max(
                    0.0,
                    time.monotonic()
                    - float(state.get("started_at_monotonic", time.monotonic())),
                )
                crash_last_line = ""
                if unexpected_exit:
                    crash_last_line = write_worker_crash_report(
                        node_index=node_index,
                        state=state,
                        return_code=int(return_code),
                    )
                logger.log(
                    f"[worker:finish] node={node_index} batch={state['batch_id']} "
                    f"exit={return_code} done={done_count} requeued={requeued}",
                    source="QUEUE",
                    force_error=bool(unexpected_exit or proxy_failed or stall_timeout),
                )
                if unexpected_exit:
                    logger.log(
                        f"[worker:crash-captured] node={node_index} batch={state['batch_id']} "
                        f"exit={return_code} crash_log={worker_crash_path} "
                        f"last_line={crash_last_line!r}",
                        source="QUEUE",
                        force_error=True,
                    )
                if retry_required:
                    retry76_events_since_cpu_tune += 1
                    nodes[node_index]["retry_cooldown_until"] = (
                        time.monotonic() + max(0.0, float(WORKER_RETRY_NODE_COOLDOWN_SECONDS))
                    )
                    logger.log(
                        f"[worker:retry-reroute] node={node_index} batch={state['batch_id']} "
                        f"requeued={requeued} cooldown={WORKER_RETRY_NODE_COOLDOWN_SECONDS:.1f}s "
                        "vpn_remains_healthy=true",
                        source="QUEUE",
                    )
                if stall_timeout:
                    nodes[node_index]["retry_cooldown_until"] = (
                        time.monotonic() + max(0.0, float(WORKER_STALL_NODE_COOLDOWN_SECONDS))
                    )
                    logger.log(
                        f"[worker:stall-reroute] node={node_index} batch={state['batch_id']} "
                        f"requeued={requeued} cooldown={WORKER_STALL_NODE_COOLDOWN_SECONDS:.1f}s "
                        "vpn_remains_healthy=true",
                        source="QUEUE",
                        force_error=True,
                    )
                if done_count:
                    last_durable_completion_monotonic = time.monotonic()
                    durable_completion_seen_this_run = True
                    live_output_state["csv_dirty"] = True
                    live_output_state["xlsx_dirty"] = True
                    # در پایان Worker هم بدون انتظار برای Refresh دوره‌ای، درصد را تازه کن.
                    show_dashboard(force=True)
                try:
                    state["batch_file"].unlink(missing_ok=True)
                except Exception:
                    pass
                nodes[node_index]["busy_batch"] = None
                worker_states.pop(node_index, None)
                if proxy_failed:
                    mark_node_dead(node_index, "worker reported repeated proxy failures")
                elif retry_required or stall_timeout:
                    # Controlled reroutes are not proof that the VPN is dead.
                    pass
                elif unexpected_exit and UNEXPECTED_WORKER_EXIT_QUARANTINE:
                    mark_node_dead(
                        node_index,
                        f"unexpected worker exit={return_code}; crash details={worker_crash_path}",
                    )
                elif (
                    nodes[node_index].get("retire_after_batch")
                    and nodes[node_index].get("healthy")
                ):
                    nodes[node_index]["retire_after_batch"] = False
                    park_node(nodes[node_index])
                    logger.log(
                        f"[proxy:parked-after-batch] node={node_index} "
                        f"reason=slower-than-best-{cpu_parallelism}",
                        source="PROXY",
                    )
                update_active_file(force=True)

            counts = queue.counts()
            if counts["total"] and not worker_states:
                finished_total = (
                    counts["done"]
                    if bool(GLOBAL_QUEUE_COMPLETE_ALL_WALLETS)
                    else counts["done"] + counts["failed"]
                )
                if finished_total >= counts["total"]:
                    break

            now = time.monotonic()
            if (
                CPU_AUTO_TUNE_ENABLED
                and cpu_monitor.available
                and next_cpu_tune > 0
                and now >= next_cpu_tune
            ):
                apply_cpu_auto_tune()
                # Configurable averaging cadence; no rapid catch-up after a long blocking call.
                next_cpu_tune = now + float(CPU_AUTO_TUNE_WINDOW_SECONDS)


            # Dead recheck and continuous sort are background network jobs. Active VPNs
            # get no extra API health traffic; real wallet-worker requests drive failover.

            # Keep the fastest runtime CPU-tuned number of VPN nodes active.
            rebalance_best_active_nodes()

            # Reserve a fair share of worker slots for the heavy lane, then fill
            # every remaining slot with fresh/retry work. If one lane is empty or
            # temporarily blocked by busy buckets, the other lane may use the slot.
            queue_counts_state = queue.counts()
            queue_lane_state = queue.retry_stats()
            heavy_tail_mode_state = _is_heavy_tail_mode(
                queue_counts_state,
                queue_lane_state,
            )
            worker_limit = max(
                1,
                min(
                    int(cpu_parallelism),
                    int(heavy_tail_parallelism)
                    if heavy_tail_mode_state
                    else int(cpu_parallelism),
                ),
            )
            heavy_ready_now = int(queue_lane_state.get("heavy_ready", 0) or 0)
            heavy_running_now = sum(
                1
                for state in worker_states.values()
                if str(state.get("queue_lane") or "fresh") == "heavy"
            )
            heavy_target, heavy_share_now = _adaptive_heavy_worker_target(
                worker_limit, queue_lane_state
            )

            def _runtime_node_is_idle(node_index: int, node: dict[str, Any]) -> bool:
                return bool(
                    node.get("healthy")
                    and node_index not in worker_states
                    and not node.get("retire_after_batch")
                    and time.monotonic()
                    >= float(node.get("retry_cooldown_until") or 0.0)
                )

            # Pass 1: fill the reserved heavy quota.
            if heavy_running_now < heavy_target:
                heavy_candidates = sorted(
                    (
                        (node_index, node)
                        for node_index, node in enumerate(nodes)
                        if _runtime_node_is_idle(node_index, node)
                    ),
                    key=lambda item: _heavy_vpn_rank_key(item[1]),
                )
                for node_index, node in heavy_candidates:
                    if len(worker_states) >= worker_limit:
                        break
                    if heavy_running_now >= heavy_target:
                        break
                    if not _runtime_node_is_idle(node_index, node):
                        continue
                    if launch_batch(
                        node_index,
                        preferred_lane="heavy",
                        allow_spillover=heavy_tail_mode_state,
                    ):
                        heavy_running_now += 1

            # Pass 2: fill all remaining capacity. Normal work is preferred.
            # If it is empty, spare nodes take Heavy work ordered by endpoint speed.
            normal_candidates = sorted(
                (
                    (node_index, node)
                    for node_index, node in enumerate(nodes)
                    if _runtime_node_is_idle(node_index, node)
                ),
                key=lambda item: _vpn_rank_key(item[1]),
            )
            for node_index, node in normal_candidates:
                if len(worker_states) >= worker_limit:
                    break
                if not _runtime_node_is_idle(node_index, node):
                    continue
                if launch_batch(node_index, preferred_lane="normal"):
                    continue

            spare_heavy_candidates = sorted(
                (
                    (node_index, node)
                    for node_index, node in enumerate(nodes)
                    if _runtime_node_is_idle(node_index, node)
                ),
                key=lambda item: _heavy_vpn_rank_key(item[1]),
            )
            for node_index, node in spare_heavy_candidates:
                if len(worker_states) >= worker_limit:
                    break
                if not _runtime_node_is_idle(node_index, node):
                    continue
                launch_batch(
                    node_index,
                    preferred_lane="heavy",
                    allow_spillover=heavy_tail_mode_state,
                )

            show_dashboard()
            show_error_notice()
            write_diagnostic_snapshot(reason="periodic")
            time.sleep(0.5)

        sync_completed()
        show_dashboard(force=True)
        counts = queue.counts()
        refresh_cycle_completed = False
        if REFRESH_EXISTING_WALLETS_ON_EACH_RUN:
            refresh_cycle_completed = bool(
                not ram_safety_triggered
                and counts["total"] > 0
                and counts["done"] >= counts["total"]
                and queue.finish_refresh_cycle(refresh_since_ms)
            )
        exit_code = (
            int(RAM_SAFETY_EXIT_CODE)
            if ram_safety_triggered
            else (0 if counts["failed"] == 0 else 1)
        )
        logger.log(
            f"[queue:summary] {counts} ram_safety_triggered={ram_safety_triggered} "
            f"refresh_cycle_completed={refresh_cycle_completed} "
            f"exit_code={exit_code}",
            source="QUEUE",
            force_error=bool(counts["failed"] or ram_safety_triggered),
        )
        return exit_code

    except KeyboardInterrupt:
        _diagnostic_append(
            [
                "",
                "=" * 100,
                f"[{_log_timestamp()}] MANAGER INTERRUPT build={BUILD_ID}",
                "KeyboardInterrupt; unfinished wallets remain recoverable/pending.",
                "=" * 100,
            ]
        )
        write_diagnostic_snapshot(reason="keyboard-interrupt", force=True)
        logger.log("KeyboardInterrupt; workers are returned to pending on next launch", source="SYSTEM")
        return 130
    except Exception:
        fatal_traceback = traceback.format_exc()
        logger.log(fatal_traceback, source="FATAL", force_error=True)
        _diagnostic_append(
            [
                "",
                "=" * 100,
                f"[{_log_timestamp()}] MANAGER FATAL TRACEBACK build={BUILD_ID}",
                *fatal_traceback.rstrip().splitlines(),
                "=" * 100,
            ]
        )
        write_diagnostic_snapshot(reason="fatal-exception", force=True)
        return 1
    finally:
        continuous_sort_stop.set()
        dead_recheck_stop.set()
        active_health_stop.set()
        if continuous_sort_thread is not None:
            continuous_sort_thread.join(timeout=max(90.0, float(VPN_SPEED_TEST_TIMEOUT_SECONDS) * 3.0 + VLESS_START_TIMEOUT_SECONDS))
        if dead_recheck_thread is not None:
            dead_recheck_thread.join(timeout=max(90.0, float(VPN_SPEED_TEST_TIMEOUT_SECONDS) * 3.0 + VLESS_START_TIMEOUT_SECONDS))
        if active_health_thread is not None:
            active_health_thread.join(timeout=max(90.0, float(VPN_SPEED_TEST_TIMEOUT_SECONDS) * 3.0))
        process_continuous_sort_events()
        process_dead_recheck_events(allow_rebalance=False)
        process_active_health_events()
        # Stop children. Any wallet without a durable completed memory row is recovered as
        # pending on the next launch by recover_interrupted().
        for node_index, state in list(worker_states.items()):
            proc = state.get("proc")
            queue.unregister_pid(getattr(proc, "pid", None))
            stop_process(proc)
        for node in nodes:
            was_working = bool(node.get("healthy") or node.get("standby"))
            proc = node.get("xray_proc")
            queue.unregister_pid(getattr(proc, "pid", None))
            stop_process(proc)
            handle = node.get("xray_handle")
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
            node["xray_proc"] = None
            node["xray_handle"] = None
            node["healthy"] = False
            node["standby"] = was_working
            node["busy_batch"] = None
            node["retire_after_batch"] = False
            node["dead_recheck_in_progress"] = False
        try:
            cpu_monitor.stop()
        except Exception:
            pass
        try:
            ram_monitor.stop()
        except Exception:
            pass
        queue.recover_interrupted()
        shutdown_completed = sync_completed()
        if shutdown_completed:
            live_output_state["csv_dirty"] = True
            live_output_state["xlsx_dirty"] = True
        mirror_persistent_memory(force=True)
        sync_live_score_outputs(
            force=True,
            force_xlsx=not ram_safety_triggered,
        )
        update_active_file(force=True)
        show_dashboard(force=True)
        show_error_notice(force=True)
        write_diagnostic_snapshot(reason="shutdown", force=True)
        _finish_live_dashboard()
        if GLOBAL_QUEUE_FINAL_MERGE_ON_EXIT and not ram_safety_triggered:
            try:
                _merge_global_outputs(
                    root,
                    all_cache_dirs(),
                    universe_file,
                    refresh_since_ms=refresh_since_ms,
                )
            except Exception:
                logger.log(traceback.format_exc(), source="MERGE", force_error=True)
        elif GLOBAL_QUEUE_FINAL_MERGE_ON_EXIT and ram_safety_triggered:
            logger.log(
                "[merge:skipped] final merge skipped because RAM safety stop was triggered",
                source="RAM",
                force_error=True,
            )
        queue.close()
        if CLEAN_CONSOLE_DASHBOARD:
            sys.stdout = console_stdout
            sys.stderr = console_stderr
        logger.log("Global queue run finished", source="SYSTEM")
        manager_lock.release()
        logger.close()

def run_ranker_worker(args: argparse.Namespace) -> int:
    global NOT_SAVED_XLSX_CHECKPOINT_EVERY
    if args.skip_final_xlsx:
        NOT_SAVED_XLSX_CHECKPOINT_EVERY = 0
    mode = choose_mode(args)
    refresh_since_ms = max(0, int(args.refresh_since_ms or 0))
    if (
        mode == 2
        and REFRESH_EXISTING_WALLETS_ON_EACH_RUN
        and refresh_since_ms <= 0
    ):
        refresh_since_ms = epoch_milliseconds()

    out_dir = Path(setting(args.out_dir, OUT_DIR))
    ensure_dir(out_dir)

    delay = setting(args.delay, HTTP_DELAY)
    timeout = setting(args.timeout, HTTP_TIMEOUT)
    retries = setting(args.retries, HTTP_RETRIES)
    max_offset = setting(args.max_offset, MAX_LEADERBOARD_OFFSET)
    requested_max_wallets = setting(args.max_wallets, MAX_WALLETS_TO_SCORE)
    requested_min_positions = setting(args.min_positions, MIN_RESOLVED_POSITIONS)
    requested_min_losses = setting(args.min_losses, MIN_LOSING_POSITIONS)
    requested_min_pnl = setting(args.min_pnl, MIN_CLOSED_REALIZED_PNL)
    max_wallets = (
        requested_max_wallets
        if not FULL_WALLET_INCLUSION_MODE and FILTER_MAX_WALLETS_TO_SCORE
        else None
    )
    min_positions = (
        int(requested_min_positions)
        if not FULL_WALLET_INCLUSION_MODE and FILTER_MIN_RESOLVED_POSITIONS
        else 0
    )
    min_losses = (
        int(requested_min_losses)
        if not FULL_WALLET_INCLUSION_MODE and FILTER_MIN_LOSING_POSITIONS
        else 0
    )
    min_pnl = (
        float(requested_min_pnl)
        if not FULL_WALLET_INCLUSION_MODE and FILTER_MIN_CLOSED_REALIZED_PNL
        else 0.0
    )
    smoothing = setting(args.smoothing, SMOOTHING)
    requested_max_positions = setting(
        args.max_positions_per_wallet,
        MAX_POSITIONS_PER_WALLET,
    )
    max_positions_per_wallet = (
        int(requested_max_positions)
        if LIMIT_POSITIONS_PER_WALLET and not FULL_WALLET_INCLUSION_MODE
        else int(MAX_POSITIONS_PER_WALLET)
    )

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
        f"fallback={args.fallback_out_dir or 'none'} "
        f"refresh_since_ms={refresh_since_ms}",
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
            refresh_since_ms=refresh_since_ms,
            test_memory_file_name=(
                WORKER_TEST_MEMORY_FILE_NAME if args.worker else TEST_MEMORY_FILE_NAME
            ),
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
