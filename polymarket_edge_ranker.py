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
import csv
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from typing import Any


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
#   closed_positions_pages.jsonl حافظه صفحه‌ای؛ اگر وسط یک والت بزرگ قطع شد از ادامه می‌رود
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
USE_SECONDARY_OFFLINE_POSITION_BACKUPS = True
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
MAX_POSITIONS_PER_WALLET = 200

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

# فیلتر حداقل Recovery Factor؛ اگر روشن باشد والت‌هایی که کمتر از مقدار زیر باشند حذف می‌شوند.
FILTER_MIN_RECOVERY_FACTOR = False

# حداقل Recovery Factor قابل قبول وقتی فیلتر بالا روشن باشد.
MIN_RECOVERY_FACTOR = 5

# فیلتر فعالیت ۷ روز اخیر؛ اگر روشن باشد والت بدون معامله باز/بسته‌شده در ۷ روز اخیر حذف می‌شود.
FILTER_NO_RECENT_7D_OPEN_OR_CLOSE = True

# تعداد روز برای فیلتر فعالیت اخیر.
RECENT_ACTIVITY_DAYS = 25

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


def open_csv_append(path: Path, fieldnames: list[str]):
    exists = path.exists() and path.stat().st_size > 0
    file = path.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    if not exists:
        writer.writeheader()
        file.flush()
    return file, writer


class PolymarketClient:
    def __init__(self, delay: float = 0.12, timeout: float = 30.0, retries: int = 3):
        self.delay = delay
        self.timeout = timeout
        self.retries = retries

    def get_json(self, path: str, params: dict[str, Any]) -> Any:
        query = urllib.parse.urlencode(params, doseq=True)
        url = f"{BASE_URL}{path}?{query}"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Origin": "https://polymarket.com",
            "Referer": "https://polymarket.com/",
        }

        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            if self.delay:
                time.sleep(self.delay)
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    body = response.read().decode("utf-8")
                    return json.loads(body)
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code in (429, 500, 502, 503, 504):
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                time.sleep(1.5 * (attempt + 1))

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
    with path.open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            wallet = str(row["proxyWallet"]).lower()
            wallets[wallet] = WalletSeed(
                proxy_wallet=wallet,
                user_name=row.get("userName", ""),
                x_username=row.get("xUsername", ""),
                verified_badge=str(row.get("verifiedBadge", "")).lower() == "true",
                best_pnl=safe_float(row.get("bestPnl")),
                best_vol=safe_float(row.get("bestVol")),
                profile_views=int(safe_float(row.get("profileViews"))),
                leaderboard_hits=int(safe_float(row.get("leaderboardHits"))),
                best_rank_seen=int(safe_float(row.get("bestRankSeen"), 10**9)),
                modes=set(str(row.get("modes", "")).split("|")) if row.get("modes") else set(),
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


def fetch_closed_positions(
    client: PolymarketClient,
    wallet: str,
    limit: int = 50,
    max_positions: int = 100000,
    progress_every: int = 500,
    page_cache: dict[str, dict[int, list[dict[str, Any]]]] | None = None,
    page_cache_file=None,
) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    wallet_pages = page_cache.setdefault(wallet, {}) if page_cache is not None else {}
    for offset in range(0, max_positions, limit):
        if offset == 0 or offset % progress_every == 0:
            print(f"    [positions] {wallet} offset={offset} collected={len(positions)}", flush=True)
        if offset in wallet_pages:
            rows = wallet_pages[offset]
        else:
            rows = client.get_json(
                "/closed-positions",
                {
                    "user": wallet,
                    "limit": limit,
                    "offset": offset,
                    "sortBy": "TIMESTAMP",
                    "sortDirection": "DESC",
                },
            )
            wallet_pages[offset] = rows
            if page_cache_file is not None:
                page_cache_file.write(
                    json.dumps(
                        {
                            "proxyWallet": wallet,
                            "offset": offset,
                            "rows": rows,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                page_cache_file.flush()
        if not rows:
            break
        positions.extend(rows)
        if len(rows) < limit:
            break
    print(f"    [positions:done] {wallet} total={len(positions)}", flush=True)
    return positions


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


def position_market_key(pos: dict[str, Any]) -> str:
    """Return the most specific available identifier for a position's market."""
    for key in (
        "conditionId",
        "conditionID",
        "marketId",
        "marketID",
        "questionId",
        "questionID",
        "marketSlug",
        "slug",
        "title",
    ):
        value = str(pos.get(key) or "").strip().lower()
        if value:
            return f"{key.lower()}:{value}"
    return ""


def position_outcome_key(pos: dict[str, Any]) -> str:
    """Return a normalized outcome label used for repeat-entry and hedge detection."""
    for key in ("outcome", "outcomeName", "side", "outcomeIndex"):
        value = str(pos.get(key) if pos.get(key) is not None else "").strip().lower()
        if value:
            return value
    return ""


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
    market_outcome_position_counts: dict[tuple[str, str], int] = {}
    market_outcomes: dict[str, set[str]] = {}
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
        market_key = position_market_key(pos)
        outcome_key = position_outcome_key(pos)
        if market_key and outcome_key:
            market_outcome_key = (market_key, outcome_key)
            market_outcome_position_counts[market_outcome_key] = (
                market_outcome_position_counts.get(market_outcome_key, 0) + 1
            )
            market_outcomes.setdefault(market_key, set()).add(outcome_key)
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
    same_outcome_volume_additions = sum(
        max(position_count - 1, 0)
        for position_count in market_outcome_position_counts.values()
    )
    hedged_market_count = sum(
        1
        for outcomes in market_outcomes.values()
        if "yes" in outcomes and "no" in outcomes
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
        "sameOutcomeVolumeAdditions": same_outcome_volume_additions,
        "hedgedMarketCount": hedged_market_count,
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
) -> None:
    raw_path = out_dir / RAW_CLOSED_POSITIONS_LOG_FILE_NAME
    secondary_raw_path = out_dir / SECONDARY_RAW_CLOSED_POSITIONS_LOG_FILE_NAME
    fail_path = out_dir / "closed_positions_failed.csv"
    score_path = out_dir / "edge_scores.xlsx"
    not_saved_reasons_path = out_dir / NOT_SAVED_REASONS_FILE_NAME
    not_saved_reason_stats_path = out_dir / NOT_SAVED_REASON_STATS_FILE_NAME
    progress_path = out_dir / "edge_scores_progress.csv"
    memory_path = out_dir / TEST_MEMORY_FILE_NAME
    page_cache_path = out_dir / CLOSED_POSITION_PAGE_CACHE_FILE_NAME
    secondary_page_cache_path = out_dir / SECONDARY_CLOSED_POSITION_PAGE_CACHE_FILE_NAME
    universe_path = out_dir / "wallet_universe.csv"

    ranked_wallets = (
        list(wallets.values())
        if preserve_wallet_order
        else sorted(wallets.values(), key=lambda item: item.best_pnl, reverse=True)
    )
    if max_wallets:
        ranked_wallets = ranked_wallets[:max_wallets]

    score_fieldnames = get_score_fieldnames()
    score_by_wallet = {
        str(row.get("proxyWallet") or "").lower(): row
        for row in load_progress_scores(progress_path)
        if row.get("proxyWallet")
    }
    not_saved_reasons_by_wallet: dict[str, dict[str, Any]] = {}
    not_saved_reason_stats: dict[str, dict[str, Any]] = {}
    filtered_wallets_to_purge: set[str] = set()
    tested_wallets = load_test_memory(memory_path)
    if tested_wallets:
        print(f"[resume] loaded test memory for {len(tested_wallets)} wallets", flush=True)

    cached_positions = load_closed_positions_cache(raw_path)
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
    if page_cache:
        print(f"[resume] loaded page cache for {len(page_cache)} wallets", flush=True)
    if USE_SECONDARY_OFFLINE_POSITION_BACKUPS:
        secondary_page_cache = load_closed_position_page_cache(secondary_page_cache_path)
        added_pages = merge_missing_closed_position_page_cache(page_cache, secondary_page_cache)
        if secondary_page_cache:
            print(
                f"[resume] loaded secondary page cache for {len(secondary_page_cache)} wallets; "
                f"added {added_pages} missing pages",
                flush=True,
            )

    fail_file, fail_writer = open_csv_append(fail_path, ["proxyWallet", "error"])
    memory_file, memory_writer = open_csv_append(
        memory_path,
        ["proxyWallet", "userName", "status", "reason", "testedAt"],
    )
    with raw_path.open("a", encoding="utf-8") as raw_file, fail_file, memory_file, page_cache_path.open(
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
                    positions = fetch_closed_positions(
                        client,
                        seed.proxy_wallet,
                        max_positions=max_positions_per_wallet,
                        page_cache=page_cache,
                        page_cache_file=page_cache_file,
                    )
                    raw_file.write(
                        json.dumps(
                            {"proxyWallet": seed.proxy_wallet, "positions": positions},
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    raw_file.flush()
                    cached_positions[seed.proxy_wallet] = positions
                except Exception as exc:
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
                    continue

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
    write_all_score_outputs(score_by_wallet.values(), score_path, out_dir, score_fieldnames)
    if PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS:
        rewrite_closed_positions_cache(raw_path, cached_positions)
        rewrite_closed_position_page_cache(page_cache_path, page_cache)
    if PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE and filtered_wallets_to_purge:
        rewrite_wallet_universe_without_wallets(universe_path, filtered_wallets_to_purge)


def mode_2_filter_reason(score: dict[str, Any]) -> str:
    if FILTER_ALL_RECENT_BALANCES_NEGATIVE and score["allRecentBalancesNegative"]:
        return "all recent balances are negative"
    if FILTER_NEGATIVE_NET_EDGE and score["netEdge"] < 0:
        return f"netEdge {score['netEdge']} < 0"
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
            if wallet and isinstance(positions, list):
                cached[wallet] = positions
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
            if wallet and offset >= 0 and isinstance(rows, list):
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
            file.write(json.dumps({"proxyWallet": wallet, "positions": positions}, ensure_ascii=False) + "\n")


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
    with universe_path.open("r", newline="", encoding="utf-8") as file:
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
    filtered_wallets_to_purge.add(wallet)
    if PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS:
        cached_positions.pop(wallet, None)
        page_cache.pop(wallet, None)
        rewrite_closed_positions_cache(raw_path, cached_positions)
        rewrite_closed_position_page_cache(page_cache_path, page_cache)
    if PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE:
        rewrite_wallet_universe_without_wallets(universe_path, filtered_wallets_to_purge)


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
        "sameOutcomeVolumeAdditions",
        "hedgedMarketCount",
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
        "sameOutcomeVolumeAdditions": score.get("sameOutcomeVolumeAdditions", ""),
        "hedgedMarketCount": score.get("hedgedMarketCount", ""),
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
        ("sameOutcomeVolumeAdditions", True),
        ("hedgedMarketCount", True),
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
        "sameOutcomeVolumeAdditions",
        "hedgedMarketCount",
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
            f"min_recovery_factor={FILTER_MIN_RECOVERY_FACTOR}:{MIN_RECOVERY_FACTOR} "
            f"recent_activity_days={FILTER_NO_RECENT_7D_OPEN_OR_CLOSE}:{RECENT_ACTIVITY_DAYS} "
            f"short_hold={FILTER_SHORT_HOLD_RATIO}:{MAX_SHORT_HOLD_RATIO}/{SHORT_HOLD_MAX_HOURS}h "
            f"purge_position_jsonl={PURGE_FILTERED_WALLETS_FROM_POSITION_BACKUPS} "
            f"purge_wallet_universe={PURGE_FILTERED_WALLETS_FROM_WALLET_UNIVERSE} "
            f"live_all_result_files={UPDATE_ALL_RESULT_FILES_AFTER_EACH_WALLET} "
            f"live_progress_csv={UPDATE_PROGRESS_CSV_AFTER_EACH_WALLET}"
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
        print("[live output] edge_scores_progress.csv updates during scoring")
        print("[final output] edge_scores.xlsx and edge_scores_by_<factor>.xlsx are sorted after scoring finishes")
    print("")


def main() -> int:
    args = build_parser().parse_args()
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
    client = PolymarketClient(delay=delay, timeout=timeout, retries=retries)

    universe_path = out_dir / "wallet_universe.csv"
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
    )
    print(f"[done] results: {out_dir / 'edge_scores.xlsx'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
