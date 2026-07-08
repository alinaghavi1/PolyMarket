#!/usr/bin/env python3
"""
Build a broad Polymarket wallet universe from every official leaderboard mode,
then rank wallets with the Net Edge score:

where:
    win_edge  = 1 - avgPrice  for profitable closed positions
    loss_risk = avgPrice      for losing closed positions
    Net Edge  = sum(win_edge) - sum(loss_risk)

This is a statistical ranking tool, not proof of insider trading.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
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
#   wallet_test_memory.csv     حافظه تست؛ اگر پاکش کنی تست از اول شروع می‌شود
#   closed_positions_raw.jsonl دیتای خام کامل هر والت؛ برای فرمول‌های بعدی نگهش دار
#   closed_positions_pages.jsonl حافظه صفحه‌ای؛ اگر وسط یک والت بزرگ قطع شد از ادامه می‌رود
#   edge_scores_progress.csv   خروجی زنده؛ بعد از هر والت مرتب و آپدیت می‌شود
#   edge_scores.csv            خروجی نهایی مرتب‌شده بعد از پایان کار
# =============================================================================

# آدرس API عمومی پلی‌مارکت. معمولاً لازم نیست تغییرش بدهی.
BASE_URL = "https://data-api.polymarket.com"

# مود اجرا:
#   1 = استخراج والت‌ها از همه حالت‌های لیدربورد
#   2 = تست کردن والت‌های استخراج‌شده و محاسبه Net Edge Score
RUN_MODE = 2

# پوشه خروجی. برای اینکه مود 2 بتواند خروجی مود 1 را بخواند، بین دو مود تغییرش نده.
OUT_DIR = "polymarket_edge_output"

# اسم فایل حافظه مود 2.
# هر والت بعد از تست شدن داخل این فایل ثبت می‌شود.
# اگر برنامه را ببندی و دوباره اجرا کنی، والت‌های ثبت‌شده را رد می‌کند و ادامه می‌دهد.
# اگر می‌خواهی تست از اول شروع شود، فقط همین فایل را پاک کن.
TEST_MEMORY_FILE_NAME = "wallet_test_memory.csv"

# اسم فایل دیتای خام کامل.
# هر وقت یک والت کامل گرفته شد، همه پوزیشن‌های بسته‌شده‌اش اینجا ذخیره می‌شود.
# اگر بعداً فرمول را عوض کردی، این فایل را پاک نکن تا دوباره API نگیری.
RAW_CLOSED_POSITIONS_LOG_FILE_NAME = "closed_positions_raw.jsonl"

# اسم فایل cache صفحه‌ای پوزیشن‌ها.
# اگر وسط گرفتن یک والت بزرگ قطع شود، صفحه‌های گرفته‌شده داخل این فایل می‌ماند.
# اجرای بعدی همان والت را از offset بعدی ادامه می‌دهد، نه از اول.
CLOSED_POSITION_PAGE_CACHE_FILE_NAME = "closed_positions_pages.jsonl"

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
MAX_POSITIONS_PER_WALLET = 1000

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

# این مقدار فعلاً فقط برای سازگاری با نسخه‌های قبلی مانده است.
# در نسخه فعلی score اصلی فقط netEdge است و هیچ تقسیمی انجام نمی‌شود.
SMOOTHING = 0.01


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
    leaderboard_hits: int = 0
    best_rank_seen: int = 10**9
    modes: set[str] = field(default_factory=set)

    def update(self, row: dict[str, Any], mode: str) -> None:
        self.user_name = self.user_name or str(row.get("userName") or "")
        self.x_username = self.x_username or str(row.get("xUsername") or "")
        self.verified_badge = bool(self.verified_badge or row.get("verifiedBadge"))
        self.best_pnl = max(self.best_pnl, safe_float(row.get("pnl")))
        self.best_vol = max(self.best_vol, safe_float(row.get("vol")))
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
                leaderboard_hits=int(safe_float(row.get("leaderboardHits"))),
                best_rank_seen=int(safe_float(row.get("bestRankSeen"), 10**9)),
                modes=set(str(row.get("modes", "")).split("|")) if row.get("modes") else set(),
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


def score_positions(positions: list[dict[str, Any]], smoothing: float = 0.01) -> dict[str, Any]:
    wins = 0
    losses = 0
    breakeven = 0
    sum_win_edge = 0.0
    sum_loss_risk = 0.0
    sum_win_edge_sq = 0.0
    sum_loss_risk_sq = 0.0
    realized_pnl = 0.0
    total_bought = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    current_consecutive_wins = 0
    current_consecutive_losses = 0
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    pnl_series: list[float] = []
    equity = 0.0
    peak_equity = 0.0
    max_drawdown = 0.0

    for pos in positions:
        avg_price = min(max(safe_float(pos.get("avgPrice")), 0.0), 1.0)
        pnl = safe_float(pos.get("realizedPnl"))
        bought = safe_float(pos.get("totalBought"))
        realized_pnl += pnl
        total_bought += bought
        pnl_series.append(pnl)
        equity += pnl
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)

        if pnl > 0:
            wins += 1
            gross_profit += pnl
            current_consecutive_wins += 1
            current_consecutive_losses = 0
            max_consecutive_wins = max(max_consecutive_wins, current_consecutive_wins)
            edge = max(0.0, 1.0 - avg_price)
            sum_win_edge += edge
            sum_win_edge_sq += edge * edge
        elif pnl < 0:
            losses += 1
            gross_loss += abs(pnl)
            current_consecutive_losses += 1
            current_consecutive_wins = 0
            max_consecutive_losses = max(max_consecutive_losses, current_consecutive_losses)
            risk = max(0.0, avg_price)
            sum_loss_risk += risk
            sum_loss_risk_sq += risk * risk
        else:
            breakeven += 1
            current_consecutive_wins = 0
            current_consecutive_losses = 0

    resolved = wins + losses
    net_edge = sum_win_edge - sum_loss_risk
    net_edge_score = net_edge
    edge_rally_raw = 0.0
    edge_rally = net_edge_score
    win_rate = wins / resolved if resolved else 0.0
    adjusted_win_rate = wilson_lower_bound(wins, resolved)
    roi_closed = realized_pnl / total_bought if total_bought > 0 else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0.0
    recovery_factor = realized_pnl / max_drawdown if max_drawdown > 0 else 0.0
    expected_payoff = realized_pnl / resolved if resolved else 0.0
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
        "netEdge": net_edge,
        "netEdgeScore": net_edge_score,
        "edgeRallyRaw": edge_rally_raw,
        "edgeRally": edge_rally,
        "realizedPnlClosed": realized_pnl,
        "totalBoughtClosed": total_bought,
        "roiClosed": roi_closed,
        "maxDrawdown": max_drawdown,
        "profitFactor": profit_factor,
        "recoveryFactor": recovery_factor,
        "sharpeRatio": sharpe_ratio,
        "expectedPayoff": expected_payoff,
        "maxConsecutiveWins": max_consecutive_wins,
        "maxConsecutiveLosses": max_consecutive_losses,
        "grossProfit": gross_profit,
        "grossLoss": gross_loss,
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
) -> None:
    raw_path = out_dir / RAW_CLOSED_POSITIONS_LOG_FILE_NAME
    fail_path = out_dir / "closed_positions_failed.csv"
    score_path = out_dir / "edge_scores.csv"
    progress_path = out_dir / "edge_scores_progress.csv"
    memory_path = out_dir / TEST_MEMORY_FILE_NAME
    page_cache_path = out_dir / CLOSED_POSITION_PAGE_CACHE_FILE_NAME

    ranked_wallets = sorted(wallets.values(), key=lambda item: item.best_pnl, reverse=True)
    if max_wallets:
        ranked_wallets = ranked_wallets[:max_wallets]

    score_fieldnames = get_score_fieldnames()
    score_by_wallet = {
        str(row.get("proxyWallet") or "").lower(): row
        for row in load_progress_scores(progress_path)
        if row.get("proxyWallet")
    }
    tested_wallets = load_test_memory(memory_path)
    if tested_wallets:
        print(f"[resume] loaded test memory for {len(tested_wallets)} wallets", flush=True)

    cached_positions = load_closed_positions_cache(raw_path)
    if cached_positions:
        print(f"[resume] loaded closed-position cache for {len(cached_positions)} wallets", flush=True)
    page_cache = load_closed_position_page_cache(page_cache_path)
    if page_cache:
        print(f"[resume] loaded page cache for {len(page_cache)} wallets", flush=True)

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
                except Exception as exc:
                    fail_writer.writerow({"proxyWallet": seed.proxy_wallet, "error": repr(exc)})
                    fail_file.flush()
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
                continue
            if score["realizedPnlClosed"] < min_pnl:
                write_test_memory_row(
                    memory_writer,
                    memory_file,
                    seed,
                    status="filtered",
                    reason=f"realizedPnlClosed {score['realizedPnlClosed']} < {min_pnl}",
                )
                tested_wallets.add(seed.proxy_wallet)
                continue

            roi_proxy = seed.best_pnl / seed.best_vol if seed.best_vol > 0 else 0.0
            score_row = {
                "proxyWallet": seed.proxy_wallet,
                "userName": seed.user_name,
                "xUsername": seed.x_username,
                "verifiedBadge": seed.verified_badge,
                "bestPnlLeaderboard": seed.best_pnl,
                "bestVolLeaderboard": seed.best_vol,
                "roiProxyLeaderboard": roi_proxy,
                "leaderboardHits": seed.leaderboard_hits,
                "bestRankSeen": seed.best_rank_seen,
                "modes": "|".join(sorted(seed.modes)),
                **score,
            }
            score_by_wallet[seed.proxy_wallet] = score_row
            write_sorted_scores_csv(score_by_wallet.values(), progress_path, score_fieldnames)
            write_test_memory_row(
                memory_writer,
                memory_file,
                seed,
                status="scored",
                reason="ok",
            )
            tested_wallets.add(seed.proxy_wallet)

    write_sorted_scores_csv(score_by_wallet.values(), score_path, score_fieldnames)


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


def sorted_score_rows(rows: Any) -> list[dict[str, Any]]:
    score_rows = [dict(row) for row in rows if row and row.get("proxyWallet")]
    score_rows.sort(
        key=lambda row: (
            safe_float(row.get("netEdgeScore"), safe_float(row.get("netEdge"))),
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
        "sumWinEdge",
        "sumLossRisk",
        "sumWinEdgeSq",
        "sumLossRiskSq",
        "realizedPnlClosed",
        "totalBoughtClosed",
        "roiClosed",
        "maxDrawdown",
        "profitFactor",
        "recoveryFactor",
        "sharpeRatio",
        "expectedPayoff",
        "maxConsecutiveWins",
        "maxConsecutiveLosses",
        "grossProfit",
        "grossLoss",
        "edgeRally",
        "edgeRallyRaw",
        "bestPnlLeaderboard",
        "bestVolLeaderboard",
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
        print(f"[memory] {TEST_MEMORY_FILE_NAME} controls resume; delete it to restart scoring")
        print(f"[raw data] keep {RAW_CLOSED_POSITIONS_LOG_FILE_NAME} and {CLOSED_POSITION_PAGE_CACHE_FILE_NAME}")
        print("[live output] edge_scores_progress.csv updates during scoring")
        print("[final output] edge_scores.csv is sorted after scoring finishes")
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
    if mode == 2:
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
    )
    print(f"[done] results: {out_dir / 'edge_scores.csv'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
