#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
pokercraft.py — агрегатор турнирной статистики PokerOK.
Принимает CSV-файлы, парсит и агрегирует результаты турниров.
"""

import argparse
import os
import sys
import csv
import json
import datetime as dt
from dataclasses import dataclass
from typing import List, Optional, Dict


# ============================================================
# ---------------------- DATA MODEL ---------------------------
# ============================================================

@dataclass
class TournamentRecord:
    date: dt.datetime
    tournament: str
    buyin_total: float
    rake: float
    prize: float
    profit: float
    placement: Optional[int]
    field_size: Optional[int]
    currency: str
    t_type: str


# ============================================================
# ------------------------ ARGPARSE ---------------------------
# ============================================================

def parse_args():
    p = argparse.ArgumentParser(
        description="PokerCraft tournament aggregator (PokerOK / GG)"
    )

    p.add_argument("--mode", default="summaries", choices=["summaries", "hh"],
                   help="Режим обработки данных")

    p.add_argument("--input", required=True,
                   help="Путь к файлу или директории с CSV/summary")

    p.add_argument("--output", default="./output",
                   help="Папка для сохранения результатов")

    p.add_argument("--currency", default=None,
                   help="Фильтр по валюте (например, USD)")

    p.add_argument("--min-date", default=None,
                   help="Минимальная дата (YYYY-MM-DD)")

    p.add_argument("--max-date", default=None,
                   help="Максимальная дата (YYYY-MM-DD)")

    p.add_argument("--delimiter", default=",",
                   help="Разделитель CSV (по умолчанию: запятая)")

    p.add_argument("--encoding", default="utf-8",
                   help="Кодировка входных файлов")

    p.add_argument("--verbose", action="store_true",
                   help="Подробный вывод логов")

    p.add_argument("--no-console-table", action="store_true",
                   help="Отключить подробные таблицы в консоли")

    return p.parse_args()


# ============================================================
# --------------------- FILE DISCOVERY ------------------------
# ============================================================

def discover_files(path: str, mode: str) -> List[str]:
    """Возвращает список файлов по пути (одно имя или директория)."""
    if not os.path.exists(path):
        print(f"[ERROR] Путь не найден: {path}")
        sys.exit(1)

    if os.path.isfile(path):
        return [path]

    ext = ".csv" if mode == "summaries" else ".txt"

    result = []
    for root, _, files in os.walk(path):
        for f in files:
            if f.lower().endswith(ext):
                result.append(os.path.join(root, f))

    return result


# ============================================================
# ---------------------- CSV PARSING --------------------------
# ============================================================

def parse_date(value: str) -> Optional[dt.datetime]:
    """Пытается привести строку к datetime."""
    if not value:
        return None

    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return dt.datetime.strptime(value.strip(), fmt)
        except:
            pass
    return None


def to_float(value: str) -> float:
    if value is None or value == "":
        return 0.0
    value = value.replace(",", ".")
    try:
        return float(value)
    except:
        return 0.0


def to_int(value: str) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except:
        return None


def load_summaries(files: List[str], delimiter: str, encoding: str, verbose: bool) -> List[TournamentRecord]:
    """Читает CSV-файлы PokerCraft и создаёт список TournamentRecord."""
    records = []

    required_fields = ["Date", "Tournament", "BuyIn", "Result"]

    for file in files:
        if verbose:
            print(f"[INFO] Чтение файла: {file}")

        try:
            with open(file, "r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f, delimiter=delimiter)

                missing = [c for c in required_fields if c not in reader.fieldnames]
                if missing:
                    print(f"[WARNING] Файл пропущен, не хватает колонок {missing}: {file}")
                    continue

                for row in reader:
                    d = parse_date(row.get("Date"))

                    buyin = to_float(row.get("BuyIn"))
                    prize = to_float(row.get("Result"))
                    rake = to_float(row.get("Rake")) if "Rake" in row else 0.0
                    placement = to_int(row.get("Placement"))
                    field_size = to_int(row.get("FieldSize"))
                    currency = row.get("Currency", "").strip() or "UNKNOWN"
                    t_type = row.get("Type", "").strip() or "Unknown"

                    profit = prize - buyin

                    if d is None:
                        if verbose:
                            print(f"[WARNING] Некорректная дата, пропуск строки: {row}")
                        continue

                    record = TournamentRecord(
                        date=d,
                        tournament=row.get("Tournament", "Unknown"),
                        buyin_total=buyin,
                        rake=rake,
                        prize=prize,
                        profit=profit,
                        placement=placement,
                        field_size=field_size,
                        currency=currency,
                        t_type=t_type,
                    )

                    records.append(record)

        except Exception as e:
            print(f"[ERROR] Ошибка чтения '{file}': {e}")

    return records


# ============================================================
# ------------- FILTERING & AGGREGATION -----------------------
# ============================================================

def filter_records(records: List[TournamentRecord],
                   currency: Optional[str],
                   min_date: Optional[str],
                   max_date: Optional[str]) -> List[TournamentRecord]:

    result = []
    d_min = dt.datetime.strptime(min_date, "%Y-%m-%d") if min_date else None
    d_max = dt.datetime.strptime(max_date, "%Y-%m-%d") if max_date else None

    for r in records:
        if currency and r.currency != currency:
            continue
        if d_min and r.date < d_min:
            continue
        if d_max and r.date > d_max:
            continue
        result.append(r)

    return result


def aggregate_overall(records: List[TournamentRecord]) -> Dict:
    total_buyin = sum(r.buyin_total for r in records)
    total_prize = sum(r.prize for r in records)
    profit = total_prize - total_buyin

    itm_count = sum(1 for r in records if r.prize > 0)
    total = len(records)

    roi = (profit / total_buyin * 100) if total_buyin > 0 else 0
    itm = (itm_count / total * 100) if total > 0 else 0
    abi = (total_buyin / total) if total > 0 else 0

    return {
        "total_tournaments": total,
        "total_buyin": total_buyin,
        "total_prize": total_prize,
        "total_profit": profit,
        "roi_percent": roi,
        "itm_percent": itm,
        "abi": abi,
    }


def get_limit_group(buyin: float) -> str:
    """Группировка по лимитам."""
    if buyin < 5:
        return "Micro (0–5)"
    elif buyin < 22:
        return "Low (5–22)"
    elif buyin < 109:
        return "Mid (22–109)"
    return "High (109+)"


def aggregate_by_limits(records: List[TournamentRecord]) -> Dict[str, Dict]:
    groups = {}

    for r in records:
        g = get_limit_group(r.buyin_total)
        if g not in groups:
            groups[g] = {"count": 0, "buyin": 0, "prize": 0, "profit": 0}
        groups[g]["count"] += 1
        groups[g]["buyin"] += r.buyin_total
        groups[g]["prize"] += r.prize
        groups[g]["profit"] += r.profit

    for g, v in groups.items():
        v["roi_percent"] = (v["profit"] / v["buyin"] * 100) if v["buyin"] > 0 else 0
        v["abi"] = (v["buyin"] / v["count"]) if v["count"] > 0 else 0

    return groups


def aggregate_by_type(records: List[TournamentRecord]) -> Dict[str, Dict]:
    groups = {}

    for r in records:
        t = r.t_type
        if t not in groups:
            groups[t] = {"count": 0, "buyin": 0, "prize": 0, "profit": 0}
        groups[t]["count"] += 1
        groups[t]["buyin"] += r.buyin_total
        groups[t]["prize"] += r.prize
        groups[t]["profit"] += r.profit

    for t, v in groups.items():
        v["roi_percent"] = (v["profit"] / v["buyin"] * 100) if v["buyin"] > 0 else 0
        v["abi"] = (v["buyin"] / v["count"]) if v["count"] else 0

    return groups


# ============================================================
# ----------------------- PRINTING ----------------------------
# ============================================================

def print_overall_summary(summary: Dict):
    print("\n===== OVERALL SUMMARY =====")
    print(f"Tournaments:       {summary['total_tournaments']}")
    print(f"Total Buy-In:      {summary['total_buyin']:.2f}")
    print(f"Total Prize:       {summary['total_prize']:.2f}")
    print(f"Profit:            {summary['total_profit']:.2f}")
    print(f"ROI (%):           {summary['roi_percent']:.2f}")
    print(f"ITM (%):           {summary['itm_percent']:.2f}")
    print(f"ABI:               {summary['abi']:.2f}")


def print_group_table(title: str, data: Dict[str, Dict]):
    print(f"\n===== {title} =====")
    print(f"{'Group':25} {'Count':>5} {'BuyIn':>10} {'Prize':>10} {'Profit':>10} {'ROI%':>8} {'ABI':>8}")
    print("-" * 80)

    for g, v in data.items():
        print(
            f"{g:25} {v['count']:>5} "
            f"{v['buyin']:>10.2f} {v['prize']:>10.2f} {v['profit']:>10.2f} "
            f"{v['roi_percent']:>8.2f} {v['abi']:>8.2f}"
        )


# ============================================================
# ------------------------ SAVING -----------------------------
# ============================================================

def save_aggregated_csv(records: List[TournamentRecord], out_dir: str):
    path = os.path.join(out_dir, "tournaments_aggregated.csv")
    os.makedirs(out_dir, exist_ok=True)

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date", "tournament", "buyin_total", "rake", "prize", "profit",
            "placement", "field_size", "currency", "type", "limit_group"
        ])

        for r in records:
            writer.writerow([
                r.date.isoformat(sep=" "),
                r.tournament,
                r.buyin_total,
                r.rake,
                r.prize,
                r.profit,
                r.placement,
                r.field_size,
                r.currency,
                r.t_type,
                get_limit_group(r.buyin_total),
            ])

    print(f"[OK] CSV сохранён: {path}")


def save_overall_json(summary: Dict, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "summary_overall.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4)

    print(f"[OK] JSON сохранён: {path}")


# ============================================================
# -------------------------- MAIN -----------------------------
# ============================================================

def main():
    args = parse_args()

    print("[INFO] Поиск файлов...")
    files = discover_files(args.input, args.mode)

    if len(files) == 0:
        print("[ERROR] Файлы не найдены.")
        sys.exit(1)

    # Load
    print(f"[INFO] Найдено файлов: {len(files)}")
    records = load_summaries(files, args.delimiter, args.encoding, args.verbose)

    if len(records) == 0:
        print("[ERROR] Не удалось загрузить ни одной записи.")
        sys.exit(1)

    # Filter
    records = filter_records(records, args.currency, args.min_date, args.max_date)

    if len(records) == 0:
        print("[ERROR] Все записи были отфильтрованы. Нет данных для анализа.")
        sys.exit(1)

    # Aggregate
    summary = aggregate_overall(records)
    limits = aggregate_by_limits(records)
    types = aggregate_by_type(records)

    # Print
    print_overall_summary(summary)

    if not args.no_console_table:
        print_group_table("BY LIMITS", limits)
        print_group_table("BY TOURNAMENT TYPE", types)

    # Save
    save_aggregated_csv(records, args.output)
    save_overall_json(summary, args.output)


if __name__ == "__main__":
    main()