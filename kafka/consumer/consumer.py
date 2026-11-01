#!/usr/bin/env python3
"""
Step 12 - Real-time sales monitor.
Consumes 'sales-transactions' and:
  1. maintains live KPIs (revenue, category, store, top products)
  2. tracks stock per store/product and publishes alerts to 'restock-alerts'
     when an item falls below its reorder level
  3. lands micro-batches as CSV in data/streaming/sale_date=YYYY-MM-DD/
     so Hive can query real-time data (table: retail_db.sales_stream)
"""
import argparse
import csv
import json
import os
import signal
import time
from collections import Counter, deque
from datetime import datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parents[2]))
INV_FILE = PROJECT_DIR / "data" / "inventory" / "inventory.csv"
STREAM_DIR = PROJECT_DIR / "data" / "streaming"
COLUMNS = ["transaction_id", "transaction_date", "transaction_time", "store_id",
           "store_city", "product_id", "product_name", "category", "quantity",
           "unit_price", "cost_price", "discount_pct", "total_amount", "profit",
           "payment_mode"]
running = True


def stop(*_):
    global running
    running = False


class InventoryTracker:
    """In-memory stock levels; emits one alert when an item crosses its reorder level."""

    def __init__(self, path):
        self.items = {}
        with path.open() as f:
            for r in csv.DictReader(f):
                stock, reorder = int(r["current_stock"]), int(r["reorder_level"])
                self.items[(r["store_id"], r["product_id"])] = {
                    "product_name": r["product_name"], "stock": stock,
                    "reorder": reorder, "max": int(r["max_stock"]),
                    "alerted": stock < reorder,   # already flagged by batch Step 8
                }

    def apply(self, t):
        item = self.items.get((t["store_id"], t["product_id"]))
        if item is None:
            return None
        item["stock"] = max(0, item["stock"] - int(t["quantity"]))
        if item["stock"] < item["reorder"] and not item["alerted"]:
            item["alerted"] = True
            return {
                "alert_ts": datetime.now().isoformat(timespec="seconds"),
                "store_id": t["store_id"], "store_city": t["store_city"],
                "product_id": t["product_id"], "product_name": item["product_name"],
                "current_stock": item["stock"], "reorder_level": item["reorder"],
                "status": "OUT_OF_STOCK" if item["stock"] == 0 else "LOW",
                "suggested_order_qty": item["max"] - item["stock"],
            }
        return None


class MicroBatchWriter:
    """Buffers transactions and writes them as CSV files partitioned by date."""

    def __init__(self, out_dir, batch_size, flush_secs):
        self.out_dir, self.batch_size, self.flush_secs = out_dir, batch_size, flush_secs
        self.buffer, self.last_flush = [], time.time()
        self.files = self.rows = 0

    def add(self, t):
        self.buffer.append(t)

    def due(self):
        return len(self.buffer) >= self.batch_size or (
            self.buffer and time.time() - self.last_flush >= self.flush_secs)

    def flush(self):
        if self.buffer:
            by_date = {}
            for t in self.buffer:
                by_date.setdefault(t["transaction_date"], []).append(t)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            for day, rows in by_date.items():
                part = self.out_dir / f"sale_date={day}"
                part.mkdir(parents=True, exist_ok=True)
                tmp = part / f".batch_{stamp}.csv.tmp"      # hidden from Hive while writing
                with tmp.open("w", newline="") as f:
                    w = csv.writer(f)
                    for t in rows:
                        w.writerow([t[c] for c in COLUMNS])
                tmp.rename(part / f"batch_{stamp}.csv")   # atomic publish
                self.files += 1
            self.rows += len(self.buffer)
            self.buffer.clear()
        self.last_flush = time.time()


class LiveStats:
    def __init__(self):
        self.events = self.units = self.bad = self.alert_count = 0
        self.revenue = self.profit = 0.0
        self.by_category, self.by_store, self.product_units = Counter(), Counter(), Counter()
        self.recent = deque()
        self.last_alert = None

    def update(self, t):
        self.events += 1
        self.units += int(t["quantity"])
        self.revenue += float(t["total_amount"])
        self.profit += float(t["profit"])
        self.by_category[t["category"]] += float(t["total_amount"])
        self.by_store[t["store_city"]] += float(t["total_amount"])
        self.product_units[t["product_name"]] += int(t["quantity"])
        now = time.time()
        self.recent.append(now)
        while self.recent and now - self.recent[0] > 10:
            self.recent.popleft()

    def report(self, writer):
        fmt = lambda items: " | ".join(f"{k} Rs {v:,.0f}" for k, v in items)
        top = " | ".join(f"{k} ({v})" for k, v in self.product_units.most_common(3))
        last = (f"{self.last_alert['store_city']} - {self.last_alert['product_name']} "
                f"stock={self.last_alert['current_stock']}") if self.last_alert else "-"
        print("\n".join([
            "",
            f"================ LIVE SALES MONITOR  {datetime.now():%H:%M:%S} ================",
            f"Transactions : {self.events:,}   Units: {self.units:,}   "
            f"Rate: {len(self.recent) / 10:.1f} tx/s   Bad msgs: {self.bad}",
            f"Revenue      : Rs {self.revenue:,.2f}   Profit: Rs {self.profit:,.2f}",
            f"By category  : {fmt(self.by_category.most_common())}",
            f"By store     : {fmt(self.by_store.most_common())}",
            f"Top products : {top}",
            f"Restock alerts: {self.alert_count}   latest: {last}",
            f"Landed for Hive: {writer.files} files, {writer.rows:,} rows -> data/streaming/",
        ]), flush=True)


def main():
    ap = argparse.ArgumentParser(description="Real-time sales monitor")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default=os.environ.get("SALES_TOPIC", "sales-transactions"))
    ap.add_argument("--alert-topic", default=os.environ.get("ALERT_TOPIC", "restock-alerts"))
    ap.add_argument("--group", default=os.environ.get("CONSUMER_GROUP", "retail-monitor"))
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--flush-secs", type=float, default=10)
    ap.add_argument("--report-secs", type=float, default=10)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": args.group,
                         "auto.offset.reset": "earliest", "enable.auto.commit": True})
    consumer.subscribe([args.topic])
    alerts_out = Producer({"bootstrap.servers": args.bootstrap, "client.id": "restock-alerter"})

    inventory = InventoryTracker(INV_FILE)
    writer = MicroBatchWriter(STREAM_DIR, args.batch_size, args.flush_secs)
    stats = LiveStats()
    last_report = time.time()
    print(f"[consumer] <- {args.bootstrap}  topic={args.topic}  group={args.group}", flush=True)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[consumer] error: {msg.error()}", flush=True)
            elif msg is not None:
                try:
                    txn = json.loads(msg.value())
                except (ValueError, TypeError):
                    stats.bad += 1
                    txn = None
                if txn:
                    stats.update(txn)
                    writer.add(txn)
                    alert = inventory.apply(txn)
                    if alert:
                        stats.alert_count += 1
                        stats.last_alert = alert
                        alerts_out.produce(args.alert_topic, key=alert["store_id"],
                                           value=json.dumps(alert))
                        print(f"[ALERT] {alert['status']}: {alert['store_city']} - "
                              f"{alert['product_name']} stock={alert['current_stock']} "
                              f"(reorder at {alert['reorder_level']}) -> order "
                              f"{alert['suggested_order_qty']} units", flush=True)
            alerts_out.poll(0)
            if writer.due():
                writer.flush()
            if time.time() - last_report >= args.report_secs:
                stats.report(writer)
                last_report = time.time()
    finally:
        writer.flush()
        alerts_out.flush(5)
        consumer.close()
        stats.report(writer)
        print("[consumer] stopped.", flush=True)


if __name__ == "__main__":
    main()
