#!/usr/bin/env python3
"""
Step 12 - Real-time POS simulator.
Publishes sales transactions as JSON to the Kafka topic 'sales-transactions',
keyed by store_id. Product and store popularity mirror the historical data.
"""
import argparse
import csv
import json
import os
import signal
import time
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path

import numpy as np
from confluent_kafka import Producer

PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parents[2]))
HIST_FILE = PROJECT_DIR / "data" / "historical" / "sales_history.csv"
QTY_LAMBDA = {"Groceries": 2.0, "Household": 1.0, "Clothing": 0.6, "Electronics": 0.15}
running = True


def stop(*_):
    global running
    running = False


def load_catalog():
    """Build product/store catalog and popularity weights from history."""
    products, stores = {}, {}
    p_count, s_count = Counter(), Counter()
    with HIST_FILE.open() as f:
        for r in csv.DictReader(f):
            pid, sid = r["product_id"], r["store_id"]
            if pid not in products:
                products[pid] = {
                    "product_id": pid, "product_name": r["product_name"],
                    "category": r["category"],
                    "unit_price": float(r["unit_price"]),
                    "cost_price": float(r["cost_price"]),
                }
            stores[sid] = r["store_city"]
            p_count[pid] += 1
            s_count[sid] += 1
    pids, sids = sorted(products), sorted(stores)
    pw = np.array([p_count[p] for p in pids], float)
    sw = np.array([s_count[s] for s in sids], float)
    return products, stores, pids, pw / pw.sum(), sids, sw / sw.sum()


def make_transaction(rng, catalog):
    products, stores, pids, pw, sids, sw = catalog
    p = products[pids[rng.choice(len(pids), p=pw)]]
    sid = sids[rng.choice(len(sids), p=sw)]
    qty = int(1 + rng.poisson(QTY_LAMBDA[p["category"]]))
    disc = int(rng.choice([0, 5, 10, 15, 20], p=[0.55, 0.15, 0.15, 0.10, 0.05]))
    total = round(qty * p["unit_price"] * (1 - disc / 100), 2)
    now = datetime.now()
    return {
        "transaction_id": f"RT{uuid.uuid4().hex[:10].upper()}",
        "transaction_date": now.strftime("%Y-%m-%d"),
        "transaction_time": now.strftime("%H:%M:%S"),
        "store_id": sid,
        "store_city": stores[sid],
        "product_id": p["product_id"],
        "product_name": p["product_name"],
        "category": p["category"],
        "quantity": qty,
        "unit_price": p["unit_price"],
        "cost_price": p["cost_price"],
        "discount_pct": disc,
        "total_amount": total,
        "profit": round(total - qty * p["cost_price"], 2),
        "payment_mode": str(rng.choice(["UPI", "Card", "Cash", "Wallet"],
                                       p=[0.45, 0.30, 0.20, 0.05])),
        "event_ts": now.isoformat(timespec="milliseconds"),
    }


def main():
    ap = argparse.ArgumentParser(description="Real-time POS sales producer")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default=os.environ.get("SALES_TOPIC", "sales-transactions"))
    ap.add_argument("--rate", type=float, default=float(os.environ.get("TX_RATE", 5)),
                    help="average transactions per second")
    ap.add_argument("--count", type=int, default=int(os.environ.get("TX_COUNT", 0)),
                    help="number of transactions to send (0 = run forever)")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    rng = np.random.default_rng()
    catalog = load_catalog()
    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": "pos-simulator",
                         "acks": "all", "linger.ms": 20})
    stats = {"sent": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err:
            stats["failed"] += 1
            print(f"[producer] delivery failed: {err}", flush=True)

    print(f"[producer] -> {args.bootstrap}  topic={args.topic}  rate~{args.rate}/s  "
          f"count={'infinite' if args.count == 0 else args.count}", flush=True)

    while running and (args.count == 0 or stats["sent"] < args.count):
        txn = make_transaction(rng, catalog)
        producer.produce(args.topic, key=txn["store_id"], value=json.dumps(txn),
                         on_delivery=on_delivery)
        producer.poll(0)
        stats["sent"] += 1
        if stats["sent"] % 100 == 0:
            print(f"[producer] sent {stats['sent']:,} | last: {txn['store_city']} - "
                  f"{txn['product_name']} x{txn['quantity']} = Rs {txn['total_amount']:,.2f}",
                  flush=True)
        time.sleep(rng.exponential(1 / args.rate))   # Poisson arrivals

    producer.flush(10)
    print(f"[producer] stopped. sent={stats['sent']:,} failed={stats['failed']}", flush=True)


if __name__ == "__main__":
    main()
