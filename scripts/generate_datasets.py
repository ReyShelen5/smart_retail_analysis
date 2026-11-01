#!/usr/bin/env python3
"""
Smart Retail Analytics - synthetic dataset generator.

Step 2: data/historical/sales_history.csv  (sales transactions)
Step 3: data/inventory/inventory.csv       (stock per store x product)
"""
import csv
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np

SEED = 42
N_TX = 50_000
START, END = date(2026, 1, 1), date(2026, 6, 30)

BASE = Path(__file__).resolve().parent.parent
HIST_FILE = BASE / "data" / "historical" / "sales_history.csv"
INV_FILE = BASE / "data" / "inventory" / "inventory.csv"

rng = np.random.default_rng(SEED)

STORES = [("S01", "Chennai"), ("S02", "Bengaluru"), ("S03", "Mumbai"),
          ("S04", "Delhi"), ("S05", "Hyderabad")]
STORE_WEIGHTS = np.array([0.24, 0.22, 0.20, 0.19, 0.15])

# category -> (min margin, max margin, supplier, [(product, unit price INR)])
CATALOG = {
    "Groceries": (0.10, 0.20, "FreshFarm Distributors", [
        ("Basmati Rice 5kg", 650), ("Toor Dal 1kg", 160), ("Sunflower Oil 1L", 180),
        ("Wheat Atta 5kg", 290), ("Sugar 1kg", 48), ("Tea Powder 500g", 260),
        ("Coffee Powder 200g", 320), ("Milk 1L", 62), ("Bread 400g", 45),
        ("Eggs 12 Pack", 84), ("Salt 1kg", 28), ("Biscuits Pack", 40),
        ("Instant Noodles 4 Pack", 56), ("Paneer 200g", 90), ("Ghee 500ml", 320)]),
    "Electronics": (0.08, 0.25, "TechWorld Supplies", [
        ("Smartphone", 15999), ("Bluetooth Earbuds", 1999), ("Power Bank 10000mAh", 1299),
        ("LED TV 32 inch", 13999), ("Laptop", 52999), ("USB-C Charger", 799),
        ("Smartwatch", 2999), ("Wireless Mouse", 599), ("Keyboard", 899),
        ("Bluetooth Speaker", 2499), ("LED Bulb 9W", 120), ("Extension Board", 450),
        ("Electric Kettle", 1199), ("Mixer Grinder", 3299), ("Hair Dryer", 1099)]),
    "Clothing": (0.30, 0.50, "StyleThread Apparel", [
        ("Cotton T-Shirt", 499), ("Denim Jeans", 1499), ("Formal Shirt", 1199),
        ("Kurta", 999), ("Saree", 2499), ("Track Pants", 799), ("Hoodie", 1299),
        ("Socks 3 Pack", 249), ("Leggings", 399), ("Jacket", 2199), ("Shorts", 599),
        ("Kids T-Shirt", 349), ("Dupatta", 449), ("Polo T-Shirt", 699), ("Night Suit", 899)]),
    "Household": (0.20, 0.35, "HomeEssentials Traders", [
        ("Detergent Powder 1kg", 220), ("Dishwash Liquid 500ml", 110),
        ("Floor Cleaner 1L", 180), ("Toilet Cleaner 500ml", 95), ("Bath Soap 4 Pack", 160),
        ("Shampoo 340ml", 280), ("Toothpaste 150g", 110), ("Mop Set", 699),
        ("Bucket 20L", 249), ("Storage Container Set", 599), ("Bedsheet", 899),
        ("Towel", 349), ("Pressure Cooker 3L", 1599), ("Non-stick Pan", 899),
        ("Garbage Bags", 120)]),
}
CATEGORY_SHARE = {"Groceries": 0.40, "Electronics": 0.15, "Clothing": 0.20, "Household": 0.25}
QTY_LAMBDA = {"Groceries": 2.0, "Household": 1.0, "Clothing": 0.6, "Electronics": 0.15}


def build_products():
    products, pid = [], 1
    for cat, (lo, hi, supplier, items) in CATALOG.items():
        for name, price in items:
            margin = rng.uniform(lo, hi)
            products.append({
                "product_id": f"P{pid:03d}", "product_name": name, "category": cat,
                "unit_price": float(price), "cost_price": round(price * (1 - margin), 2),
                "supplier": supplier,
            })
            pid += 1
    return products


def product_weights(products):
    w = np.zeros(len(products))
    for cat, share in CATEGORY_SHARE.items():
        idx = [i for i, p in enumerate(products) if p["category"] == cat]
        w[idx] = share * rng.dirichlet(np.full(len(idx), 0.8))  # skewed popularity
    return w / w.sum()


def generate_sales(products):
    days = [START + timedelta(d) for d in range((END - START).days + 1)]
    day_w = np.array([1.3 if d.weekday() >= 5 else 1.0 for d in days])
    day_w *= np.linspace(1.0, 1.15, len(days))           # gentle growth trend
    day_w /= day_w.sum()

    day_idx = rng.choice(len(days), N_TX, p=day_w)
    hours = np.arange(9, 22)
    hour_w = np.array([2, 3, 4, 5, 5, 4, 4, 5, 6, 7, 7, 6, 4], float)
    hour = rng.choice(hours, N_TX, p=hour_w / hour_w.sum())
    minute = rng.integers(0, 60, N_TX)
    second = rng.integers(0, 60, N_TX)
    store_idx = rng.choice(len(STORES), N_TX, p=STORE_WEIGHTS)
    prod_idx = rng.choice(len(products), N_TX, p=product_weights(products))
    lam = np.array([QTY_LAMBDA[products[i]["category"]] for i in prod_idx])
    qty = 1 + rng.poisson(lam)
    discount = rng.choice([0, 5, 10, 15, 20], N_TX, p=[0.55, 0.15, 0.15, 0.10, 0.05])
    payment = rng.choice(["UPI", "Card", "Cash", "Wallet"], N_TX, p=[0.45, 0.30, 0.20, 0.05])

    order = np.argsort(day_idx * 86400 + hour * 3600 + minute * 60 + second, kind="stable")

    header = ["transaction_id", "transaction_date", "transaction_time", "store_id",
              "store_city", "product_id", "product_name", "category", "quantity",
              "unit_price", "cost_price", "discount_pct", "total_amount", "profit",
              "payment_mode"]
    units = np.zeros((len(STORES), len(products)), dtype=int)

    HIST_FILE.parent.mkdir(parents=True, exist_ok=True)
    with HIST_FILE.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for n, i in enumerate(order, start=1):
            p, (sid, city) = products[prod_idx[i]], STORES[store_idx[i]]
            q, d = int(qty[i]), int(discount[i])
            total = round(q * p["unit_price"] * (1 - d / 100), 2)
            profit = round(total - q * p["cost_price"], 2)
            units[store_idx[i], prod_idx[i]] += q
            w.writerow([f"T{n:07d}", days[day_idx[i]].isoformat(),
                        f"{hour[i]:02d}:{minute[i]:02d}:{second[i]:02d}",
                        sid, city, p["product_id"], p["product_name"], p["category"],
                        q, p["unit_price"], p["cost_price"], d, total, profit, payment[i]])
    return units, len(days)


def generate_inventory(products, units, n_days):
    header = ["product_id", "product_name", "category", "store_id", "current_stock",
              "reorder_level", "max_stock", "unit_cost", "last_restock_date", "supplier"]
    low = 0
    INV_FILE.parent.mkdir(parents=True, exist_ok=True)
    with INV_FILE.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for s, (sid, _) in enumerate(STORES):
            for j, p in enumerate(products):
                avg_daily = units[s, j] / n_days
                reorder = max(5, math.ceil(avg_daily * 7))   # ~1 week of cover
                max_stock = reorder * 4
                if rng.random() < 0.25:
                    stock = int(rng.integers(0, reorder))     # needs restocking
                    low += 1
                else:
                    stock = int(rng.integers(reorder, max_stock + 1))
                restock = END - timedelta(days=int(rng.integers(0, 30)))
                w.writerow([p["product_id"], p["product_name"], p["category"], sid,
                            stock, reorder, max_stock, p["cost_price"],
                            restock.isoformat(), p["supplier"]])
    return low


if __name__ == "__main__":
    products = build_products()
    units, n_days = generate_sales(products)
    low = generate_inventory(products, units, n_days)
    print(f"Sales     : {HIST_FILE.relative_to(BASE)}  ({N_TX:,} rows, {n_days} days)")
    print(f"Inventory : {INV_FILE.relative_to(BASE)}  "
          f"({len(STORES) * len(products)} rows, {low} below reorder level)")
