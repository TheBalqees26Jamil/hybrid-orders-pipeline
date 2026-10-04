"""
Section 3 - Materialized Views with INCREMENTAL refresh.

The incremental mechanism of the project is the `id_run` carried by every orders_validated document
(each upsert stamps the current run). For each MV we keep, in `mv_state`, the set of id_run values already
folded in. A refresh:
  1. finds pending runs   = distinct(id_run in orders_validated) - processed runs
  2. finds the DAYS touched by those runs
  3. recomputes ONLY those days (replace, never add -> safe for upserted/updated orders, idempotent)
  4. removes stale rows of those days, then marks the runs as processed.
`full=True` ignores the state and rebuilds everything (fallback only).
"""
import json
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from pymongo import ReplaceOne

from config.settings import COLLECTION_VALIDATED as V
from config.final_settings import MV_DAILY_SALES, MV_TOP_PRODUCTS, MV_STATE_COLLECTION
from src import mongo_setup
from src.indexes import create_indexes

MAX_RANGES = 400
RC = "$record_cleaned."


# ----------------------------- helpers -----------------------------
def days_to_ranges(days) -> list:
    """['2025-02-24','2025-02-25','2025-03-01'] -> [('2025-02-24','2025-02-26'), ('2025-03-01','2025-03-02')]
    (end is exclusive). Collapses to one min..max range if there are too many separate ranges."""
    parsed = []
    for d in set(days):
        try:
            parsed.append(date.fromisoformat(str(d)[:10]))
        except ValueError:
            continue
    parsed.sort()
    ranges = []
    for d in parsed:
        if ranges and d == ranges[-1][1]:
            ranges[-1][1] = d + timedelta(days=1)
        else:
            ranges.append([d, d + timedelta(days=1)])
    if len(ranges) > MAX_RANGES:
        ranges = [[parsed[0], parsed[-1] + timedelta(days=1)]]
    return [(s.isoformat(), e.isoformat()) for s, e in ranges]


def _range_match(field: str, ranges) -> dict:
    if ranges is None:
        return {}
    conds = [{field: {"$gte": s, "$lt": e}} for s, e in ranges]
    return conds[0] if len(conds) == 1 else {"$or": conds}


def _pending(mv_name: str, full: bool):
    db = mongo_setup.get_db()
    all_runs = {r for r in db[V].distinct("id_run") if r}
    state = db[MV_STATE_COLLECTION].find_one({"_id": mv_name}) or {}
    processed = set(state.get("processed_runs", []))
    pending = all_runs if full else all_runs - processed
    return sorted(pending), all_runs, processed


def _save_state(mv_name, all_runs, processed, pending, stats):
    new_processed = (processed | set(pending)) & all_runs
    mongo_setup.get_db()[MV_STATE_COLLECTION].update_one(
        {"_id": mv_name},
        {"$set": {"processed_runs": sorted(new_processed),
                  "last_refresh_at": datetime.now(timezone.utc).isoformat(), "last_stats": stats}},
        upsert=True)


def _affected_days(pending: list) -> list:
    pipeline = [{"$match": {"id_run": {"$in": pending}}},
                {"$group": {"_id": {"$substrBytes": [RC + "order_date", 0, 10]}}}]
    return [d["_id"] for d in mongo_setup.get_db()[V].aggregate(pipeline, allowDiskUse=True) if d["_id"]]


# ----------------------------- MV 1: daily_sales_summary -----------------------------
def refresh_daily_sales(full: bool = False) -> dict:
    t0 = time.time()
    db = mongo_setup.get_db()
    pending, all_runs, processed = _pending(MV_DAILY_SALES, full)
    base = {"mv": MV_DAILY_SALES, "mode": "full" if full else "incremental", "pending_runs": len(pending)}
    if not pending:
        return {**base, "status": "up_to_date", "rows_in_mv": db[MV_DAILY_SALES].count_documents({}),
                "seconds": round(time.time() - t0, 3)}

    ranges = None if full else days_to_ranges(_affected_days(pending))
    token = uuid.uuid4().hex
    deleted = 0
    if ranges != []:
        match = _range_match("record_cleaned.order_date", ranges)
        num = lambda f: {"$convert": {"input": RC + f, "to": "double", "onError": 0, "onNull": 0}}
        pipeline = [
            {"$match": match},
            {"$group": {"_id": {"$substrBytes": [RC + "order_date", 0, 10]},
                        "orders_count": {"$sum": 1},
                        "total_sales": {"$sum": num("total_amount")},
                        "total_delivery_cost": {"$sum": num("delivery_cost")}}},
            {"$set": {"date": "$_id",
                      "avg_order_value": {"$round": [{"$divide": ["$total_sales", "$orders_count"]}, 2]},
                      "total_sales": {"$round": ["$total_sales", 2]},
                      "total_delivery_cost": {"$round": ["$total_delivery_cost", 2]},
                      "refresh_token": token, "refreshed_at": "$$NOW"}},
            {"$merge": {"into": MV_DAILY_SALES, "on": "_id",
                        "whenMatched": "replace", "whenNotMatched": "insert"}},
        ]
        list(db[V].aggregate(pipeline, allowDiskUse=True))   # $merge runs when the cursor is consumed
        stale = {**_range_match("_id", ranges), "refresh_token": {"$ne": token}}
        deleted = db[MV_DAILY_SALES].delete_many(stale).deleted_count

    stats = {**base, "status": "refreshed", "date_ranges_recomputed": "all" if ranges is None else len(ranges),
             "stale_rows_removed": deleted, "rows_in_mv": db[MV_DAILY_SALES].count_documents({}),
             "seconds": round(time.time() - t0, 3)}
    _save_state(MV_DAILY_SALES, all_runs, processed, pending, stats)
    return stats


# ----------------------------- MV 2: top_products_summary -----------------------------
_KEY_FIELDS = ("sku", "product_id", "item_id", "id", "product_sku", "product_name", "name", "title")
_NAME_FIELDS = ("name", "product_name", "title")
_QTY_FIELDS = ("qty", "quantity", "count", "units")
_PRICE_FIELDS = ("price", "unit_price", "unit_cost", "cost")
_LINE_TOTAL_FIELDS = ("line_total", "total", "subtotal")


def _to_float(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def extract_items(items_json) -> list:
    """Parses items_json defensively (field names differ between datasets) -> list of dicts."""
    if items_json in (None, ""):
        return []
    try:
        data = json.loads(items_json) if isinstance(items_json, str) else items_json
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(data, dict):
        data = data["items"] if isinstance(data.get("items"), list) else [data]
    if not isinstance(data, list):
        return []
    out = []
    for it in data:
        if not isinstance(it, dict):
            continue
        key = next((str(it[f]) for f in _KEY_FIELDS if it.get(f) not in (None, "")), None)
        if key is None:
            continue
        name = next((str(it[f]) for f in _NAME_FIELDS if it.get(f)), key)
        qty = next((q for f in _QTY_FIELDS if (q := _to_float(it.get(f))) is not None), 1.0)
        line_total = next((t for f in _LINE_TOTAL_FIELDS if (t := _to_float(it.get(f))) is not None), None)
        price = next((p for f in _PRICE_FIELDS if (p := _to_float(it.get(f))) is not None), None)
        revenue = line_total if line_total is not None else (qty * price if price is not None else 0.0)
        out.append({"product": key, "name": name, "quantity": qty, "revenue": revenue})
    return out


def refresh_top_products(full: bool = False) -> dict:
    t0 = time.time()
    db = mongo_setup.get_db()
    pending, all_runs, processed = _pending(MV_TOP_PRODUCTS, full)
    base = {"mv": MV_TOP_PRODUCTS, "mode": "full" if full else "incremental", "pending_runs": len(pending)}
    if not pending:
        return {**base, "status": "up_to_date", "rows_in_mv": db[MV_TOP_PRODUCTS].count_documents({}),
                "seconds": round(time.time() - t0, 3)}

    ranges = None if full else days_to_ranges(_affected_days(pending))
    token = uuid.uuid4().hex
    now = datetime.now(timezone.utc).isoformat()
    written = deleted = 0
    if ranges != []:
        match = _range_match("record_cleaned.order_date", ranges)
        cur = db[V].find(match, {"record_cleaned.order_date": 1, "record_cleaned.items_json": 1}).batch_size(5000)
        agg = {}
        for doc in cur:
            rc = doc.get("record_cleaned", {})
            day = str(rc.get("order_date", ""))[:10]
            for it in extract_items(rc.get("items_json")):
                e = agg.setdefault((day, it["product"]),
                                   {"product_name": it["name"], "quantity": 0.0, "revenue": 0.0, "order_lines": 0})
                e["quantity"] += it["quantity"]
                e["revenue"] += it["revenue"]
                e["order_lines"] += 1
        ops = []
        for (day, product), e in agg.items():
            ops.append(ReplaceOne({"_id": f"{day}|{product}"},
                                  {"_id": f"{day}|{product}", "date": day, "product": product,
                                   "product_name": e["product_name"], "quantity": e["quantity"],
                                   "revenue": round(e["revenue"], 2), "order_lines": e["order_lines"],
                                   "refresh_token": token, "refreshed_at": now}, upsert=True))
            if len(ops) >= 5000:
                db[MV_TOP_PRODUCTS].bulk_write(ops, ordered=False)
                written += len(ops)
                ops = []
        if ops:
            db[MV_TOP_PRODUCTS].bulk_write(ops, ordered=False)
            written += len(ops)
        stale = {**_range_match("date", ranges), "refresh_token": {"$ne": token}}
        deleted = db[MV_TOP_PRODUCTS].delete_many(stale).deleted_count

    stats = {**base, "status": "refreshed", "date_ranges_recomputed": "all" if ranges is None else len(ranges),
             "rows_written": written, "stale_rows_removed": deleted,
             "rows_in_mv": db[MV_TOP_PRODUCTS].count_documents({}), "seconds": round(time.time() - t0, 3)}
    _save_state(MV_TOP_PRODUCTS, all_runs, processed, pending, stats)
    return stats


# ----------------------------- entry points -----------------------------
REFRESHERS = {MV_DAILY_SALES: refresh_daily_sales, MV_TOP_PRODUCTS: refresh_top_products}


def refresh_all(mv: str = "all", full: bool = False) -> dict:
    if mv != "all" and mv not in REFRESHERS:
        raise KeyError(mv)
    create_indexes()  # idempotent; keeps id_run / order_date lookups fast
    names = list(REFRESHERS) if mv == "all" else [mv]
    return {"refreshed": [REFRESHERS[n](full=full) for n in names]}


def read_mv(name: str, limit: int = 20) -> dict:
    if name not in REFRESHERS:
        raise KeyError(name)
    coll = mongo_setup.get_db()[name]
    sort = [("_id", -1)] if name == MV_DAILY_SALES else [("quantity", -1)]
    rows = list(coll.find({}, {"_id": 0, "refresh_token": 0}).sort(sort).limit(max(1, min(limit, 500))))
    return {"mv": name, "total_rows": coll.count_documents({}), "sample": rows}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Refresh materialized views (incremental by default).")
    ap.add_argument("--mv", default="all")
    ap.add_argument("--full", action="store_true")
    a = ap.parse_args()
    try:
        print(json.dumps(refresh_all(a.mv, a.full), indent=2, default=str))
    finally:
        mongo_setup.close_client()
