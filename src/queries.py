
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import COLLECTION_VALIDATED as V, COLLECTION_QUARANTINE as Q, QuarantineErrorCodes as QEC
from src import mongo_setup

RC = "record_cleaned."


def _lim(p, default):
    try:
        return max(1, min(int(p.get("limit", default)), 1000))
    except (TypeError, ValueError):
        return default


QUERIES = {
    "order_by_id": {
        "description": "Fetch one validated order by its business key (id_order).",
        "collection": V, "index": "uniq_id_order (unique index created by mongo_setup)",
        "params": ["order_id"],
        "build": lambda p: ({"id_order": p["order_id"]}, None, 1),
    },
    "orders_by_customer": {
        "description": "All validated orders of one customer, newest first.",
        "collection": V, "index": "idx_validated_customer",
        "params": ["customer_id", "limit"],
        "build": lambda p: ({RC + "customer_id": p["customer_id"]}, [(RC + "order_date", -1)], _lim(p, 50)),
    },
    "orders_by_city_status": {
        "description": "Orders in a city with a given status, newest first (e.g. confirmed orders in Sana'a).",
        "collection": V, "index": "idx_validated_city_status_date",
        "params": ["city", "status", "limit"],
        "build": lambda p: ({RC + "city": p["city"], RC + "status": p["status"]},
                            [(RC + "order_date", -1)], _lim(p, 50)),
    },
    "orders_by_date_range": {
        "description": "Orders whose order_date falls in [date_from, date_to) (YYYY-MM-DD).",
        "collection": V, "index": "idx_validated_order_date",
        "params": ["date_from", "date_to", "limit"],
        "build": lambda p: ({RC + "order_date": {"$gte": p["date_from"], "$lt": p["date_to"]}},
                            [(RC + "order_date", 1)], _lim(p, 100)),
    },
    "quarantined_by_error_code": {
        "description": "Quarantined records that carry a specific error code.",
        "collection": Q, "index": "idx_quarantine_codes",
        "params": ["error_code", "limit"],
        "build": lambda p: ({"codes_error": p["error_code"]}, None, _lim(p, 50)),
    },
    "orders_by_run": {
        "description": "Validated orders written/updated by one ingestion run (id_run).",
        "collection": V, "index": "idx_validated_id_run",
        "params": ["id_run", "limit"],
        "build": lambda p: ({"id_run": p["id_run"]}, None, _lim(p, 50)),
    },
}


def sample_defaults() -> dict:
    """Default parameter values taken from real documents currently in the DB."""
    db = mongo_setup.get_db()
    d = {}
    v = db[V].find_one({}, {"id_order": 1, "id_run": 1, RC + "customer_id": 1, RC + "city": 1,
                            RC + "status": 1, RC + "order_date": 1})
    if v:
        rc = v.get("record_cleaned", {})
        d.update(order_id=v.get("id_order"), id_run=v.get("id_run"), customer_id=rc.get("customer_id"),
                 city=rc.get("city"), status=rc.get("status"))
        day = str(rc.get("order_date", ""))[:10]
        try:
            d["date_from"] = day
            d["date_to"] = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        except ValueError:
            pass
    q = db[Q].find_one({}, {"codes_error": 1})
    codes = (q or {}).get("codes_error") or []
    d["error_code"] = codes[0] if codes else QEC.ID_ORDER_MISSING
    return d


def resolve_params(supplied=None) -> dict:
    p = sample_defaults()
    for k, v in (supplied or {}).items():
        if v not in (None, ""):
            p[k] = v
    return p


def build_find(name: str, supplied=None, resolved=None) -> dict:
    if name not in QUERIES:
        raise KeyError(name)
    spec = QUERIES[name]
    p = resolved if resolved is not None else resolve_params(supplied)
    if supplied and resolved is not None:
        p = {**resolved, **{k: v for k, v in supplied.items() if v not in (None, "")}}
    flt, sort, limit = spec["build"](p)
    return {"collection": spec["collection"], "filter": flt, "sort": sort, "limit": limit,
            "params_used": {k: p.get(k) for k in spec["params"]}}


def run_query(name: str, supplied=None) -> dict:
    b = build_find(name, supplied)
    cur = mongo_setup.get_db()[b["collection"]].find(b["filter"], {"_id": 0})
    if b["sort"]:
        cur = cur.sort(b["sort"])
    results = list(cur.limit(b["limit"]))
    return {"name": name, "description": QUERIES[name]["description"], "collection": b["collection"],
            "index": QUERIES[name]["index"], "params_used": b["params_used"],
            "filter": b["filter"], "count": len(results), "results": results}


def list_queries() -> list:
    return [{"name": n, "description": s["description"], "collection": s["collection"],
             "index": s["index"], "params": s["params"]} for n, s in QUERIES.items()]
