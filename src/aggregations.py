"""Section 2 - Aggregation reports. Each has a clear name and runs independently."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import COLLECTION_VALIDATED as V, COLLECTION_QUARANTINE as Q
from config.final_settings import MV_TOP_PRODUCTS
from src import mongo_setup

RC = "$record_cleaned."


def _num(path):
    """record_cleaned amounts are stored as strings -> convert safely."""
    return {"$convert": {"input": path, "to": "double", "onError": 0, "onNull": 0}}


def _lim(p, default):
    try:
        return max(1, min(int(p.get("limit", default)), 1000))
    except (TypeError, ValueError):
        return default


def _sales_by_city(p):
    return [
        {"$group": {"_id": RC + "city", "orders_count": {"$sum": 1},
                    "total_sales": {"$sum": _num(RC + "total_amount")}}},
        {"$set": {"avg_order_value": {"$round": [{"$divide": ["$total_sales", "$orders_count"]}, 2]},
                  "total_sales": {"$round": ["$total_sales", 2]}}},
        {"$sort": {"total_sales": -1}}, {"$limit": _lim(p, 50)},
        {"$project": {"_id": 0, "city": "$_id", "orders_count": 1, "total_sales": 1, "avg_order_value": 1}},
    ]


def _top_customers(p):
    return [
        {"$group": {"_id": RC + "customer_id", "customer_name": {"$first": RC + "customer_name"},
                    "orders_count": {"$sum": 1}, "total_spent": {"$sum": _num(RC + "total_amount")}}},
        {"$set": {"total_spent": {"$round": ["$total_spent", 2]}}},
        {"$sort": {"total_spent": -1}}, {"$limit": _lim(p, 10)},
        {"$project": {"_id": 0, "customer_id": "$_id", "customer_name": 1, "orders_count": 1, "total_spent": 1}},
    ]


def _orders_by_status(p):
    return [
        {"$group": {"_id": RC + "status", "orders_count": {"$sum": 1},
                    "total_amount": {"$sum": _num(RC + "total_amount")}}},
        {"$set": {"total_amount": {"$round": ["$total_amount", 2]}}},
        {"$sort": {"orders_count": -1}},
        {"$project": {"_id": 0, "status": "$_id", "orders_count": 1, "total_amount": 1}},
    ]


def _sales_by_period(p):
    n = {"day": 10, "month": 7, "year": 4}.get(str(p.get("granularity", "month")).lower(), 7)
    return [
        {"$group": {"_id": {"$substrBytes": [RC + "order_date", 0, n]}, "orders_count": {"$sum": 1},
                    "total_sales": {"$sum": _num(RC + "total_amount")}}},
        {"$set": {"total_sales": {"$round": ["$total_sales", 2]}}},
        {"$sort": {"_id": 1}}, {"$limit": _lim(p, 1000)},
        {"$project": {"_id": 0, "period": "$_id", "orders_count": 1, "total_sales": 1}},
    ]


def _payment_methods(p):
    return [
        {"$group": {"_id": {"method": RC + "payment_method", "payment_status": RC + "payment_status"},
                    "orders_count": {"$sum": 1}, "paid_amount": {"$sum": _num(RC + "payment_amount")}}},
        {"$set": {"paid_amount": {"$round": ["$paid_amount", 2]}}},
        {"$sort": {"orders_count": -1}},
        {"$project": {"_id": 0, "payment_method": "$_id.method", "payment_status": "$_id.payment_status",
                      "orders_count": 1, "paid_amount": 1}},
    ]


def _quarantine_by_code(p):
    return [
        {"$unwind": "$codes_error"},
        {"$group": {"_id": "$codes_error", "records": {"$sum": 1}}},
        {"$sort": {"records": -1}},
        {"$project": {"_id": 0, "error_code": "$_id", "records": 1}},
    ]


def _top_products(p):
    return [
        {"$group": {"_id": "$product", "product_name": {"$first": "$product_name"},
                    "total_quantity": {"$sum": "$quantity"}, "total_revenue": {"$sum": "$revenue"}}},
        {"$set": {"total_revenue": {"$round": ["$total_revenue", 2]}}},
        {"$sort": {"total_quantity": -1}}, {"$limit": _lim(p, 10)},
        {"$project": {"_id": 0, "product": "$_id", "product_name": 1, "total_quantity": 1, "total_revenue": 1}},
    ]


def _prepare_top_products():
    db = mongo_setup.get_db()
    if db[MV_TOP_PRODUCTS].count_documents({}, limit=1) == 0:
        from src.materialized_views import refresh_top_products
        refresh_top_products()


AGGREGATIONS = {
    "sales_by_city": {"description": "Orders count, total sales and average order value per city.",
                      "collection": V, "params": ["limit"], "build": _sales_by_city},
    "top_customers": {"description": "Top customers by total spent.",
                      "collection": V, "params": ["limit"], "build": _top_customers},
    "orders_by_status": {"description": "Distribution of orders by status.",
                         "collection": V, "params": [], "build": _orders_by_status},
    "sales_by_period": {"description": "Sales per period; granularity=day|month|year (default month).",
                        "collection": V, "params": ["granularity", "limit"], "build": _sales_by_period},
    "payment_methods": {"description": "Orders and paid amounts by payment method and payment status.",
                        "collection": V, "params": [], "build": _payment_methods},
    "quarantine_by_error_code": {"description": "How many quarantined records per error code.",
                                 "collection": Q, "params": [], "build": _quarantine_by_code},
    "top_products": {"description": "Best-selling products (reads the top_products_summary materialized view).",
                     "collection": MV_TOP_PRODUCTS, "params": ["limit"], "build": _top_products,
                     "prepare": _prepare_top_products},
}


def run_aggregation(name: str, supplied=None) -> dict:
    if name not in AGGREGATIONS:
        raise KeyError(name)
    spec = AGGREGATIONS[name]
    if spec.get("prepare"):
        spec["prepare"]()
    p = {k: v for k, v in (supplied or {}).items() if v not in (None, "")}
    results = list(mongo_setup.get_db()[spec["collection"]].aggregate(spec["build"](p), allowDiskUse=True))
    return {"name": name, "description": spec["description"], "collection": spec["collection"],
            "count": len(results), "results": results}


def list_aggregations() -> list:
    return [{"name": n, "description": s["description"], "collection": s["collection"],
             "params": s["params"]} for n, s in AGGREGATIONS.items()]


if __name__ == "__main__":
    import argparse, json
    ap = argparse.ArgumentParser()
    ap.add_argument("name", choices=list(AGGREGATIONS))
    ap.add_argument("--limit", type=int)
    a = ap.parse_args()
    try:
        print(json.dumps(run_aggregation(a.name, {"limit": a.limit} if a.limit else None),
                         indent=2, ensure_ascii=False, default=str))
    finally:
        mongo_setup.close_client()
