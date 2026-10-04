"""Section 1 - Indexes. Every index documents which queries it serves and why."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from pymongo.errors import OperationFailure

from config.settings import COLLECTION_VALIDATED as V, COLLECTION_QUARANTINE as Q
from config.final_settings import MV_TOP_PRODUCTS
from src import mongo_setup

INDEX_DEFINITIONS = [
    {
        "name": "idx_validated_customer",
        "collection": V,
        "keys": [("record_cleaned.customer_id", 1)],
        "type": "single",
        "serves": ["orders_by_customer"],
        "why": "Equality lookup on customer_id. Without it MongoDB scans the whole collection (COLLSCAN); "
               "with it only that customer's entries are read.",
    },
    {
        "name": "idx_validated_city_status_date",
        "collection": V,
        "keys": [("record_cleaned.city", 1), ("record_cleaned.status", 1), ("record_cleaned.order_date", -1)],
        "type": "compound",
        "serves": ["orders_by_city_status"],
        "why": "Compound index following the ESR rule: two equality fields (city, status) first, then the sort "
               "field (order_date desc). Serves filter + sort + limit from the index, no in-memory SORT.",
    },
    {
        "name": "idx_validated_order_date",
        "collection": V,
        "keys": [("record_cleaned.order_date", 1)],
        "type": "single",
        "serves": ["orders_by_date_range"],
        "why": "Range query on the standardized ISO date string (sorts chronologically). Also used by the "
               "materialized-view refresh to recompute only affected days.",
    },
    {
        "name": "idx_validated_id_run",
        "collection": V,
        "keys": [("id_run", 1)],
        "type": "single",
        "serves": ["orders_by_run"],
        "why": "Incremental refresh of materialized views finds new/changed records by id_run; also lets "
               "users inspect one ingestion run.",
    },
    {
        "name": "idx_quarantine_codes",
        "collection": Q,
        "keys": [("codes_error", 1)],
        "type": "multikey",
        "serves": ["quarantined_by_error_code"],
        "why": "codes_error is an array; a multikey index lets us find all quarantined records for one error code.",
    },
    {
        "name": "idx_top_products_date",
        "collection": MV_TOP_PRODUCTS,
        "keys": [("date", 1)],
        "type": "single",
        "serves": [],
        "why": "Lets the incremental refresh of top_products_summary replace only the affected date ranges.",
    },
]

INDEX_BY_NAME = {d["name"]: d for d in INDEX_DEFINITIONS}


def create_indexes() -> list:
    db = mongo_setup.get_db()
    out = []
    for d in INDEX_DEFINITIONS:
        try:
            db[d["collection"]].create_index(d["keys"], name=d["name"])
            status = "ensured"
        except OperationFailure as e:
            status = f"skipped: {e}"
        out.append({"name": d["name"], "collection": d["collection"],
                    "keys": [list(k) for k in d["keys"]], "type": d["type"],
                    "serves": d["serves"], "status": status})
    return out


def drop_indexes() -> list:
    """Drops only the indexes defined here (used to measure 'before' in explain)."""
    db = mongo_setup.get_db()
    dropped = []
    for d in INDEX_DEFINITIONS:
        coll = db[d["collection"]]
        try:
            if d["name"] in coll.index_information():
                coll.drop_index(d["name"])
                dropped.append(d["name"])
        except OperationFailure:
            pass
    return dropped
