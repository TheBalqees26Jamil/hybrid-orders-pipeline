import sys
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, OperationFailure

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
    QualityStatus,
)

_client = None  # Shared (singleton) connection to avoid opening multiple unnecessary connections


def get_client() -> MongoClient:
    """Returns a single shared MongoDB client for the lifetime of the run."""
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client


def get_db():
    """Returns the target database."""
    return get_client()[MONGO_DB_NAME]


def get_raw_collection():
    return get_db()[COLLECTION_RAW]


def get_validated_collection():
    return get_db()[COLLECTION_VALIDATED]


def get_quarantine_collection():
    return get_db()[COLLECTION_QUARANTINE]


def close_client():
    """Closes the connection safely - always called inside finally in the main entry point."""
    global _client
    if _client is not None:
        _client.close()
        _client = None


# ===================== Stage 9: collection setup =====================

_VALIDATED_SCHEMA_VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["id_order", "quality_status"],
        "properties": {
            "id_order": {
                "bsonType": "string",
                "description": "Business key - required, must be a non-null string.",
            },
            "quality_status": {
                "enum": [QualityStatus.VALID, QualityStatus.CORRECTED],
                "description": "Must be 'valid' or 'corrected' - quarantined records never live here.",
            },
        },
    }
}


def _ensure_plain_collection(db, name: str) -> str:
    
    if name in db.list_collection_names():
        return "already existed - left as-is (no validator, no index)"
    db.create_collection(name)
    return "created (no validator, no index)"


def _ensure_validated_collection(db) -> str:
    
    name = COLLECTION_VALIDATED
    if name not in db.list_collection_names():
        db.create_collection(name, validator=_VALIDATED_SCHEMA_VALIDATOR)
        status = "created with JSON Schema validator"
    else:
        db.command(
            "collMod",
            name,
            validator=_VALIDATED_SCHEMA_VALIDATOR,
            validationLevel="moderate",
        )
        status = "already existed - validator applied/updated via collMod"

    db[name].create_index("id_order", unique=True, name="uniq_id_order")
    return f"{status}; unique index on 'id_order' ensured"


def _ensure_quarantine_collection(db) -> str:
    
    name = COLLECTION_QUARANTINE
    if name not in db.list_collection_names():
        db.create_collection(name)
        status = "created (no validator)"
    else:
        status = "already existed (no validator - left as-is)"

    try:
        db[name].create_index(
            [("file_source", 1), ("number_row_source", 1)],
            unique=True,
            name="uniq_file_source_number_row_source",
        )
        index_status = "unique compound index on (file_source, number_row_source) ensured"
    except OperationFailure as e:
        index_status = (
            f"[WARNING] could not create unique index on "
            f"(file_source, number_row_source) - {type(e).__name__}: {e}. "
            f"quarantine_orders most likely already has duplicate "
            f"(file_source, number_row_source) documents from before this "
            f"fix. Existing data was NOT touched/deleted. To enable the "
            f"DB-level uniqueness guarantee, manually de-duplicate "
            f"quarantine_orders (keep one document per "
            f"(file_source, number_row_source) pair) and re-run setup. "
            f"The application-level upsert still prevents new duplicates "
            f"in the meantime."
        )
        print(f"  {index_status}")

    return f"{status}; {index_status}"


def setup_collections() -> dict:
    
    db = get_db()
    summary = {}

    summary[COLLECTION_RAW] = _ensure_plain_collection(db, COLLECTION_RAW)
    summary[COLLECTION_VALIDATED] = _ensure_validated_collection(db)
    summary[COLLECTION_QUARANTINE] = _ensure_quarantine_collection(db)

    print("-" * 60)
    print("MongoDB collection setup (Stage 9):")
    for coll_name, status in summary.items():
        print(f"  {coll_name}: {status}")
    print("-" * 60)

    return summary


if __name__ == "__main__":
    setup_collections()