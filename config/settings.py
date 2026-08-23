import os
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()

# ===================== Base paths =====================
BASE_DIR = Path(__file__).resolve().parent.parent          # project root
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"      # holds the real (large) source CSV
SAMPLE_DIR = DATA_DIR / "sample"    # holds the generated small sample
REPORTS_DIR = BASE_DIR / "reports"
RESULTS_JSON_PATH = REPORTS_DIR / "results.json"


RAW_INPUT_FILE = os.getenv("RAW_INPUT_FILE", str(INPUT_DIR / "orders_huge_mixed_quality.csv"))
SAMPLE_OUTPUT_FILE = os.getenv("SAMPLE_OUTPUT_FILE", str(SAMPLE_DIR / "orders_sample.csv"))
SAMPLE_DEFAULT_ROWS = int(os.getenv("SAMPLE_DEFAULT_ROWS", "100000"))


CSV_ENCODING = os.getenv("CSV_ENCODING", "utf-8-sig")

SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))

# ===================== Python Batch Loader =====================
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))   # number of records per insert_many batch

# ===================== PySpark Loader =====================
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")   # local[*] by default, or spark://IP:7077 for cluster mode
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "HybridOrdersPipeline")

SPARK_DEFAULT_PARTITIONS = int(os.getenv("SPARK_DEFAULT_PARTITIONS", "8"))

# Spark MongoDB Connector version (Maven package) - used with --packages at runtime
SPARK_MONGO_CONNECTOR_PACKAGE = os.getenv(
    "SPARK_MONGO_CONNECTOR_PACKAGE",
    "org.mongodb.spark:mongo-spark-connector_2.12:11.1.0",
)

# ===================== MongoDB =====================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "orders_pipeline_db")

COLLECTION_RAW = os.getenv("COLLECTION_RAW", "orders_raw")
COLLECTION_VALIDATED = os.getenv("COLLECTION_VALIDATED", "orders_validated")
COLLECTION_QUARANTINE = os.getenv("COLLECTION_QUARANTINE", "quarantine_orders")

RAW_CSV_COLUMNS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json",
]

# ===================== Quality rules =====================

TARGET_CURRENCY = "YER"

# ===================== Quarantine error codes =====================
class QuarantineErrorCodes:
    ID_ORDER_MISSING = "ID_ORDER_MISSING"
    ID_CUSTOMER_MISSING = "ID_CUSTOMER_MISSING"
    DATE_IMPOSSIBLE_INVALID = "DATE_IMPOSSIBLE_INVALID"
    JSON_ITEMS_CORRUPTED = "JSON_ITEMS_CORRUPTED"
    ITEMS_EMPTY = "ITEMS_EMPTY"
    PRICE_UNKNOWN = "PRICE_UNKNOWN"
    VALUE_NEGATIVE_AMBIGUOUS = "VALUE_NEGATIVE_AMBIGUOUS"
    ID_ORDER_DUPLICATE = "ID_ORDER_DUPLICATE"
    ERRORS_CONFLICTING_MULTIPLE = "ERRORS_CONFLICTING_MULTIPLE"


# ===================== Quality status values =====================
class QualityStatus:
    VALID = "valid"
    CORRECTED = "corrected"
    QUARANTINED = "quarantined"


# Ensure required directories exist when settings are loaded
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
INPUT_DIR.mkdir(parents=True, exist_ok=True)
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
