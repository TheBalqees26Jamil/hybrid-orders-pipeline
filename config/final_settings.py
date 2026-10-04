
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# New collections created by the final phase
MV_DAILY_SALES = "daily_sales_summary"
MV_TOP_PRODUCTS = "top_products_summary"
MV_STATE_COLLECTION = "mv_state"        # tracks which id_run values each MV already processed
JOB_RUNS_COLLECTION = "job_runs"        # execution log of scheduled / manual jobs

# Paths (REPORTS_DIR / DATA_DIR come from the midterm config/settings.py)
from config.settings import REPORTS_DIR, DATA_DIR  # noqa: E402

UPLOAD_DIR = os.getenv("UPLOAD_DIR", str(DATA_DIR / "uploads"))

# Scheduler
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"
JOB_TIMEZONE = os.getenv("JOB_TIMEZONE", "UTC")
JOB_MV_REFRESH_INTERVAL_MINUTES = int(os.getenv("JOB_MV_REFRESH_INTERVAL_MINUTES", "15"))
JOB_DAILY_REPORT_CRON = os.getenv("JOB_DAILY_REPORT_CRON", "0 2 * * *")  # every day 02:00
