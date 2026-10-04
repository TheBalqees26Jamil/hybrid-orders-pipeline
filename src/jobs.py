"""Section 4 - Scheduled jobs (APScheduler) with execution log in `job_runs`; every job can be run manually."""
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.final_settings import (JOB_RUNS_COLLECTION, REPORTS_DIR, JOB_TIMEZONE,
                                   JOB_MV_REFRESH_INTERVAL_MINUTES, JOB_DAILY_REPORT_CRON)
from src import mongo_setup
from src.aggregations import run_aggregation
from src.materialized_views import refresh_all


def job_refresh_materialized_views() -> dict:
    return refresh_all("all", full=False)


def job_daily_sales_report() -> dict:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = Path(REPORTS_DIR)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"daily_report_{ts}.json"
    sections = ("sales_by_city", "top_customers", "orders_by_status", "sales_by_period", "top_products")
    report = {"generated_at": datetime.now(timezone.utc).isoformat()}
    for name in sections:
        report[name] = run_aggregation(name, {"limit": 10})["results"]
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return {"report_file": str(path), "rows_per_section": {k: len(report[k]) for k in sections}}


def _interval_trigger():
    from apscheduler.triggers.interval import IntervalTrigger
    return IntervalTrigger(minutes=JOB_MV_REFRESH_INTERVAL_MINUTES, timezone=JOB_TIMEZONE)


def _cron_trigger():
    from apscheduler.triggers.cron import CronTrigger
    return CronTrigger.from_crontab(JOB_DAILY_REPORT_CRON, timezone=JOB_TIMEZONE)


JOBS = {
    "refresh_materialized_views": {
        "description": "Incrementally refreshes daily_sales_summary and top_products_summary.",
        "func": job_refresh_materialized_views, "trigger": _interval_trigger,
        "schedule": f"every {JOB_MV_REFRESH_INTERVAL_MINUTES} minute(s)",
    },
    "daily_sales_report": {
        "description": "Builds a periodic JSON sales report (aggregations) into reports/.",
        "func": job_daily_sales_report, "trigger": _cron_trigger,
        "schedule": f"cron '{JOB_DAILY_REPORT_CRON}' ({JOB_TIMEZONE})",
    },
}

_locks = {n: threading.Lock() for n in JOBS}
_scheduler = None


def run_job(name: str, trigger: str = "manual") -> dict:
    """Runs a job and records name, trigger, start/end time, status and result/error in job_runs."""
    if name not in JOBS:
        raise KeyError(name)
    if not _locks[name].acquire(blocking=False):
        return {"job_name": name, "status": "skipped", "reason": "already running"}
    started = datetime.now(timezone.utc)
    t0 = time.time()
    result, error, status = None, None, "success"
    try:
        result = JOBS[name]["func"]()
    except Exception as e:  # noqa - failure must be logged, not crash the scheduler
        status, error = "failed", f"{type(e).__name__}: {e}"
    finally:
        _locks[name].release()
    doc = {"job_name": name, "trigger": trigger, "started_at": started.isoformat(),
           "ended_at": datetime.now(timezone.utc).isoformat(), "duration_seconds": round(time.time() - t0, 3),
           "status": status, "result": result, "error": error}
    try:
        mongo_setup.get_db()[JOB_RUNS_COLLECTION].insert_one(dict(doc))
    except Exception as e:  # noqa
        doc["log_error"] = f"{type(e).__name__}: {e}"
    return doc


def get_jobs(history: int = 5) -> list:
    out = []
    coll = mongo_setup.get_db()[JOB_RUNS_COLLECTION]
    for name, spec in JOBS.items():
        runs = list(coll.find({"job_name": name}, {"_id": 0}).sort("started_at", -1).limit(history))
        next_run = None
        if _scheduler is not None:
            try:
                nr = _scheduler.get_job(name).next_run_time
                next_run = nr.isoformat() if nr else None
            except Exception:  # noqa
                pass
        out.append({"name": name, "description": spec["description"], "schedule": spec["schedule"],
                    "next_run": next_run, "last_run": runs[0] if runs else None, "recent_runs": runs})
    return out


def start_scheduler(blocking: bool = False):
    global _scheduler
    if _scheduler is not None and getattr(_scheduler, "running", False):
        return _scheduler
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.schedulers.blocking import BlockingScheduler
    s = (BlockingScheduler if blocking else BackgroundScheduler)(timezone=JOB_TIMEZONE)
    for name, spec in JOBS.items():
        s.add_job(run_job, trigger=spec["trigger"](), id=name, name=name, args=[name, "scheduled"],
                  max_instances=1, coalesce=True, replace_existing=True)
    _scheduler = s
    s.start()
    return s


def stop_scheduler():
    global _scheduler
    if _scheduler is not None and getattr(_scheduler, "running", False):
        _scheduler.shutdown(wait=False)
    _scheduler = None


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Scheduled jobs: list | run <name> | scheduler")
    ap.add_argument("command", choices=["list", "run", "scheduler"])
    ap.add_argument("name", nargs="?")
    a = ap.parse_args()
    try:
        if a.command == "list":
            print(json.dumps(get_jobs(), indent=2, default=str))
        elif a.command == "run":
            print(json.dumps(run_job(a.name), indent=2, default=str))
        else:
            print("Standalone scheduler running. Ctrl+C to stop.")
            start_scheduler(blocking=True)
    finally:
        mongo_setup.close_client()
