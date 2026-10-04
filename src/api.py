"""Section 5 - Unified FastAPI layer. Swagger UI at /docs. Run: uvicorn src.api:app --port 8000"""
import math
import shutil
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parent.parent))

from bson import ObjectId
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse

from config.settings import MONGO_DB_NAME
from config.final_settings import ENABLE_SCHEDULER, UPLOAD_DIR
from src import mongo_setup, jobs
from src.aggregations import AGGREGATIONS, list_aggregations, run_aggregation
from src.explain_report import explain_before_after
from src.indexes import create_indexes
from src.main import run_pipeline
from src.materialized_views import read_mv, refresh_all, REFRESHERS
from src.queries import QUERIES, list_queries, run_query


def to_jsonable(o):
    if isinstance(o, dict):
        return {str(k): to_jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple, set)):
        return [to_jsonable(v) for v in o]
    if isinstance(o, ObjectId):
        return str(o)
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
        return None
    return o


@asynccontextmanager
async def lifespan(app: FastAPI):
    if ENABLE_SCHEDULER:
        jobs.start_scheduler()
    yield
    jobs.stop_scheduler()
    mongo_setup.close_client()


app = FastAPI(title="Hybrid Orders Pipeline API",
              description="Unified run/test interface for the orders ETL project (Big Data - Phase 2).",
              version="2.0", lifespan=lifespan)


@app.get("/health")
def health():
    try:
        mongo_setup.get_client().admin.command("ping")
        sched = getattr(jobs._scheduler, "running", False) if jobs._scheduler else False
        return {"status": "ok", "mongo": "up", "database": MONGO_DB_NAME, "scheduler_running": sched}
    except Exception as e:  # noqa
        return JSONResponse(status_code=503, content={"status": "error", "mongo": "down", "detail": str(e)})


@app.post("/ingest")
def ingest(file: Optional[UploadFile] = File(None), input_path: Optional[str] = Form(None)):
   
    if file is None and not input_path:
        raise HTTPException(400, "Provide either a CSV `file` upload or an `input_path`.")
    if file is not None and file.filename:
        up = Path(UPLOAD_DIR)
        up.mkdir(parents=True, exist_ok=True)
        target = (up / Path(file.filename).name).resolve()   # same name -> same file_source -> idempotent
        with open(target, "wb") as out:
            shutil.copyfileobj(file.file, out)
        path = str(target)
    else:
        if not Path(input_path).exists():
            raise HTTPException(404, f"File not found: {input_path}")
        path = input_path
    try:
        metrics = run_pipeline(path, close_client=False)
    except Exception as e:  # noqa
        raise HTTPException(500, f"Pipeline failed: {type(e).__name__}: {e}")
    return to_jsonable({"status": "ok", "input": path, "metrics": metrics})


@app.post("/indexes")
def indexes(explain: bool = False):
    """Creates the indexes. With explain=true: drops them, runs explain before, recreates, runs explain after."""
    if explain:
        return to_jsonable({"status": "ok", **explain_before_after()})
    return {"status": "ok", "indexes": create_indexes()}


@app.get("/queries")
def queries_list():
    return {"queries": list_queries()}


@app.get("/queries/{name}")
def queries_run(name: str, request: Request):
    if name not in QUERIES:
        raise HTTPException(404, f"Unknown query '{name}'. See GET /queries.")
    return to_jsonable(run_query(name, dict(request.query_params)))


@app.get("/aggregations")
def aggregations_list():
    return {"aggregations": list_aggregations()}


@app.get("/aggregations/{name}")
def aggregations_run(name: str, request: Request):
    if name not in AGGREGATIONS:
        raise HTTPException(404, f"Unknown aggregation '{name}'. See GET /aggregations.")
    return to_jsonable(run_aggregation(name, dict(request.query_params)))


@app.post("/refresh-mv")
def refresh_mv(mv: str = "all", full: bool = False):
    """Incremental refresh of the materialized views (full=true only forces a complete rebuild)."""
    if mv != "all" and mv not in REFRESHERS:
        raise HTTPException(404, f"Unknown MV '{mv}'. Use all | {' | '.join(REFRESHERS)}")
    result = refresh_all(mv, full)
    result["preview"] = {n: read_mv(n, 5) for n in ([mv] if mv != "all" else list(REFRESHERS))}
    return to_jsonable(result)


@app.get("/mv/{name}")
def mv_read(name: str, limit: int = 20):
    if name not in REFRESHERS:
        raise HTTPException(404, f"Unknown MV '{name}'.")
    return to_jsonable(read_mv(name, limit))


@app.get("/jobs")
def jobs_list(history: int = 5):
    return to_jsonable({"jobs": jobs.get_jobs(history)})


@app.post("/jobs/{name}/run")
def jobs_run(name: str):
    if name not in jobs.JOBS:
        raise HTTPException(404, f"Unknown job '{name}'. See GET /jobs.")
    return to_jsonable(jobs.run_job(name, "manual"))
