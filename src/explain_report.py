"""Section 1 - explain('executionStats') BEFORE and AFTER creating the indexes."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.final_settings import REPORTS_DIR
from src import mongo_setup
from src.queries import QUERIES, build_find, resolve_params
from src.indexes import INDEX_BY_NAME, create_indexes, drop_indexes

DEFAULT_EXPLAIN_QUERIES = ["orders_by_customer", "orders_by_city_status", "orders_by_date_range"]


def _collect(plan, stages, indexes):
    if not isinstance(plan, dict):
        return
    if "stage" in plan:
        stages.append(plan["stage"])
    if "indexName" in plan:
        indexes.append(plan["indexName"])
    for k in ("inputStage", "outerStage", "innerStage", "queryPlan"):
        if k in plan:
            _collect(plan[k], stages, indexes)
    for s in plan.get("inputStages", []):
        _collect(s, stages, indexes)


def explain_query(name: str, resolved: dict) -> dict:
    b = build_find(name, None, resolved)
    find_cmd = {"find": b["collection"], "filter": b["filter"], "limit": b["limit"]}
    if b["sort"]:
        find_cmd["sort"] = dict(b["sort"])
    res = mongo_setup.get_db().command({"explain": find_cmd, "verbosity": "executionStats"})
    es = res["executionStats"]
    stages, idx = [], []
    _collect(res["queryPlanner"]["winningPlan"], stages, idx)
    scan = "IXSCAN" if "IXSCAN" in stages else ("COLLSCAN" if "COLLSCAN" in stages else "OTHER")
    return {
        "scan_type": scan,
        "plan_stages": " > ".join(stages),
        "index_used": idx[0] if idx else None,
        "in_memory_sort": "SORT" in stages,
        "n_returned": es.get("nReturned"),
        "keys_examined": es.get("totalKeysExamined"),
        "docs_examined": es.get("totalDocsExamined"),
        "execution_ms": es.get("executionTimeMillis"),
    }


def explain_before_after(query_names=None, params=None, write_report=True) -> dict:
    names = query_names or DEFAULT_EXPLAIN_QUERIES
    for n in names:
        if n not in QUERIES:
            raise KeyError(n)
    resolved = resolve_params(params)

    dropped = drop_indexes()
    before = {n: explain_query(n, resolved) for n in names}
    created = create_indexes()
    after = {n: explain_query(n, resolved) for n in names}

    comparison = []
    for n in names:
        idx_name = after[n]["index_used"] or QUERIES[n]["index"]
        d = INDEX_BY_NAME.get(idx_name) or INDEX_BY_NAME.get(QUERIES[n]["index"])
        comparison.append({
            "query": n,
            "params": build_find(n, None, resolved)["params_used"],
            "index": idx_name,
            "why_this_index": d["why"] if d else "see mongo_setup (unique index)",
            "before": before[n], "after": after[n],
            "impact": (f"{before[n]['scan_type']} -> {after[n]['scan_type']}; "
                       f"docs examined {before[n]['docs_examined']} -> {after[n]['docs_examined']}; "
                       f"time {before[n]['execution_ms']} ms -> {after[n]['execution_ms']} ms"),
        })
    report = {"generated_at": datetime.now(timezone.utc).isoformat(),
              "indexes_dropped_for_baseline": dropped, "indexes_created": created, "comparison": comparison}
    if write_report:
        report["report_files"] = _write_report(report)
    return report


def _write_report(report: dict) -> list:
    out = Path(REPORTS_DIR)
    out.mkdir(parents=True, exist_ok=True)
    jp, mp = out / "explain_report.json", out / "explain_report.md"
    jp.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    lines = ["# Explain report (executionStats) - before vs after indexes", "",
             f"Generated: {report['generated_at']}", ""]
    for c in report["comparison"]:
        b, a = c["before"], c["after"]
        lines += [f"## {c['query']}", f"- Params: `{c['params']}`", f"- Index: `{c['index']}`",
                  f"- Why: {c['why_this_index']}", "",
                  "| Metric | Before | After |", "|---|---|---|",
                  f"| Scan type | {b['scan_type']} | {a['scan_type']} |",
                  f"| Plan | {b['plan_stages']} | {a['plan_stages']} |",
                  f"| In-memory sort | {b['in_memory_sort']} | {a['in_memory_sort']} |",
                  f"| nReturned | {b['n_returned']} | {a['n_returned']} |",
                  f"| totalKeysExamined | {b['keys_examined']} | {a['keys_examined']} |",
                  f"| totalDocsExamined | {b['docs_examined']} | {a['docs_examined']} |",
                  f"| executionTimeMillis | {b['execution_ms']} | {a['execution_ms']} |", "",
                  f"**Impact:** {c['impact']}", ""]
    mp.write_text("\n".join(lines), encoding="utf-8")
    return [str(jp), str(mp)]


if __name__ == "__main__":
    try:
        r = explain_before_after()
        for c in r["comparison"]:
            print(c["query"], "->", c["impact"])
        print("Report:", r["report_files"])
    finally:
        mongo_setup.close_client()
