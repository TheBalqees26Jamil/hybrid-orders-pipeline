"""Calls every required endpoint and prints PASS/FAIL.
Usage: python -m src.smoke_test [--base http://127.0.0.1:8000] [--input data/orders_sample.csv]"""
import argparse
import json
import urllib.error
import urllib.request


def call(method, url, data=None, headers=None):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=3600) as r:
            return r.status, json.loads(r.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode("utf-8", "ignore")[:200]}
    except Exception as e:  # noqa
        return 0, {"error": str(e)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--input", default=None, help="CSV path on the SERVER; if omitted /ingest is skipped")
    a = ap.parse_args()
    b = a.base.rstrip("/")
    results = []

    def check(label, status, ok):
        results.append(ok)
        print(f"[{'PASS' if ok else 'FAIL'}] {label} (HTTP {status})")

    s, _ = call("GET", b + "/health");               check("GET /health", s, s == 200)
    if a.input:
        body = f"input_path={a.input}".encode()
        s, _ = call("POST", b + "/ingest", body, {"Content-Type": "application/x-www-form-urlencoded"})
        check("POST /ingest", s, s == 200)
    s, r = call("POST", b + "/indexes");             check("POST /indexes", s, s == 200)
    s, r = call("GET", b + "/queries");              check("GET /queries", s, s == 200 and len(r.get("queries", [])) >= 5)
    for q in [x["name"] for x in r.get("queries", [])]:
        s, rr = call("GET", f"{b}/queries/{q}");     check(f"GET /queries/{q}", s, s == 200)
    s, r = call("GET", b + "/aggregations");         check("GET /aggregations", s, s == 200 and len(r.get("aggregations", [])) >= 5)
    for g in [x["name"] for x in r.get("aggregations", [])]:
        s, rr = call("GET", f"{b}/aggregations/{g}"); check(f"GET /aggregations/{g}", s, s == 200 and rr.get("count", 0) >= 0)
    s, _ = call("POST", b + "/refresh-mv");          check("POST /refresh-mv", s, s == 200)
    s, r = call("GET", b + "/jobs");                 check("GET /jobs", s, s == 200 and len(r.get("jobs", [])) >= 2)
    for j in [x["name"] for x in r.get("jobs", [])]:
        s, rr = call("POST", f"{b}/jobs/{j}/run");   check(f"POST /jobs/{j}/run", s, s == 200 and rr.get("status") == "success")
    s, _ = call("GET", b + "/docs");                 check("GET /docs (Swagger)", s, s == 200)
    print(f"\n{sum(results)}/{len(results)} checks passed")


if __name__ == "__main__":
    main()
