import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import RESULTS_JSON_PATH


def load_metrics_history() -> list:
    
    path = Path(RESULTS_JSON_PATH)
    if not path.exists():
        return []

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return []
        data = json.loads(content)
        if not isinstance(data, list):
            print(f"[metrics] Warning: {path} did not contain a JSON array - starting a new history.")
            return []
        return data
    except json.JSONDecodeError as e:
        print(f"[metrics] Warning: {path} could not be parsed ({e}) - starting a new history.")
        return []


def save_metrics(run_metrics: dict) -> None:
    
    path = Path(RESULTS_JSON_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)

    history = load_metrics_history()
    history.append(run_metrics)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False, default=str)

    print(f"[metrics] Run metrics for id_run={run_metrics.get('id_run')} appended to {path} "
          f"(history now has {len(history)} run(s)).")
