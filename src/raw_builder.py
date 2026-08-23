from datetime import datetime, timezone


def build_raw_record(row: dict, id_run: str, file_source: str,
                      number_row_source, engine_used: str) -> dict:
    
    return {
        "id_run": id_run,
        "file_source": file_source,
        "number_row_source": number_row_source,
        "at_ingested": datetime.now(timezone.utc).isoformat(),
        "engine_used": engine_used,
        "record_raw": row,
    }
