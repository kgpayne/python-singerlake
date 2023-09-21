from datetime import datetime


def get_time_extracted(record: dict) -> datetime:
    """Return the time extracted from a record."""
    time_extracted = record.get("time_extracted") or record.get("record", {}).get(
        "_sdc_extracted_at"
    )
    if not time_extracted:
        raise ValueError("Record does not contain time_extracted")

    return datetime.fromisoformat(time_extracted)
