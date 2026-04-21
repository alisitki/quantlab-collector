"""
Shared state semantics for compaction planner and runner.
"""

from typing import Any, Dict, Iterable, Optional

PARTIAL_DAY_RETRY_ERROR = "Partial day data, retry expected"

COMPLETE_STATUSES = frozenset({"success", "quarantine", "skipped"})
INCOMPLETE_STATUSES = frozenset(
    {
        "partial",
        "locked",
        "failed",
        "download_failed",
        "aborted",
        "no_files",
        "in_progress",
        "stalled",
        "unknown",
    }
)


def get_entry_day_quality(entry: Optional[Dict[str, Any]]) -> Optional[str]:
    if not entry:
        return None
    return entry.get("day_quality") or entry.get("day_quality_post")


def infer_counts_as_complete(
    status: Optional[str],
    *,
    day_quality: Optional[str] = None,
    error: Optional[str] = None,
    skip_reason: Optional[str] = None,
) -> bool:
    if status in {"success", "quarantine"}:
        return True
    if status == "skipped":
        if day_quality == "PARTIAL":
            return False
        if str(error or "").strip() == PARTIAL_DAY_RETRY_ERROR:
            return False
        if skip_reason == "partial_day":
            return False
        return True
    return False


def entry_counts_as_complete(entry: Optional[Dict[str, Any]]) -> bool:
    if not entry:
        return False

    explicit = entry.get("counts_as_complete")
    if isinstance(explicit, bool):
        return explicit

    return infer_counts_as_complete(
        entry.get("status"),
        day_quality=get_entry_day_quality(entry),
        error=entry.get("error"),
        skip_reason=entry.get("skip_reason"),
    )


def partition_results_are_complete(results: Iterable[Dict[str, Any]]) -> bool:
    results = list(results)
    return bool(results) and all(entry_counts_as_complete(result) for result in results)
