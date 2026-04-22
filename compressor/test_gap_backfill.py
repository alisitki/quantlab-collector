#!/usr/bin/env python3

from run_gap_backfill import classify_missing_partition, should_overwrite_retry


def test_missing_state_is_retryable():
    bucket, reason = classify_missing_partition(None)
    assert bucket == "retryable"
    assert reason == "missing_state"


def test_partial_is_hard_failure():
    bucket, reason = classify_missing_partition({"status": "partial", "day_quality": "PARTIAL"})
    assert bucket == "hard_failure"
    assert reason == "day_quality:partial"


def test_quarantine_other_is_retryable():
    bucket, reason = classify_missing_partition({"status": "quarantine", "error_type": "OTHER"})
    assert bucket == "retryable"
    assert reason == "state_retryable"


def test_invalid_thrift_is_hard_failure():
    bucket, reason = classify_missing_partition({"status": "quarantine", "error": "Couldn't deserialize thrift: invalid TType"})
    assert bucket == "hard_failure"
    assert reason == "error_text:corrupt_or_dict_conflict"


def test_success_gap_uses_overwrite():
    assert should_overwrite_retry("success") is True
    assert should_overwrite_retry("skipped") is True
    assert should_overwrite_retry("quarantine") is False


if __name__ == "__main__":
    test_missing_state_is_retryable()
    test_partial_is_hard_failure()
    test_quarantine_other_is_retryable()
    test_invalid_thrift_is_hard_failure()
    test_success_gap_uses_overwrite()
    print("ok")
