"""
quality_gates.py
----------------
Reusable validation functions used by ingestion scripts, Spark jobs, and
the final report. Import these functions wherever you need data quality checks.

Usage:
    from validation.quality_gates import check_row_count_ratio, check_error_rate, log_gate_result
"""


def check_row_count_ratio(source_count, target_count, min_ratio=0.95):
    """
    Check that target_count is at least min_ratio of source_count.

    For example, if source has 500 rows and staging has 480, the ratio is 0.96.
    Used to catch unexpected data loss between pipeline stages.

    Returns:
        (passed: bool, message: str)
    """
    if source_count == 0:
        return False, "source_count is 0 — cannot calculate ratio"

    ratio = target_count / source_count
    passed = ratio >= min_ratio
    message = f"ratio={ratio:.2f} (min={min_ratio}) — source={source_count}, target={target_count}"
    return passed, message


def check_error_rate(total_rows, error_rows, max_rate=0.10):
    """
    Check that the fraction of error rows does not exceed max_rate.

    For example, if 50 out of 500 rows failed, error_rate = 0.10.
    A high error rate may indicate a schema change or data quality regression.

    Returns:
        (passed: bool, message: str)
    """
    if total_rows == 0:
        return False, "total_rows is 0 — cannot calculate error rate"

    rate = error_rows / total_rows
    passed = rate <= max_rate
    message = f"error_rate={rate:.2f} (max={max_rate}) — errors={error_rows}, total={total_rows}"
    return passed, message


def log_gate_result(gate_name, table, passed, message):
    """
    Print a structured log line for a quality gate result.

    Example output:
        [QG3] bookings: PASSED — ratio=0.96 (min=0.95) — source=500, target=480
    """
    status = "PASSED" if passed else "FAILED"
    print(f"[{gate_name}] {table}: {status} — {message}")
