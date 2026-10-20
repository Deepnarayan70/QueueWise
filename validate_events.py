"""
QueueWise -- Real-Time Queue Analytics and Capacity Planning Using Apache Spark
Stage 1: Queue Event Dataset Validator

Description:
    Validates the integrity, schema compliance, temporal consistency, and
    synthetic labeling of the generated queue events dataset.

Author: QueueWise Team (INT315 Cluster Computing Project)
"""

import argparse
import csv
import datetime
import os
import sys
from collections import defaultdict
from typing import Dict, List, Set, Tuple


REQUIRED_COLUMNS = [
    "event_id",
    "customer_id",
    "event_type",
    "timestamp",
    "queue_id",
    "counter_id",
    "service_type",
    "priority_level",
    "wait_time_seconds",
    "service_duration_seconds",
    "is_synthetic",
    "data_source",
]

VALID_EVENT_TYPES = {"ARRIVAL", "SERVICE_START", "SERVICE_COMPLETION"}


class ValidationResult:
    """Collects check outcomes, errors, warnings, and summary statistics."""

    def __init__(self) -> None:
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.passed_checks: List[str] = []
        self.stats: Dict[str, object] = {}

    def add_pass(self, check_name: str) -> None:
        self.passed_checks.append(check_name)

    def add_error(self, message: str) -> None:
        self.errors.append(message)

    def add_warning(self, message: str) -> None:
        self.warnings.append(message)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


def run_validation(csv_path: str, verbose: bool = True) -> bool:
    """
    Executes a comprehensive validation suite against the queue events CSV file.

    Returns:
        True if all critical checks pass, False otherwise.
    """
    result = ValidationResult()

    # Check 1: File existence and size
    if not os.path.exists(csv_path):
        result.add_error(f"File not found: {csv_path}")
        _print_report(result, csv_path)
        return False

    file_size = os.path.getsize(csv_path)
    if file_size == 0:
        result.add_error(f"File is empty: {csv_path}")
        _print_report(result, csv_path)
        return False

    result.add_pass(f"File existence and non-zero size ({file_size / 1024:.1f} KB)")

    # Read and parse CSV
    rows: List[Dict[str, str]] = []
    with open(csv_path, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []

        # Check 2: Schema validation
        missing_cols = [col for col in REQUIRED_COLUMNS if col not in header]
        if missing_cols:
            result.add_error(f"Missing required columns in header: {missing_cols}")
        else:
            result.add_pass(f"Schema compliance (all {len(REQUIRED_COLUMNS)} required columns present)")

        for row_idx, r in enumerate(reader, start=2):  # line 2 is first data row
            rows.append(r)

    if not rows:
        result.add_error("Dataset contains zero data rows after header.")
        _print_report(result, csv_path)
        return False

    # Check 3: Synthetic labeling audit
    non_synthetic_count = 0
    non_synthetic_source_count = 0
    for r in rows:
        if r.get("is_synthetic", "").upper() != "TRUE":
            non_synthetic_count += 1
        if r.get("data_source", "") != "SYNTHETIC_SIMULATION":
            non_synthetic_source_count += 1

    if non_synthetic_count > 0:
        result.add_error(
            f"Synthetic integrity violation: {non_synthetic_count} rows do not have is_synthetic=TRUE."
        )
    elif non_synthetic_source_count > 0:
        result.add_error(
            f"Synthetic integrity violation: {non_synthetic_source_count} rows do not have data_source='SYNTHETIC_SIMULATION'."
        )
    else:
        result.add_pass("Synthetic data audit (100% of rows explicitly labeled as synthetic)")

    # Check 4: Event types validity & Event ID uniqueness
    invalid_event_types: Set[str] = set()
    event_ids: Set[str] = set()
    duplicate_event_ids: Set[str] = set()

    for r in rows:
        etype = r.get("event_type", "")
        if etype not in VALID_EVENT_TYPES:
            invalid_event_types.add(etype)

        eid = r.get("event_id", "")
        if not eid:
            result.add_error("Encountered empty or blank event_id.")
        elif eid in event_ids:
            duplicate_event_ids.add(eid)
        else:
            event_ids.add(eid)

    if invalid_event_types:
        result.add_error(f"Invalid event types detected: {invalid_event_types}")
    else:
        result.add_pass(f"Event type validation (strictly {VALID_EVENT_TYPES})")

    if duplicate_event_ids:
        result.add_error(f"Duplicate event IDs detected (e.g., {list(duplicate_event_ids)[:5]})")
    else:
        result.add_pass(f"Event ID uniqueness ({len(event_ids)} distinct unique IDs)")

    # Check 5: Chronological ordering
    parsed_timestamps: List[datetime.datetime] = []
    timestamp_format_errors = 0
    out_of_order_count = 0

    for idx, r in enumerate(rows):
        ts_str = r.get("timestamp", "")
        try:
            ts = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            parsed_timestamps.append(ts)
            if idx > 0 and ts < parsed_timestamps[idx - 1]:
                out_of_order_count += 1
        except ValueError:
            timestamp_format_errors += 1

    if timestamp_format_errors > 0:
        result.add_error(f"Timestamp parsing errors on {timestamp_format_errors} rows (expected 'YYYY-MM-DD HH:MM:SS').")
    elif out_of_order_count > 0:
        result.add_error(f"Chronological ordering violation: {out_of_order_count} rows are out of order.")
    else:
        result.add_pass("Chronological total ordering (monotonic timestamp progression)")

    # Check 6: Customer lifecycle consistency & Queuing logic
    # Group events by customer
    customer_events: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)
    for r in rows:
        cid = r.get("customer_id", "")
        etype = r.get("event_type", "")
        if not cid:
            result.add_error("Found row with blank customer_id.")
            continue
        if etype in customer_events[cid]:
            result.add_error(f"Customer {cid} has duplicate {etype} event.")
        customer_events[cid][etype] = r

    lifecycle_incomplete = 0
    temporal_lifecycle_violations = 0
    counter_mismatches = 0
    queue_mismatches = 0
    negative_duration_errors = 0

    counter_service_intervals: Dict[str, List[Tuple[datetime.datetime, datetime.datetime, str]]] = defaultdict(list)

    total_wait_seconds = 0
    total_service_seconds = 0
    completed_customers = 0

    for cid, evts in customer_events.items():
        arr = evts.get("ARRIVAL")
        start = evts.get("SERVICE_START")
        comp = evts.get("SERVICE_COMPLETION")

        if not (arr and start and comp):
            lifecycle_incomplete += 1
            continue

        completed_customers += 1

        t_arr = datetime.datetime.strptime(arr["timestamp"], "%Y-%m-%d %H:%M:%S")
        t_start = datetime.datetime.strptime(start["timestamp"], "%Y-%m-%d %H:%M:%S")
        t_comp = datetime.datetime.strptime(comp["timestamp"], "%Y-%m-%d %H:%M:%S")

        # Temporal logic: ARRIVAL <= SERVICE_START < SERVICE_COMPLETION
        if not (t_arr <= t_start < t_comp):
            temporal_lifecycle_violations += 1

        # Queue consistency
        if not (arr["queue_id"] == start["queue_id"] == comp["queue_id"]):
            queue_mismatches += 1

        # Counter consistency
        if start["counter_id"] != comp["counter_id"] or not start["counter_id"]:
            counter_mismatches += 1
        else:
            counter_service_intervals[start["counter_id"]].append((t_start, t_comp, cid))

        # Numeric wait & service duration checks
        try:
            wait_s = int(start["wait_time_seconds"])
            dur_s = int(comp["service_duration_seconds"])

            calc_wait = int((t_start - t_arr).total_seconds())
            calc_dur = int((t_comp - t_start).total_seconds())

            if wait_s != calc_wait:
                result.add_error(f"Customer {cid} wait_time_seconds ({wait_s}) does not match timestamp difference ({calc_wait}).")
            if dur_s != calc_dur:
                result.add_error(f"Customer {cid} service_duration_seconds ({dur_s}) does not match timestamp difference ({calc_dur}).")

            if wait_s < 0 or dur_s <= 0:
                negative_duration_errors += 1

            total_wait_seconds += wait_s
            total_service_seconds += dur_s
        except (ValueError, TypeError):
            negative_duration_errors += 1

    if lifecycle_incomplete > 0:
        result.add_error(f"Lifecycle incompleteness: {lifecycle_incomplete} customers have missing events.")
    else:
        result.add_pass(f"Customer lifecycle completeness ({len(customer_events)} customers have ARRIVAL, START, COMPLETION)")

    if temporal_lifecycle_violations > 0:
        result.add_error(f"Temporal lifecycle violations: {temporal_lifecycle_violations} customers have t_arr <= t_start < t_comp violated.")
    else:
        result.add_pass("Temporal ordering logic per customer (Arrival <= Service Start < Service Completion)")

    if queue_mismatches > 0:
        result.add_error(f"Queue ID mismatches across customer lifecycle: {queue_mismatches} cases.")
    else:
        result.add_pass("Queue ID integrity (identical queue across customer events)")

    if counter_mismatches > 0:
        result.add_error(f"Counter ID mismatches: {counter_mismatches} cases where start and completion counters differ.")
    else:
        result.add_pass("Counter ID integrity (consistent service counter assignment)")

    if negative_duration_errors > 0:
        result.add_error(f"Invalid wait or service duration numeric values: {negative_duration_errors} cases.")
    else:
        result.add_pass("Wait time and service duration numerical sanity (wait >= 0, duration > 0)")

    # Check 7: Counter concurrency check (A counter must serve only one customer at a time)
    concurrency_overlaps = 0
    for counter_id, intervals in counter_service_intervals.items():
        # Sort intervals by start time
        intervals.sort(key=lambda x: x[0])
        for i in range(len(intervals) - 1):
            cur_start, cur_end, cur_cust = intervals[i]
            next_start, next_end, next_cust = intervals[i + 1]
            if cur_end > next_start:
                concurrency_overlaps += 1
                result.add_error(
                    f"Counter overlap on {counter_id}: {cur_cust} (ended {cur_end}) overlaps with {next_cust} (started {next_start})"
                )

    if concurrency_overlaps == 0:
        result.add_pass(
            f"Physical counter concurrency constraints ({len(counter_service_intervals)} counters validated, zero overlaps)"
        )

    # Collect statistics
    if completed_customers > 0:
        result.stats["total_rows"] = len(rows)
        result.stats["total_customers"] = len(customer_events)
        result.stats["avg_wait_minutes"] = round((total_wait_seconds / completed_customers) / 60.0, 2)
        result.stats["avg_service_minutes"] = round((total_service_seconds / completed_customers) / 60.0, 2)
        result.stats["earliest_event"] = parsed_timestamps[0] if parsed_timestamps else "N/A"
        result.stats["latest_event"] = parsed_timestamps[-1] if parsed_timestamps else "N/A"
        result.stats["counters_active"] = len(counter_service_intervals)

    if verbose:
        _print_report(result, csv_path)

    return result.is_valid


def _print_report(result: ValidationResult, csv_path: str) -> None:
    """Print an attractive summary report of validation checks."""
    print("\n================================================================")
    print(" QueueWise Stage 1: Dataset Validation Report")
    print("================================================================")
    print(f"Target File: {csv_path}")
    print("----------------------------------------------------------------")

    print(f"[+] Passed Checks ({len(result.passed_checks)}):")
    for check in result.passed_checks:
        print(f"    [OK] {check}")

    if result.warnings:
        print(f"\n[!] Warnings ({len(result.warnings)}):")
        for warn in result.warnings:
            print(f"    [WARN] {warn}")

    if result.errors:
        print(f"\n[-] Errors ({len(result.errors)}):")
        for err in result.errors:
            print(f"    [FAIL] {err}")
    else:
        print("\n[-] Errors: None (0)")

    if result.stats:
        print("----------------------------------------------------------------")
        print(" Summary Statistics:")
        print(f"    - Total Events:              {result.stats.get('total_rows')}")
        print(f"    - Distinct Customers:        {result.stats.get('total_customers')}")
        print(f"    - Active Counters:           {result.stats.get('counters_active')}")
        print(f"    - Average Wait Time:         {result.stats.get('avg_wait_minutes')} minutes")
        print(f"    - Average Service Duration:  {result.stats.get('avg_service_minutes')} minutes")
        print(f"    - Simulation Span:           {result.stats.get('earliest_event')} to {result.stats.get('latest_event')}")

    print("----------------------------------------------------------------")
    if result.is_valid:
        print(" OVERALL STATUS: [PASS] All integrity checks passed successfully.")
    else:
        print(" OVERALL STATUS: [FAIL] Dataset has validation issues.")
    print("================================================================\n")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="QueueWise Stage 1: Queue Event Dataset Validator"
    )
    parser.add_argument(
        "--file",
        type=str,
        default=os.path.join("data", "queue_events.csv"),
        help="Path to the queue events CSV file (default: data/queue_events.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    is_valid = run_validation(args.file, verbose=True)
    if not is_valid:
        sys.exit(1)


if __name__ == "__main__":
    main()
