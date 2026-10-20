"""
QueueWise -- Real-Time Queue Analytics and Capacity Planning Using Apache Spark
Stage 1: Synthetic Queue Event Generator

Description:
    Generates reproducible synthetic queue events for a university help desk
    with multiple service counters. Models arrival, service-start, and
    service-completion lifecycle events with realistic queuing dynamics (FIFO,
    counter availability, peak hours, and service duration distributions).

Author: QueueWise Team (INT315 Cluster Computing Project)
"""

import argparse
import csv
import datetime
import math
import os
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class ServiceCategory:
    """Represents a specific service category offered at the university help desk."""
    queue_id: str
    service_name: str
    assigned_counters: List[str]
    mean_service_minutes: float
    service_std_minutes: float
    min_service_seconds: int
    arrival_weight: float  # Relative popularity/arrival volume


# Configuration for the University Help Desk queues and counters
DEFAULT_SERVICES: List[ServiceCategory] = [
    ServiceCategory(
        queue_id="Q_ADMISSIONS",
        service_name="Admissions & Enrollment Verification",
        assigned_counters=["COUNTER_01", "COUNTER_02"],
        mean_service_minutes=7.0,
        service_std_minutes=2.5,
        min_service_seconds=120,
        arrival_weight=0.30,
    ),
    ServiceCategory(
        queue_id="Q_FINANCIAL_AID",
        service_name="Scholarships & Fee Payments",
        assigned_counters=["COUNTER_03", "COUNTER_04"],
        mean_service_minutes=9.5,
        service_std_minutes=3.0,
        min_service_seconds=180,
        arrival_weight=0.25,
    ),
    ServiceCategory(
        queue_id="Q_ACADEMIC_RECORDS",
        service_name="Transcripts & Grade Records",
        assigned_counters=["COUNTER_05", "COUNTER_06"],
        mean_service_minutes=6.0,
        service_std_minutes=2.0,
        min_service_seconds=90,
        arrival_weight=0.25,
    ),
    ServiceCategory(
        queue_id="Q_ID_GENERAL",
        service_name="Campus ID Card & General Help",
        assigned_counters=["COUNTER_07", "COUNTER_08"],
        mean_service_minutes=4.0,
        service_std_minutes=1.5,
        min_service_seconds=60,
        arrival_weight=0.20,
    ),
]


@dataclass
class QueueEvent:
    """Represents a single event in the queue event log."""
    event_id: str
    customer_id: str
    event_type: str  # ARRIVAL, SERVICE_START, SERVICE_COMPLETION
    timestamp: datetime.datetime
    queue_id: str
    counter_id: Optional[str]
    service_type: str
    priority_level: str  # Standard, Priority
    wait_time_seconds: Optional[int]
    service_duration_seconds: Optional[int]
    is_synthetic: bool = True
    data_source: str = "SYNTHETIC_SIMULATION"

    def to_dict(self) -> Dict[str, str]:
        """Convert event to a dictionary for CSV serialization."""
        return {
            "event_id": self.event_id,
            "customer_id": self.customer_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "queue_id": self.queue_id,
            "counter_id": self.counter_id if self.counter_id else "",
            "service_type": self.service_type,
            "priority_level": self.priority_level,
            "wait_time_seconds": (
                str(self.wait_time_seconds) if self.wait_time_seconds is not None else ""
            ),
            "service_duration_seconds": (
                str(self.service_duration_seconds)
                if self.service_duration_seconds is not None
                else ""
            ),
            "is_synthetic": "TRUE" if self.is_synthetic else "FALSE",
            "data_source": self.data_source,
        }


def get_peak_arrival_multiplier(sim_time: datetime.datetime, desk_open_time: datetime.datetime) -> float:
    """
    Simulate realistic university help desk arrival rates:
    - Moderate flow early morning (09:00 - 10:30)
    - High rush mid-day / lunch transition (11:00 - 14:00)
    - Steady afternoon (14:00 - 16:00)
    - Tapering off before closing (16:00 - 17:00)
    """
    hour = sim_time.hour + (sim_time.minute / 60.0)
    if 11.0 <= hour <= 13.5:
        return 1.8  # Rush hour
    elif 10.0 <= hour < 11.0 or 13.5 < hour <= 15.0:
        return 1.2  # Normal busy
    elif 9.0 <= hour < 10.0:
        return 0.9  # Opening ramp-up
    else:
        return 0.7  # Late afternoon wind-down


def sample_service_duration(service: ServiceCategory, rng: random.Random) -> int:
    """Sample service duration using a Gaussian distribution, bounded at min_service_seconds."""
    raw_seconds = rng.gauss(
        mu=service.mean_service_minutes * 60,
        sigma=service.service_std_minutes * 60,
    )
    duration = int(max(service.min_service_seconds, round(raw_seconds)))
    return duration


def simulate_university_helpdesk(
    num_customers: int = 500,
    seed: int = 42,
    start_date_str: str = "2026-10-19 09:00:00",
    services: Optional[List[ServiceCategory]] = None,
) -> List[QueueEvent]:
    """
    Executes a discrete queue simulation for a university help desk.

    Ensures:
    1. Reproducibility using a dedicated random number generator.
    2. Strict FIFO processing per queue with realistic multi-counter allocation.
    3. Physically valid non-overlapping service times on individual counters.
    4. Chronological consistency: arrival <= service_start < service_completion.
    """
    if services is None:
        services = DEFAULT_SERVICES

    rng = random.Random(seed)

    start_datetime = datetime.datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")

    # Track counter availability: counter_id -> datetime when free
    counter_free_time: Dict[str, datetime.datetime] = {}
    for svc in services:
        for counter in svc.assigned_counters:
            counter_free_time[counter] = start_datetime

    # Service selection weights
    weights = [s.arrival_weight for s in services]

    # Generate customer arrivals across time
    current_arrival_time = start_datetime
    raw_events: List[Tuple[datetime.datetime, int, QueueEvent]] = []

    # Priority distribution: ~15% Priority (e.g. differently abled or scheduled appointment), 85% Standard
    priority_options = ["Standard", "Priority"]
    priority_weights = [0.85, 0.15]

    for cust_idx in range(1, num_customers + 1):
        customer_id = f"CUST_{cust_idx:04d}"

        # Select service category
        service: ServiceCategory = rng.choices(services, weights=weights, k=1)[0]

        # Inter-arrival delay based on Poisson arrival process with peak multipliers
        multiplier = get_peak_arrival_multiplier(current_arrival_time, start_datetime)
        base_mean_arrival_interval = 45.0  # seconds between arrivals on average across all queues
        adjusted_interval = base_mean_arrival_interval / multiplier
        delta_seconds = rng.expovariate(1.0 / adjusted_interval)
        current_arrival_time += datetime.timedelta(seconds=max(5, round(delta_seconds)))

        arrival_timestamp = current_arrival_time
        priority = rng.choices(priority_options, weights=priority_weights, k=1)[0]

        # 1. Arrival Event
        arrival_evt = QueueEvent(
            event_id="",  # Assigned later in chronological order
            customer_id=customer_id,
            event_type="ARRIVAL",
            timestamp=arrival_timestamp,
            queue_id=service.queue_id,
            counter_id=None,
            service_type=service.service_name,
            priority_level=priority,
            wait_time_seconds=None,
            service_duration_seconds=None,
            is_synthetic=True,
            data_source="SYNTHETIC_SIMULATION",
        )
        # Event type order precedence for ties: SERVICE_COMPLETION (1), ARRIVAL (2), SERVICE_START (3)
        raw_events.append((arrival_timestamp, 2, arrival_evt))

        # Counter scheduling: pick the counter in this service queue that becomes free earliest
        available_counters = service.assigned_counters
        chosen_counter = min(available_counters, key=lambda c: counter_free_time[c])
        earliest_free = counter_free_time[chosen_counter]

        # Service starts either when customer arrives or when counter finishes previous customer
        service_start_time = max(arrival_timestamp, earliest_free)

        # Calculate wait time
        wait_seconds = int((service_start_time - arrival_timestamp).total_seconds())

        # Sample service duration
        duration_seconds = sample_service_duration(service, rng)

        # Service completion time
        completion_time = service_start_time + datetime.timedelta(seconds=duration_seconds)

        # Update counter free time
        counter_free_time[chosen_counter] = completion_time

        # 2. Service-Start Event
        start_evt = QueueEvent(
            event_id="",
            customer_id=customer_id,
            event_type="SERVICE_START",
            timestamp=service_start_time,
            queue_id=service.queue_id,
            counter_id=chosen_counter,
            service_type=service.service_name,
            priority_level=priority,
            wait_time_seconds=wait_seconds,
            service_duration_seconds=None,
            is_synthetic=True,
            data_source="SYNTHETIC_SIMULATION",
        )
        raw_events.append((service_start_time, 3, start_evt))

        # 3. Service-Completion Event
        comp_evt = QueueEvent(
            event_id="",
            customer_id=customer_id,
            event_type="SERVICE_COMPLETION",
            timestamp=completion_time,
            queue_id=service.queue_id,
            counter_id=chosen_counter,
            service_type=service.service_name,
            priority_level=priority,
            wait_time_seconds=wait_seconds,
            service_duration_seconds=duration_seconds,
            is_synthetic=True,
            data_source="SYNTHETIC_SIMULATION",
        )
        raw_events.append((completion_time, 1, comp_evt))

    # Sort all events chronologically
    # Tie-breaking: timestamp asc, event priority order asc (COMPLETION before START before ARRIVAL)
    raw_events.sort(key=lambda x: (x[0], x[1]))

    # Assign sequential global event IDs to maintain total ordering
    ordered_events: List[QueueEvent] = []
    for seq, (_, _, evt) in enumerate(raw_events, start=1):
        evt.event_id = f"EVT_{seq:06d}"
        ordered_events.append(evt)

    return ordered_events


def save_events_to_csv(events: List[QueueEvent], output_path: str) -> None:
    """Save queue events to a CSV file, ensuring output directory exists."""
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    fieldnames = [
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

    with open(output_path, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for evt in events:
            writer.writerow(evt.to_dict())


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments for the synthetic generator."""
    parser = argparse.ArgumentParser(
        description="QueueWise Stage 1: Synthetic Queue Event Generator for University Help Desk"
    )
    parser.add_argument(
        "--num-customers",
        type=int,
        default=500,
        help="Number of simulated customers (default: 500)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible event generation (default: 42)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=os.path.join("data", "queue_events.csv"),
        help="Output CSV path (default: data/queue_events.csv)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-10-19 09:00:00",
        help="Simulation starting timestamp YYYY-MM-DD HH:MM:SS (default: 2026-10-19 09:00:00)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Automatically trigger dataset validation after generation",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    print("================================================================")
    print(" QueueWise -- Synthetic Queue Event Generator (Stage 1)")
    print("================================================================")
    print(f"[*] Configuration:")
    print(f"    - Random Seed:            {args.seed}")
    print(f"    - Simulated Customers:    {args.num_customers}")
    print(f"    - Expected Event Count:   {args.num_customers * 3}")
    print(f"    - Start Timestamp:        {args.start_date}")
    print(f"    - Output Destination:     {args.output}")
    print(f"    - Synthetic Flag:         TRUE (all records labeled synthetic)")
    print("----------------------------------------------------------------")

    print("[*] Simulating university help desk queue dynamics...")
    events = simulate_university_helpdesk(
        num_customers=args.num_customers,
        seed=args.seed,
        start_date_str=args.start_date,
    )

    print(f"[*] Generated {len(events)} events across {args.num_customers} customers.")
    print(f"[*] Writing dataset to '{args.output}'...")
    save_events_to_csv(events, args.output)

    file_size_kb = os.path.getsize(args.output) / 1024
    print(f"[+] Dataset successfully created: {args.output} ({file_size_kb:.1f} KB)")
    print(f"[+] Date range: {events[0].timestamp} --> {events[-1].timestamp}")

    if args.validate:
        print("\n[*] Running integrated validation suite...")
        import validate_events
        is_valid = validate_events.run_validation(args.output)
        if not is_valid:
            print("[!] Validation FAILED. Check errors above.")
            exit(1)
        else:
            print("[+] All validation checks PASSED successfully.")

    print("================================================================")


if __name__ == "__main__":
    main()
