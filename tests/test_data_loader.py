"""
Unit tests for data_loader.py module in QueueWise.
INT315 Cluster Computing Capstone Project.
"""

import os
import unittest
import pandas as pd

from data_loader import (
    QUEUE_COLORS,
    QUEUE_NAMES,
    EXPECTED_OUTPUT_FILES,
    get_directory_mtimes,
    load_analytical_datasets,
    load_raw_queue_events,
    compute_executive_kpis,
    compute_custom_threshold_metrics,
)


class TestDataLoader(unittest.TestCase):
    def test_constants_and_mappings(self):
        self.assertEqual(len(QUEUE_COLORS), 4)
        self.assertEqual(len(QUEUE_NAMES), 4)
        self.assertIn("Q_FINANCIAL_AID", QUEUE_COLORS)
        self.assertIn("Q_FINANCIAL_AID", QUEUE_NAMES)
        self.assertEqual(len(EXPECTED_OUTPUT_FILES), 11)

    def test_get_directory_mtimes(self):
        mtimes = get_directory_mtimes("outputs")
        self.assertIsInstance(mtimes, tuple)
        self.assertGreater(len(mtimes), 0)
        for fname, mtime in mtimes:
            self.assertTrue(fname.endswith(".csv") or fname.endswith(".json"))
            self.assertIsInstance(mtime, float)

    def test_load_analytical_datasets(self):
        dfs, missing = load_analytical_datasets("outputs")
        self.assertEqual(len(dfs), 11)
        # Verify critical datasets are present and non-empty
        for key in ["queue_metrics", "sla_breaches", "counter_utilization", "capacity_comparison", "reallocation_metrics"]:
            self.assertIsNotNone(dfs.get(key), f"Expected dataset {key} to be loaded")
            self.assertIsInstance(dfs[key], pd.DataFrame)
            self.assertFalse(dfs[key].empty)

    def test_load_raw_queue_events(self):
        raw_df = load_raw_queue_events()
        self.assertIsNotNone(raw_df)
        self.assertIsInstance(raw_df, pd.DataFrame)
        self.assertEqual(len(raw_df), 1500)  # 500 customers * 3 events

    def test_compute_executive_kpis(self):
        dfs, _ = load_analytical_datasets("outputs")
        kpis = compute_executive_kpis(dfs["queue_metrics"], dfs["sla_breaches"])
        self.assertEqual(kpis["total_customers"], 500)
        self.assertEqual(kpis["tot_served"], 500)
        self.assertAlmostEqual(kpis["weighted_avg_wait"], 85.8, places=1)
        self.assertAlmostEqual(kpis["breach_rate_30m"], 57.6, places=1)
        self.assertEqual(kpis["tot_breached_30m"], 288)
        self.assertEqual(kpis["bottleneck_id"], "Q_FINANCIAL_AID")
        self.assertEqual(kpis["bottleneck_name"], "Financial Aid & Scholarships")
        self.assertAlmostEqual(kpis["bottleneck_wait"], 152.5, places=1)

    def test_compute_custom_threshold_metrics(self):
        raw_df = load_raw_queue_events()
        dfs, _ = load_analytical_datasets("outputs")
        all_queues = list(QUEUE_NAMES.keys())

        # 30 minute benchmark across all queues
        total, breached, rate, info = compute_custom_threshold_metrics(
            raw_df, dfs["sla_breaches"], all_queues, 30
        )
        self.assertEqual(total, 500)
        self.assertEqual(breached, 288)
        self.assertEqual(rate, 57.6)
        self.assertIn("288 of 500", info)

        # 60 minute benchmark across all queues
        total_60, breached_60, rate_60, info_60 = compute_custom_threshold_metrics(
            raw_df, dfs["sla_breaches"], all_queues, 60
        )
        self.assertEqual(total_60, 500)
        self.assertLess(breached_60, breached)


if __name__ == "__main__":
    unittest.main()
