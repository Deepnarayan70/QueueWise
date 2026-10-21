"""
QueueWise Flask REST API & Frontend Test Suite
Automated functional verification of REST endpoints, query filters,
data integrity, dataset previews, downloads, security guards, and view routes.
"""

import unittest
from backend.app import create_app


class TestQueueWiseFlaskAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = create_app({"TESTING": True})
        cls.client = cls.app.test_client()

    def test_health_check(self):
        res = self.client.get("/api/health")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["data"]["status"], "healthy")
        self.assertEqual(data["data"]["datasets_loaded"], 11)
        self.assertTrue(data["data"]["raw_events_loaded"])
        self.assertEqual(data["data"]["service_workload_seconds"], 204658)

    def test_overview_endpoint(self):
        res = self.client.get("/api/overview")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]

        # Validate KPIs
        kpis = data["kpis"]
        self.assertEqual(kpis["total_customers"], 500)
        self.assertAlmostEqual(kpis["weighted_avg_wait"], 85.8, places=1)
        self.assertAlmostEqual(kpis["breach_rate_30m"], 57.6, places=1)
        self.assertEqual(kpis["tot_breached_30m"], 288)
        self.assertEqual(kpis["bottleneck_id"], "Q_FINANCIAL_AID")
        self.assertEqual(kpis["bottleneck_name"], "Financial Aid & Scholarships")
        self.assertAlmostEqual(kpis["bottleneck_wait"], 152.5, places=1)

        # Validate benchmark items & hourly arrivals
        self.assertEqual(len(data["benchmark"]["items"]), 4)
        self.assertEqual(data["benchmark"]["sla_target_minutes"], 30.0)
        self.assertEqual(len(data["hourly_arrivals"]), 5)
        self.assertGreaterEqual(len(data["alerts"]), 3)

    def test_queues_list(self):
        res = self.client.get("/api/queues")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]
        self.assertEqual(len(data), 4)
        qids = [q["queue_id"] for q in data]
        self.assertIn("Q_FINANCIAL_AID", qids)
        self.assertIn("Q_ADMISSIONS", qids)
        self.assertIn("Q_ACADEMIC_RECORDS", qids)
        self.assertIn("Q_ID_GENERAL", qids)

    def test_queue_performance_default(self):
        res = self.client.get("/api/queues/performance")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]
        self.assertEqual(len(data["metrics"]), 4)
        self.assertEqual(data["threshold_stats"]["total_customers"], 500)
        self.assertEqual(data["threshold_stats"]["breached_customers"], 288)
        self.assertAlmostEqual(data["threshold_stats"]["breach_rate_pct"], 57.6, places=1)
        self.assertEqual(len(data["wait_distribution"]), 4)
        self.assertEqual(len(data["ml_risk"]), 4)
        self.assertGreaterEqual(len(data["ml_metrics"]), 2)

    def test_queue_performance_filtered(self):
        res = self.client.get("/api/queues/performance?queues=Q_FINANCIAL_AID,Q_ADMISSIONS&threshold=45")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]
        self.assertEqual(len(data["metrics"]), 2)
        qids = [m["queue_id"] for m in data["metrics"]]
        self.assertEqual(set(qids), {"Q_FINANCIAL_AID", "Q_ADMISSIONS"})
        self.assertEqual(data["threshold_stats"]["threshold_minutes"], 45)

    def test_queue_performance_invalid_threshold(self):
        res = self.client.get("/api/queues/performance?threshold=-5")
        self.assertEqual(res.status_code, 400)
        res_str = self.client.get("/api/queues/performance?threshold=abc")
        self.assertEqual(res_str.status_code, 400)

    def test_queue_performance_invalid_queues(self):
        res = self.client.get("/api/queues/performance?queues=INVALID_QUEUE_ID")
        self.assertEqual(res.status_code, 400)

    def test_counter_utilization(self):
        res = self.client.get("/api/counters/utilization")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]
        self.assertEqual(data["operating_window_seconds"], 37311)
        self.assertEqual(data["target_threshold_pct"], 75.0)
        self.assertEqual(data["critical_threshold_pct"], 90.0)
        self.assertEqual(len(data["counters"]), 8)

        # Check first counter
        c01 = next(c for c in data["counters"] if c["counter_id"] == "COUNTER_01")
        self.assertEqual(c01["queue_id"], "Q_ADMISSIONS")
        self.assertAlmostEqual(c01["utilization_pct"], 83.2, places=1)

    def test_capacity_baseline(self):
        res = self.client.get("/api/capacity/baseline")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]
        self.assertEqual(data["total_counters"], 8)
        self.assertEqual(data["total_service_workload_seconds"], 204658)
        self.assertEqual(len(data["baseline"]), 4)

    def test_capacity_scenario(self):
        res = self.client.get("/api/capacity/scenario")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]
        outcomes = data["outcomes"]
        self.assertAlmostEqual(outcomes["fa_wait_delta_minutes"], -87.5, places=1)
        self.assertAlmostEqual(outcomes["fa_wait_delta_pct"], -57.4, places=1)
        self.assertAlmostEqual(outcomes["max_wait_reduction_minutes"], -184.3, places=1)
        self.assertAlmostEqual(outcomes["closing_time_delta_hours"], -1.5, places=1)
        self.assertEqual(outcomes["total_service_workload_seconds"], 204658)
        self.assertTrue(outcomes["workload_conserved"])

        self.assertEqual(len(data["comparison"]), 4)
        self.assertEqual(len(data["reallocated_counters"]), 8)
        self.assertEqual(len(data["tradeoff_notes"]), 3)

    def test_datasets_list(self):
        res = self.client.get("/api/datasets")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]
        self.assertEqual(len(data), 12)
        keys = [d["key"] for d in data]
        self.assertIn("queue_metrics", keys)
        self.assertIn("counter_utilization", keys)
        self.assertIn("raw_events", keys)

    def test_dataset_preview(self):
        res = self.client.get("/api/datasets/queue_metrics?limit=10")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()["data"]
        self.assertEqual(data["key"], "queue_metrics")
        self.assertEqual(data["total_rows"], 4)
        self.assertEqual(data["preview_rows_count"], 4)
        self.assertIn("queue_id", data["columns"])
        self.assertIn("avg_wait_minutes", data["columns"])

    def test_dataset_download(self):
        res = self.client.get("/api/datasets/queue_metrics/download")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.mimetype, "text/csv")
        self.assertIn(b"queue_id,service_type", res.data)

    def test_dataset_security_path_traversal(self):
        res = self.client.get("/api/datasets/..%2f..%2fsecret")
        self.assertEqual(res.status_code, 404)
        res_unknown = self.client.get("/api/datasets/non_existent_table")
        self.assertEqual(res_unknown.status_code, 404)

        res_dl = self.client.get("/api/datasets/..%2f..%2fsecret/download")
        self.assertEqual(res_dl.status_code, 404)

    def test_ml_endpoints(self):
        res_risk = self.client.get("/api/ml/risk")
        self.assertEqual(res_risk.status_code, 200)
        self.assertEqual(len(res_risk.get_json()["data"]["risk_estimates"]), 4)

        res_metrics = self.client.get("/api/ml/metrics")
        self.assertEqual(res_metrics.status_code, 200)
        self.assertGreaterEqual(len(res_metrics.get_json()["data"]), 2)

    def test_frontend_views_render(self):
        pages = [
            "/",
            "/queue-performance",
            "/counter-utilization",
            "/capacity-planning",
            "/data-explorer",
        ]
        for p in pages:
            with self.subTest(page=p):
                res = self.client.get(p)
                self.assertEqual(res.status_code, 200)
                self.assertIn(b"QueueWise", res.data)
                self.assertIn(b"Operations Intelligence Platform", res.data)


if __name__ == "__main__":
    unittest.main()
