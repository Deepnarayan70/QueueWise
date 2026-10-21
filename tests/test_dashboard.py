"""
Streamlit AppTest suite for QueueWise Operations Platform (dashboard.py).
Automated functional verification of page navigation, layout, and filtering.
"""

import os
import unittest
from streamlit.testing.v1 import AppTest

DASHBOARD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dashboard.py"))


class TestQueueWiseDashboard(unittest.TestCase):
    def test_overview_page_renders_cleanly(self):
        at = AppTest.from_file(DASHBOARD_PATH, default_timeout=30)
        at.run()
        self.assertFalse(at.exception, f"App threw exception: {at.exception}")

    def test_navigation_across_all_pages(self):
        pages = [
            "Overview",
            "Queue Performance",
            "Counter Utilization",
            "Capacity Planning",
            "Data Explorer",
        ]
        for page in pages:
            with self.subTest(page=page):
                at = AppTest.from_file(DASHBOARD_PATH, default_timeout=30)
                at.run()
                radio = at.sidebar.radio[0]
                radio.set_value(page)
                at.run()
                self.assertFalse(at.exception, f"Page {page} threw exception: {at.exception}")

    def test_queue_performance_filtering(self):
        at = AppTest.from_file(DASHBOARD_PATH, default_timeout=30)
        at.run()
        at.sidebar.radio[0].set_value("Queue Performance")
        at.run()
        self.assertFalse(at.exception)

        # Check multiselect filter exists
        self.assertGreater(len(at.multiselect), 0)
        ms = at.multiselect[0]
        # Filter down to single queue Q_FINANCIAL_AID
        ms.set_value(["Q_FINANCIAL_AID"])
        at.run()
        self.assertFalse(at.exception)


if __name__ == "__main__":
    unittest.main()
