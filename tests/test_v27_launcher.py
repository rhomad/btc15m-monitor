import inspect
import unittest

import app as core
import vixion_v27 as v27

BASE_MONITOR_COMPUTE = core.Monitor.compute
BASE_ALT_COMPUTE = core.AltShadowMonitor.compute
BASE_OBSERVE_STATE = core.ShadowLedger.observe_state

import launcher_v27  # noqa: E402,F401


class LauncherRegression(unittest.TestCase):
    def test_decision_functions_remain_unmodified_after_launcher(self):
        self.assertIs(core.Monitor.compute, BASE_MONITOR_COMPUTE)
        self.assertIs(core.AltShadowMonitor.compute, BASE_ALT_COMPUTE)
        self.assertIs(core.ShadowLedger.observe_state, BASE_OBSERVE_STATE)

    def test_lifecycle_wrapper_traces_only_as_side_effect(self):
        src = inspect.getsource(core.AltShadowMonitor.journal_if_confirmed)
        self.assertIn("_overlay_journal", src)
        self.assertIn("lab.observe_alt_poll", src)
        self.assertIn("return result", src)

    def test_no_strategy_constants_changed(self):
        self.assertEqual(core.DEFAULTS_DATA["edge_min_points"], 10.0)
        self.assertEqual(core.DEFAULTS_DATA["fee_buffer_cents"], 1.0)
        self.assertEqual(core.DEFAULTS_DATA["max_basis_gap_bp"], 8.0)
        self.assertEqual(v27.CF_THRESHOLDS, [10.0, 7.5, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0])


if __name__ == "__main__":
    unittest.main()
