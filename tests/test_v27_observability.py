import inspect
import unittest

import app as core

# Capture the exact 2.6 decision functions before importing the overlay.
BASE_MONITOR_COMPUTE = core.Monitor.compute
BASE_ALT_COMPUTE = core.AltShadowMonitor.compute
BASE_OBSERVE_STATE = core.ShadowLedger.observe_state
BASE_MODELS = {k: dict(v) for k, v in core.ALT_LIVE_MODELS.items()}
BASE_DEFAULTS = dict(core.DEFAULTS_DATA)

import vixion_v27 as v27  # noqa: E402


class V27Regression(unittest.TestCase):
    def test_version_only_bump_at_core_global(self):
        self.assertEqual(core.APP_VERSION, "2.7-railway-observability-lab")
        self.assertEqual(v27.APP_VERSION, "2.7-railway-observability-lab")

    def test_primary_compute_is_exact_2_6_function_object(self):
        self.assertIs(core.Monitor.compute, BASE_MONITOR_COMPUTE)
        self.assertEqual(inspect.getsourcefile(core.Monitor.compute), inspect.getsourcefile(BASE_MONITOR_COMPUTE))

    def test_multiasset_compute_is_exact_2_6_function_object(self):
        self.assertIs(core.AltShadowMonitor.compute, BASE_ALT_COMPUTE)

    def test_shadow_entry_semantics_are_exact_2_6_function_object(self):
        self.assertIs(core.ShadowLedger.observe_state, BASE_OBSERVE_STATE)

    def test_live_models_unchanged(self):
        self.assertEqual(core.ALT_LIVE_MODELS, BASE_MODELS)
        self.assertEqual(BASE_MODELS["ETH"]["min_bp"], 30.0)
        self.assertEqual(BASE_MODELS["SOL"]["min_bp"], 30.0)
        self.assertEqual(BASE_MODELS["XRP"]["min_bp"], 40.0)
        self.assertEqual(BASE_MODELS["DOGE"]["min_bp"], 35.0)
        self.assertEqual(BASE_MODELS["BNB"]["min_bp"], 20.0)

    def test_core_defaults_unchanged(self):
        self.assertEqual(core.DEFAULTS_DATA, BASE_DEFAULTS)
        self.assertEqual(core.DEFAULTS_DATA["edge_min_points"], 10.0)
        self.assertEqual(core.DEFAULTS_DATA["fee_buffer_cents"], 1.0)
        self.assertEqual(core.DEFAULTS_DATA["max_basis_gap_bp"], 8.0)

    def test_counterfactual_pnl_math(self):
        win = v27._pnl_scenario(90.0, True, 1.0)
        loss = v27._pnl_scenario(90.0, False, 1.0)
        self.assertEqual(win["gross_pnl_cents"], 10.0)
        self.assertEqual(win["net_pnl_cents"], 9.0)
        self.assertEqual(loss["gross_pnl_cents"], -90.0)
        self.assertEqual(loss["net_pnl_cents"], -91.0)

    def test_thresholds_are_analytics_only_constants(self):
        self.assertEqual(v27.CF_THRESHOLDS, [10.0, 7.5, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0])
        # Production edge is still the 2.6 setting, not any sweep value.
        self.assertEqual(core.DEFAULTS_DATA["edge_min_points"], 10.0)

    def test_latent_safety_fix_not_mixed_into_this_release(self):
        src = inspect.getsource(core.ShadowLedger.observe_state)
        self.assertNotIn("0<float(ask)<100", src.replace(" ", ""))
        self.assertNotIn("0 < float(ask) < 100", src)

    def test_no_order_placement_surface_added(self):
        for module in (core, v27):
            src = inspect.getsource(module).lower()
            for forbidden in ("portfolio/orders", "create_order", "place_order", "submit_order"):
                self.assertNotIn(forbidden, src)

    def test_observer_failures_are_isolated(self):
        # The installed hooks call the original production methods first and guard
        # observer work with try/except. Their source must retain that structure.
        alt_src = inspect.getsource(core.AltShadowMonitor.journal_if_confirmed)
        btc_src = inspect.getsource(core.Monitor.maybe_alert)
        resolve_src = inspect.getsource(core.ShadowLedger.maybe_resolve)
        self.assertIn("_original_alt_journal", alt_src)
        self.assertIn("try:", alt_src)
        self.assertIn("_original_btc_alert", btc_src)
        self.assertIn("_original_resolve", resolve_src)


if __name__ == "__main__":
    unittest.main()
