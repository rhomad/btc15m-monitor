#!/usr/bin/env python3
"""Runtime launcher for the 2.7 passive overlay.

vixion_v27 observes qualifying-status polls itself. This final wrapper adds
observations for the remaining polls after a setup already exists, which is
necessary to measure the exact transition into MINUTE_LOCK/WINDOW_END even when
the newly completed minute no longer qualifies structurally.
"""
import app as core
import vixion_v27 as v27

_overlay_journal = core.AltShadowMonitor.journal_if_confirmed


def _journal_every_existing_setup_poll(self, state):
    result = _overlay_journal(self, state)
    try:
        # vixion_v27 already traced these statuses inside _overlay_journal.
        # For WAIT/API ERROR/etc., observe_alt_poll is safe: it no-ops unless a
        # production ledger setup for this asset/window already exists.
        if state.get("status") not in v27.ALT_SIGNAL_STATUSES:
            lab = getattr(self, "research", None)
            if lab:
                lab.observe_alt_poll(state, state.get("status"))
    except Exception as exc:
        print("research lab lifecycle poll:", exc, flush=True)
    return result


core.AltShadowMonitor.journal_if_confirmed = _journal_every_existing_setup_poll


if __name__ == "__main__":
    v27.main()
