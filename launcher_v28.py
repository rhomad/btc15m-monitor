#!/usr/bin/env python3
"""Runtime launcher for the 2.8 Executable Entry Timing Lab.

Boot order is the production 2.7 chain plus one more passive layer:
  app.py (2.6 engine, untouched) -> vixion_v27.py (observability) ->
  launcher_v27 lifecycle wrapper (untouched) -> vixion_v28.py (entry timing lab).

Nothing here changes a decision path. launcher_v27 is imported for its
journal_if_confirmed lifecycle wrapper only; its main() is not called from here.
"""
import launcher_v27  # noqa: F401  (installs the 2.7 lifecycle wrapper at import time)
import vixion_v28 as v28


if __name__ == "__main__":
    v28.main()
