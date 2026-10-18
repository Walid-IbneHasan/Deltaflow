from __future__ import annotations

import os

from hypothesis import HealthCheck, settings

# Wall-clock limits make property tests flaky on cold or loaded machines
# (Windows, shared CI runners) without saying anything about correctness.
# Failures are still reproducible: Hypothesis prints the failing example and
# the seed to rerun it with.
settings.register_profile("default", deadline=None, suppress_health_check=[HealthCheck.too_slow])
# HYPOTHESIS_PROFILE=thorough for a longer local soak.
settings.register_profile("thorough", parent=settings.get_profile("default"), max_examples=2000)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))
