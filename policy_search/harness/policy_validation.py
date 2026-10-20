from __future__ import annotations

import hashlib
import json
from pathlib import Path

from policy_search.harness import paths

# Mirrors the feature registry in policy_search/plugin/src/policy_spec.cpp.
# Kept in sync manually -- there are only a handful of features, and this
# lets register_policy/validate_policy fail fast in Python without invoking
# a subprocess for a plainly-malformed spec (the C++ loader still validates
# everything again on its own before any real simulation runs).
KNOWN_FEATURES = {
    "available_cores_fraction",
    "available_cores_abs",
    "remaining_storage_fraction",
    "pending_queue_depth",
    "running_jobs",
    "gflops",
    "execution_time_estimate",
    "estimated_completion_time",
    "input_locality_bytes_fraction",
    "bandwidth_from_nearest_input_site",
    "latency_from_nearest_input_site",
    "matches_historical_site",
}

# Features usable in an adaptive policy's activate_when condition: they must
# depend only on live site/grid state, not a specific job. Mirrors
# PolicySpec::is_state_only_feature in policy_spec.cpp.
STATE_ONLY_FEATURES = {
    "available_cores_fraction", "available_cores_abs", "remaining_storage_fraction",
    "pending_queue_depth", "running_jobs", "gflops",
}

STATE_AGGREGATES = {"max_over_sites", "min_over_sites", "avg_over_sites", "sum_over_sites"}
COMPARISON_OPS = {">=", "<=", ">", "<", "==", "!="}

# Encodes the historical answer directly; legitimate for a replay baseline,
# a leakage trap for anything that scores candidate sites. See
# ScoringPolicy::is_leakage_feature in policy_spec.cpp/.h.
LEAKAGE_FEATURES = {"matches_historical_site"}


class PolicySpecError(ValueError):
    pass


def validate_spec_shape(spec: dict) -> list[str]:
    """Checks the spec's JSON shape and feature names. Returns a list of
    non-fatal warnings (e.g. leakage-feature usage); raises PolicySpecError
    on anything that would fail to load in the C++ plugin.

    Dispatches to the adaptive (multi-phase) or flat (single-policy) shape
    check depending on whether "phases" is present, mirroring
    AdaptivePolicySpec::load in policy_spec.cpp.
    """
    if "policy_name" not in spec or not isinstance(spec["policy_name"], str):
        raise PolicySpecError("policy_name is required and must be a string")

    if "phases" in spec:
        return _validate_adaptive_shape(spec)
    return _validate_flat_shape(spec)


def _validate_flat_shape(spec: dict) -> list[str]:
    warnings: list[str] = []

    dispatch_mode = spec.get("dispatch_mode", "score")
    if dispatch_mode not in ("score", "replay"):
        raise PolicySpecError(f"dispatch_mode must be 'score' or 'replay', got {dispatch_mode!r}")

    feature_names_used = set()
    for f in spec.get("features", []):
        name = f.get("name")
        if name not in KNOWN_FEATURES:
            raise PolicySpecError(f"unknown feature '{name}'; known features: {sorted(KNOWN_FEATURES)}")
        if not isinstance(f.get("weight"), (int, float)):
            raise PolicySpecError(f"feature '{name}' weight must be numeric")
        feature_names_used.add(name)

    for rule in spec.get("conditional_rules", []):
        override_feature = rule.get("override_weight", {}).get("feature")
        if override_feature not in KNOWN_FEATURES:
            raise PolicySpecError(f"conditional_rules override references unknown feature '{override_feature}'")
        feature_names_used.add(override_feature)

    if dispatch_mode == "score":
        leaked = feature_names_used & LEAKAGE_FEATURES
        if leaked:
            warnings.append(
                f"policy uses {sorted(leaked)} in 'score' mode -- these encode the historical answer "
                "directly and will make comparisons against historical_replay meaningless."
            )

    norm = spec.get("normalization", "minmax_across_candidates")
    if norm not in ("minmax_across_candidates", "none"):
        raise PolicySpecError(f"normalization must be 'minmax_across_candidates' or 'none', got {norm!r}")

    return warnings


def _validate_adaptive_shape(spec: dict) -> list[str]:
    warnings: list[str] = []

    interval = spec.get("check_interval_seconds", 3600)
    if not isinstance(interval, (int, float)) or interval <= 0:
        raise PolicySpecError("check_interval_seconds must be a positive number")

    phases = spec.get("phases")
    if not isinstance(phases, list) or not phases:
        raise PolicySpecError("'phases' must be a non-empty list")

    for i, phase in enumerate(phases):
        phase_name = phase.get("phase_name")
        if not phase_name:
            raise PolicySpecError(f"phase {i} is missing 'phase_name'")

        phase_spec = phase.get("spec")
        if not isinstance(phase_spec, dict):
            raise PolicySpecError(f"phase '{phase_name}' is missing its 'spec' object")
        if phase_spec.get("dispatch_mode", "score") != "score":
            raise PolicySpecError(
                f"phase '{phase_name}' must use dispatch_mode 'score' -- 'replay' doesn't make sense "
                "inside an adaptive policy"
            )
        warnings += [f"phase '{phase_name}': {w}" for w in _validate_flat_shape(phase_spec)]

        if i == 0:
            if "activate_when" in phase:
                raise PolicySpecError(
                    f"phase 0 ('{phase_name}') is the default phase, active from t=0, and must not have "
                    "activate_when"
                )
            continue

        cond = phase.get("activate_when")
        if not isinstance(cond, dict):
            raise PolicySpecError(f"phase '{phase_name}' (index {i}) requires an 'activate_when' condition")
        feature = cond.get("feature")
        if feature not in STATE_ONLY_FEATURES:
            raise PolicySpecError(
                f"phase '{phase_name}': activate_when feature '{feature}' is job-dependent and cannot be used "
                f"in a state condition; allowed: {sorted(STATE_ONLY_FEATURES)}"
            )
        if cond.get("aggregate") not in STATE_AGGREGATES:
            raise PolicySpecError(f"phase '{phase_name}': activate_when aggregate must be one of {sorted(STATE_AGGREGATES)}")
        if cond.get("op") not in COMPARISON_OPS:
            raise PolicySpecError(f"phase '{phase_name}': activate_when op must be one of {sorted(COMPARISON_OPS)}")
        if not isinstance(cond.get("value"), (int, float)):
            raise PolicySpecError(f"phase '{phase_name}': activate_when value must be numeric")

    return warnings


def _coerce_numeric(value):
    """Some models emit numeric fields as JSON strings (e.g. weight: "-1"
    instead of -1). The C++ loader's nlohmann::json .get<double>() throws on
    a string, so silently coercing here -- before anything is written to
    disk or validated -- avoids a spec that looks fine in Python but fails
    to load in the plugin.
    """
    if isinstance(value, str):
        try:
            f = float(value)
            return int(f) if f.is_integer() and "." not in value and "e" not in value.lower() else f
        except ValueError:
            return value
    return value


def _coerce_bool(value):
    """Some models emit boolean fields as JSON strings (e.g.
    emit_debug_events: "true" instead of true). nlohmann::json's
    j.value<bool>() throws a type_error on a string just like .get<double>()
    does for numbers -- same failure mode as _coerce_numeric, different
    type. Observed in practice: a model correctly diagnosed that it needed
    emit_debug_events: true to satisfy the inspect_bottlenecks requirement,
    sent it as a string, and crashed cg-sim instead.
    """
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
    return value


def _coerce_numeric_fields(spec: dict) -> dict:
    spec = dict(spec)

    if "emit_debug_events" in spec:
        spec["emit_debug_events"] = _coerce_bool(spec["emit_debug_events"])

    if isinstance(spec.get("feasibility"), dict):
        feasibility = dict(spec["feasibility"])
        if "storage_margin_bytes" in feasibility:
            feasibility["storage_margin_bytes"] = _coerce_numeric(feasibility["storage_margin_bytes"])
        spec["feasibility"] = feasibility

    if isinstance(spec.get("features"), list):
        spec["features"] = [{**f, "weight": _coerce_numeric(f.get("weight"))} for f in spec["features"]]

    if isinstance(spec.get("conditional_rules"), list):
        rules = []
        for rule in spec["conditional_rules"]:
            rule = dict(rule)
            if isinstance(rule.get("override_weight"), dict):
                ow = dict(rule["override_weight"])
                ow["weight"] = _coerce_numeric(ow.get("weight"))
                rule["override_weight"] = ow
            rules.append(rule)
        spec["conditional_rules"] = rules

    if "check_interval_seconds" in spec:
        spec["check_interval_seconds"] = _coerce_numeric(spec["check_interval_seconds"])

    if isinstance(spec.get("phases"), list):
        phases = []
        for phase in spec["phases"]:
            phase = dict(phase)
            if isinstance(phase.get("activate_when"), dict):
                cond = dict(phase["activate_when"])
                if "value" in cond:
                    cond["value"] = _coerce_numeric(cond["value"])
                phase["activate_when"] = cond
            if isinstance(phase.get("spec"), dict):
                phase["spec"] = _coerce_numeric_fields(phase["spec"])
            phases.append(phase)
        spec["phases"] = phases

    return spec


def register_policy(policy_name: str, spec: dict, workspace: str | None = None) -> tuple[str, list[str]]:
    """Validates and writes a policy spec. Returns (sha256_hex, warnings).

    The explicit `policy_name` argument is authoritative -- it determines
    the file path -- so it's written into (or overwritten in) `spec` here
    rather than requiring the caller to redundantly duplicate it inside the
    spec object too. Numeric-looking string values are also coerced (see
    _coerce_numeric): together these remove two failure modes that cost a
    weaker tool-calling model its entire iteration budget in practice (it
    kept resending the same rejected payload without ever diagnosing why).

    `workspace`, when given, writes under that session's private
    subdirectory (paths.AGENT_WORKSPACES_DIR) instead of the shared curated
    policies/ directory -- this is how mcp_server/server.py keeps
    concurrent agent sessions, and agents in general, from overwriting each
    other's or a curated baseline's policy file. Pass None (the default)
    only for policies meant to be shared/curated, e.g. shipped baselines.
    """
    spec = _coerce_numeric_fields(spec)
    spec["policy_name"] = policy_name

    warnings = validate_spec_shape(spec)

    out_path = paths.policy_spec_path(policy_name, workspace)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(spec, indent=2, sort_keys=True)
    out_path.write_text(content)

    return hashlib.sha256(content.encode()).hexdigest(), warnings


def policy_spec_sha256(policy_name: str, workspace: str | None = None) -> str:
    return hashlib.sha256(paths.resolve_policy_spec_path(policy_name, workspace).read_bytes()).hexdigest()
