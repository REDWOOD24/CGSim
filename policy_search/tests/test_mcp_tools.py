"""End-to-end smoke test for the MCP layer: launches server.py as a real
subprocess over stdio and drives it with the `mcp` SDK's own client, so this
exercises the MCP transport itself, not just the harness functions
underneath it. Run with the policy-search conda env's python:

    conda run -n policy-search python policy_search/tests/test_mcp_tools.py
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVER_SCRIPT = REPO_ROOT / "policy_search" / "mcp_server" / "server.py"


def _content_json(result) -> dict:
    assert not result.is_error, f"tool call returned an error: {result.content}"
    text = result.content[0].text
    return json.loads(text)


async def main() -> None:
    params = StdioServerParameters(command=sys.executable, args=[str(SERVER_SCRIPT)])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            tools = (await session.list_tools()).tools
            tool_names = {t.name for t in tools}
            expected = {"describe_workload", "register_policy", "validate_policy", "run_simulation",
                        "compare_runs", "inspect_bottlenecks", "record_experiment"}
            assert expected <= tool_names, f"missing tools: {expected - tool_names}"
            print(f"[ok] {len(tools)} tools registered: {sorted(tool_names)}")

            desc = _content_json(await session.call_tool("describe_workload", {"split": "dev"}))
            assert desc["row_count"] == 3000
            print(f"[ok] describe_workload: {desc['row_count']} rows, caveats={len(desc['caveats'])}")

            baseline = _content_json(await session.call_tool(
                "run_simulation", {"policy_name": "historical_replay", "split": "dev", "num_jobs": 100}))
            assert baseline["ok"], baseline
            assert baseline["job_count"] == 100
            print(f"[ok] run_simulation historical_replay: run_id={baseline['run_id']}")

            candidate = _content_json(await session.call_tool(
                "run_simulation", {"policy_name": "least_loaded", "split": "dev", "num_jobs": 100}))
            assert candidate["ok"], candidate
            print(f"[ok] run_simulation least_loaded: run_id={candidate['run_id']}")

            cmp = _content_json(await session.call_tool(
                "compare_runs", {"baseline_run_id": baseline["run_id"], "candidate_run_id": candidate["run_id"]}))
            assert cmp["ok"], cmp
            assert cmp["paired_job_count"] > 0
            assert cmp["mean_turnaround_delta"] == cmp["mean_turnaround_delta"]  # not NaN
            print(f"[ok] compare_runs: paired={cmp['paired_job_count']} "
                  f"mean_delta={cmp['mean_turnaround_delta']:.1f} "
                  f"agreement={cmp['assignment_agreement_fraction']:.2f}")
            assert cmp["assignment_agreement_fraction"] < 1.0, "candidate never moved any job vs. baseline"

            rec = _content_json(await session.call_tool(
                "record_experiment", {"run_id": candidate["run_id"], "notes": "smoke test"}))
            assert rec["ok"], rec
            print("[ok] record_experiment")

            guarded = _content_json(await session.call_tool(
                "run_simulation", {"policy_name": "least_loaded", "split": "test", "num_jobs": 10}))
            assert not guarded["ok"] and "test" in guarded["error"]
            print("[ok] run_simulation refuses the locked test split without allow_test_split")

    print("\nALL MCP SMOKE CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
