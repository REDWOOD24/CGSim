"""The agent loop: connects to the CGSim policy-search MCP server as a real
client (same transport as policy_search/tests/test_mcp_tools.py) and drives
an iterative propose -> register -> validate -> run -> compare -> reflect
cycle using whichever LLM provider is configured (see llm_clients.py).

Run from the policy-search conda env (`mcp` needs Python >= 3.10):

    conda run -n policy-search python -m policy_search.agent.loop \\
        --provider anthropic --model claude-sonnet-5 --max-iterations 12

    conda run -n policy-search python -m policy_search.agent.loop \\
        --provider openai_compatible --model gemma-4-26b \\
        --base-url https://your-inference-endpoint/v1 --api-key-env MY_LOCAL_KEY
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from policy_search.agent import prompts
from policy_search.agent.llm_clients import ToolResult, ToolSpec, make_llm_client

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVER_SCRIPT = REPO_ROOT / "policy_search" / "mcp_server" / "server.py"
TRANSCRIPTS_DIR = Path(__file__).resolve().parent / "transcripts"

# Extra turns granted, beyond the caller's max_iterations budget, purely to
# force a skipped inspect_bottlenecks call -- see the epilogue phase below.
# Small and fixed, not open-ended: in practice this takes exactly 3 tool
# calls (re-register with emit_debug_events, re-run, inspect) plus one more
# turn to actually write the final answer citing what it saw.
EPILOGUE_BUDGET = 4


def _mcp_result_text(result) -> str:
    parts = [block.text for block in result.content if hasattr(block, "text")]
    return "\n".join(parts) if parts else json.dumps({"ok": False, "error": "empty tool result"})


def _tool_call_succeeded(content: str, is_error: bool) -> bool:
    """Every tool here returns {"ok": bool, ...} as a normal
    (non-protocol-error) result -- e.g. inspect_bottlenecks on a run with no
    debug events comes back as a plain {"ok": false, "error": "..."} with
    is_error False at the MCP level. So "did this call actually succeed" has
    to come from parsing that field, not from is_error alone.
    """
    if is_error:
        return False
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return True
    if isinstance(parsed, dict) and "ok" in parsed:
        return bool(parsed["ok"])
    return True


async def run_agent_loop(llm_client, split: str, max_iterations: int, save_transcript: bool = True,
                          dataset: str = "default") -> dict:
    params = StdioServerParameters(command=sys.executable, args=[str(SERVER_SCRIPT), "--dataset", dataset])
    transcript_log: list[dict] = []

    try:
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                mcp_tools = (await session.list_tools()).tools
                tool_specs = [ToolSpec(name=t.name, description=t.description or "", input_schema=t.input_schema)
                              for t in mcp_tools]
                print(f"Connected to MCP server with {len(tool_specs)} tools: {[t.name for t in tool_specs]}")

                initial_prompt = prompts.build_initial_user_prompt(split, max_iterations, dataset)
                history: list[dict] = [{"kind": "user", "text": initial_prompt}]
                transcript_log.append({"role": "user", "text": initial_prompt})

                # Every model we've tried skips inspect_bottlenecks unless
                # forced -- they'll happily report a plausible-sounding
                # causal story for why their winning policy works without
                # ever checking it against actual observed per-site state
                # (one run even asserted something its own reported data
                # directly contradicted). The system prompt asks for this;
                # these two flags back that up structurally via the
                # guaranteed epilogue phase below, for when asking isn't
                # enough.
                called_inspect_bottlenecks = False
                ran_any_simulation = False

                async def do_turn(label: str) -> bool:
                    """Runs one model turn: chat, then dispatch any tool
                    calls. Returns True if the turn had tool calls (loop
                    should continue), False if it was a final plain-text
                    answer, or the chat call itself failed (loop should
                    stop either way) -- updates the closed-over tracking
                    flags and shared history/transcript_log as a side
                    effect.
                    """
                    nonlocal called_inspect_bottlenecks, ran_any_simulation
                    try:
                        turn = llm_client.chat(system=prompts.SYSTEM_PROMPT, tool_specs=tool_specs, history=history)
                    except Exception as e:  # noqa: BLE001 -- e.g. a context-window overflow from a large
                        # tool result accumulated in history (this is why inspect_bottlenecks returns a
                        # bounded summary, not the raw series -- but any provider/model call can fail for
                        # other reasons too, and losing the whole transcript to an uncaught exception here
                        # is worse than stopping cleanly with whatever was learned so far).
                        print(f"[loop] {label}: LLM call failed ({type(e).__name__}: {e}) -- stopping session")
                        transcript_log.append({"role": "error", "label": label, "error": f"{type(e).__name__}: {e}"})
                        return False

                    history.append({"kind": "assistant", "turn": turn})
                    transcript_log.append({
                        "role": "assistant",
                        "label": label,
                        "text": turn.text,
                        "tool_calls": [{"name": tc.name, "input": tc.input} for tc in turn.tool_calls],
                        "usage": turn.usage,
                    })

                    print(f"\n--- {label} ---")
                    if turn.text:
                        print(f"[assistant] {turn.text}")

                    if not turn.tool_calls:
                        return False

                    results: list[ToolResult] = []
                    for tc in turn.tool_calls:
                        print(f"[tool_call] {tc.name}({json.dumps(tc.input)})")
                        try:
                            mcp_result = await session.call_tool(tc.name, tc.input)
                            content = _mcp_result_text(mcp_result)
                            is_error = mcp_result.is_error
                        except Exception as e:  # noqa: BLE001 -- surfaced to the model as a tool error
                            content = json.dumps({"ok": False, "error": str(e)})
                            is_error = True

                        if _tool_call_succeeded(content, is_error):
                            if tc.name == "inspect_bottlenecks":
                                called_inspect_bottlenecks = True
                            if tc.name == "run_simulation":
                                ran_any_simulation = True

                        print(f"[tool_result] {content[:500]}")
                        results.append(ToolResult(tool_call_id=tc.id, name=tc.name, content=content,
                                                   is_error=is_error))
                        transcript_log.append({
                            "role": "tool_result", "label": label, "name": tc.name,
                            "content": content, "is_error": is_error,
                        })
                    history.append({"kind": "tool_results", "results": results})
                    return True

                for iteration in range(max_iterations):
                    if not await do_turn(f"iteration {iteration + 1}/{max_iterations}"):
                        print("[loop] assistant produced a final answer with no tool calls -- stopping")
                        break
                else:
                    print(f"\n[loop] reached max_iterations={max_iterations} without a final answer")

                # Guaranteed epilogue: whether the model stopped cleanly or
                # ran out of budget, if it ran a real simulation but never
                # got a successful inspect_bottlenecks call, force that now
                # -- outside and independent of max_iterations, so it can
                # never be skipped just because exploration happened to
                # consume the whole budget first.
                if ran_any_simulation and not called_inspect_bottlenecks:
                    nudge = (
                        "You've used your search budget without ever calling inspect_bottlenecks "
                        "successfully. Before this session ends: re-run your best candidate with "
                        '"emit_debug_events": true if it wasn\'t already set, call inspect_bottlenecks on '
                        "it, and give a final answer that revises your explanation if what you observe "
                        "contradicts what you assumed. You have a few extra turns reserved specifically "
                        "for this."
                    )
                    print(f"\n[loop] entering guaranteed verification epilogue: {nudge}")
                    history.append({"kind": "user", "text": nudge})
                    transcript_log.append({"role": "user", "text": nudge, "note": "forced inspect_bottlenecks epilogue"})

                    for epilogue_iteration in range(EPILOGUE_BUDGET):
                        if not await do_turn(f"epilogue {epilogue_iteration + 1}/{EPILOGUE_BUDGET}"):
                            break
                    else:
                        if called_inspect_bottlenecks:
                            print(f"[loop] epilogue budget ({EPILOGUE_BUDGET}) exhausted after a successful "
                                  "inspect_bottlenecks call, but before a final answer -- its last tool "
                                  "result above is the verified data it should have used")
                        else:
                            print(f"[loop] epilogue budget ({EPILOGUE_BUDGET}) exhausted without ever "
                                  "getting a successful inspect_bottlenecks call")
    except Exception as e:  # noqa: BLE001 -- e.g. an MCP transport/session error. Whatever was learned
        # before the crash is still worth keeping (saved in the finally block below) rather than lost.
        print(f"\n[loop] session crashed: {type(e).__name__}: {e}")
        transcript_log.append({"role": "error", "error": f"{type(e).__name__}: {e}"})
    finally:
        if save_transcript:
            TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
            out_path = TRANSCRIPTS_DIR / f"transcript_{int(time.time())}.json"
            out_path.write_text(json.dumps(transcript_log, indent=2, default=str))
            print(f"\nTranscript saved to {out_path}")

    return {"transcript": transcript_log}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--provider", choices=["anthropic", "openai_compatible"], default="anthropic")
    parser.add_argument("--model", required=True)
    parser.add_argument("--base-url", default=None, help="Required for --provider openai_compatible")
    parser.add_argument("--api-key-env", default=None,
                         help="Env var holding the API key (default: ANTHROPIC_API_KEY / OPENAI_API_KEY)")
    parser.add_argument("--max-iterations", type=int, default=12)
    parser.add_argument("--split", default="dev", choices=["calib", "dev"])
    parser.add_argument("--dataset", default="default", choices=["default", "large"],
                         help="'default': 15,000 rows / 3 sites. 'large': 849,070 real PanDA rows / 31 sites.")
    parser.add_argument("--max-tokens", type=int, default=4096)
    args = parser.parse_args()

    if args.provider == "openai_compatible" and not args.base_url:
        parser.error("--base-url is required for --provider openai_compatible")

    llm_client = make_llm_client(args.provider, args.model, base_url=args.base_url,
                                  api_key_env=args.api_key_env, max_tokens=args.max_tokens)
    asyncio.run(run_agent_loop(llm_client, args.split, args.max_iterations, dataset=args.dataset))


if __name__ == "__main__":
    main()
