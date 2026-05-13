"""Matplotlib figures for data-management transfer logs (matplotlib only; no numpy)."""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path
from typing import Sequence

from plotting.dm_log_parser import (
    NegativeTransferBytesError,
    TransferRecord,
    iter_config_run_directories,
    load_site_names,
    parse_transfer_log,
    site_axis_label_padded,
    site_info_path_for_config_directory,
    sort_site_names_natural,
    stack_segment_display_label,
    stack_segment_key,
)
from plotting.job_metrics import (
    SeriesStats,
    format_cross_config_timing_report,
    job_timing_stats_bundle_for_config_dir,
)
from plotting.plot_defaults import PLOT_DEFAULTS


_MPL_PLT = None


def _ensure_matplotlib():
    global _MPL_PLT
    if _MPL_PLT is not None:
        return _MPL_PLT
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # noqa: WPS433

        _MPL_PLT = plt
        return _MPL_PLT
    except ImportError as e:
        raise ImportError(
            "Plotting requires matplotlib. Install with: pip install matplotlib"
        ) from e


def _sort_stack_segment_keys(keys: Sequence[str]) -> list[str]:
    """Reactive segments first, then proactive; then template and mode for stable colors."""

    def order(k: str) -> tuple:
        p = k.split("|", 2)
        layer = p[0] if p else k
        mode = p[1] if len(p) > 1 else ""
        tmpl = p[2] if len(p) > 2 else ""
        lo = 0 if layer == "reactive" else (1 if layer == "proactive" else 2)
        return (lo, tmpl.lower(), mode, k.lower())

    return sorted(keys, key=order)


def _bytes_axis_scientific(axis) -> None:
    import matplotlib.ticker as mticker  # noqa: WPS433

    fmt = mticker.ScalarFormatter(useMathText=True)
    fmt.set_scientific(True)
    fmt.set_powerlimits((0, 0))
    axis.set_major_formatter(fmt)


def preflight_matplotlib(repo_root: Path | None = None) -> None:
    """Fail fast with install instructions if matplotlib is missing."""
    try:
        _ensure_matplotlib()
    except ImportError as exc:
        root = repo_root or Path(__file__).resolve().parent.parent
        from plotting.plot_defaults import matplotlib_install_hint

        raise ImportError(matplotlib_install_hint(root)) from exc


def build_site_index_matrix(
    sites: Sequence[str], records: list[TransferRecord]
) -> list[list[float]]:
    """Matrix[row_src][col_dst] = total bytes."""
    idx = {s: i for i, s in enumerate(sites)}
    n = len(sites)
    mat = [[0.0] * n for _ in range(n)]
    for r in records:
        if r.bytes <= 0:
            continue
        si = idx.get(r.src)
        di = idx.get(r.dst)
        if si is None or di is None:
            continue
        mat[si][di] += float(r.bytes)
    return mat


def plot_site_heatmap_bytes(
    sites: Sequence[str],
    records: list[TransferRecord],
    out_path: Path,
    *,
    title: str,
) -> None:
    import matplotlib.colors as mcolors  # noqa: WPS433
    import matplotlib.ticker as mticker  # noqa: WPS433

    plt = _ensure_matplotlib()
    ordered = sort_site_names_natural(sites)
    axis_labels = [site_axis_label_padded(s) for s in ordered]
    mat = build_site_index_matrix(ordered, records)
    n = len(ordered)
    for i in range(n):
        for j in range(n):
            v = mat[i][j]
            if v < 0:
                msg = (
                    f"Invalid negative aggregate transfer bytes ({v}) for heatmap cell "
                    f"src={ordered[i]!r} dst={ordered[j]!r}"
                )
                print(msg, file=sys.stderr)
                raise NegativeTransferBytesError(msg)
    mx = max((mat[i][j] for i in range(n) for j in range(n)), default=0.0)
    vmax = mx if mx > 0 else 1.0
    norm = mcolors.Normalize(vmin=0.0, vmax=vmax)
    fig_w = min(
        float(PLOT_DEFAULTS["fig_cap_inches"]),
        max(float(PLOT_DEFAULTS["heatmap_min_inches"]), len(ordered) * float(PLOT_DEFAULTS["heatmap_inches_per_site"])),
    )
    fig_h = min(
        float(PLOT_DEFAULTS["fig_cap_inches"]),
        max(float(PLOT_DEFAULTS["heatmap_min_height"]), len(ordered) * float(PLOT_DEFAULTS["heatmap_inches_per_site"])),
    )
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    im = ax.imshow(mat, aspect="equal", cmap=str(PLOT_DEFAULTS["heatmap_cmap"]), norm=norm, zorder=1)
    ticks = list(range(len(ordered)))
    ax.set_xticks(ticks)
    ax.set_yticks(ticks)
    hf = int(PLOT_DEFAULTS["heatmap_fontsize"])
    ax.set_xticklabels(axis_labels, rotation=90, fontsize=hf)
    ax.set_yticklabels(axis_labels, fontsize=hf)
    for g in range(n + 1):
        ax.axhline(g - 0.5, color="0.35", linewidth=0.55, zorder=2)
        ax.axvline(g - 0.5, color="0.35", linewidth=0.55, zorder=2)
    ax.set_xlabel("Destination site")
    ax.set_ylabel("Source site")
    ax.set_title(title + "\n(color = total bytes, linear scale; colorbar uses scientific notation)")
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="Total bytes")
    cbf = mticker.ScalarFormatter(useMathText=True)
    cbf.set_scientific(True)
    cbf.set_powerlimits((0, 0))
    cbar.ax.yaxis.set_major_formatter(cbf)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=int(PLOT_DEFAULTS["dpi"]))
    plt.close(fig)


def _aggregate_edge_stacks(
    records: list[TransferRecord],
) -> tuple[list[tuple[str, str]], dict[tuple[str, str], dict[str, float]]]:
    edge_vals: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    totals: dict[tuple[str, str], float] = defaultdict(float)
    for r in records:
        if r.bytes <= 0:
            continue
        e = (r.src, r.dst)
        k = stack_segment_key(r)
        edge_vals[e][k] += r.bytes
        totals[e] += r.bytes
    ordered = sorted(totals.keys(), key=lambda e: totals[e], reverse=True)
    return ordered, edge_vals


def _stack_layer_prefix(segment_key: str) -> str:
    """First field of ``layer|mode|template`` keys."""
    i = segment_key.find("|")
    return segment_key[:i] if i >= 0 else "other"


def _stack_segment_fill_colors(keys: list[str]) -> dict[str, object]:
    """Blues family for reactive, oranges for proactive, greys for anything else."""
    import matplotlib  # noqa: WPS433

    react = [k for k in keys if _stack_layer_prefix(k) == "reactive"]
    proact = [k for k in keys if _stack_layer_prefix(k) == "proactive"]
    rest = [k for k in keys if k not in set(react) | set(proact)]
    out: dict[str, object] = {}

    def assign_shades(cmap_name: str, group: list[str], lo: float, hi: float) -> None:
        if hasattr(matplotlib, "colormaps"):
            cmap = matplotlib.colormaps[cmap_name]
        else:
            import matplotlib.cm as cm  # noqa: WPS433

            cmap = cm.get_cmap(cmap_name)
        n = len(group)
        if n <= 0:
            return
        for i, k in enumerate(group):
            t = lo + (hi - lo) * (i / max(n - 1, 1)) if n > 1 else (lo + hi) / 2.0
            out[k] = cmap(t)

    assign_shades(str(PLOT_DEFAULTS["stack_reactive_cmap"]), react, 0.38, 0.92)
    assign_shades(str(PLOT_DEFAULTS["stack_proactive_cmap"]), proact, 0.38, 0.92)
    assign_shades(str(PLOT_DEFAULTS["stack_other_cmap"]), rest, 0.35, 0.75)
    return out


def _set_stacked_bar_ylim(ax, column_totals: list[float]) -> None:
    """Leave headroom so the tallest stack stays below the axes top."""
    mx = max(column_totals) if column_totals else 0.0
    factor = float(PLOT_DEFAULTS["stack_bar_ymax_factor"])
    top = max(mx * factor, mx + 1.0)
    ax.set_ylim(0.0, top)


def plot_stacked_transfers_by_edge(
    records: list[TransferRecord],
    out_path: Path,
    *,
    title: str,
    max_edges: int | None = None,
) -> None:
    plt = _ensure_matplotlib()
    me = int(PLOT_DEFAULTS["max_edges"]) if max_edges is None else max_edges
    ordered, edge_vals = _aggregate_edge_stacks(records)
    if not ordered:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, "No byte-sized transfers to plot", ha="center", va="center")
        ax.axis("off")
        fig.savefig(out_path, dpi=int(PLOT_DEFAULTS["dpi_empty"]))
        plt.close(fig)
        return

    ordered = ordered[:me]
    keys = _sort_stack_segment_keys(list({k for e in ordered for k in edge_vals[e]}))
    key_color = _stack_segment_fill_colors(keys)

    labels = [f"{s}\n→\n{d}" for s, d in ordered]
    x = list(range(len(labels)))
    bottom = [0.0] * len(labels)
    fig_w = max(
        float(PLOT_DEFAULTS["edge_bar_min_width"]),
        len(labels) * float(PLOT_DEFAULTS["edge_bar_inches_per_label"]),
    )
    fig, ax = plt.subplots(figsize=(fig_w, float(PLOT_DEFAULTS["edge_bar_fig_height"])))
    for key in keys:
        heights = [edge_vals[e].get(key, 0.0) for e in ordered]
        if sum(heights) == 0:
            continue
        ax.bar(
            x,
            heights,
            bottom=bottom,
            label=stack_segment_display_label(key),
            color=key_color[key],
            width=float(PLOT_DEFAULTS["stack_bar_width"]),
        )
        bottom = [bottom[i] + heights[i] for i in range(len(labels))]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=int(PLOT_DEFAULTS["edge_label_fontsize"]), rotation=0)
    ax.set_ylabel("Bytes")
    ax.set_title(title)
    _bytes_axis_scientific(ax.yaxis)
    leg_fs = int(PLOT_DEFAULTS["stack_legend_fontsize"])
    ncol = 2 if len(keys) > 12 else int(PLOT_DEFAULTS["stack_legend_ncol"])
    ax.legend(
        loc="upper right",
        fontsize=leg_fs,
        frameon=True,
        fancybox=True,
        framealpha=0.92,
        ncol=ncol,
    )
    fig.tight_layout()
    _set_stacked_bar_ylim(ax, bottom)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=int(PLOT_DEFAULTS["dpi"]), bbox_inches="tight")
    plt.close(fig)


def plot_cross_configuration_stacked(
    run_root: Path,
    out_path: Path,
    *,
    title: str = "All configurations — transfer bytes by layer, mode, and template",
) -> None:
    plt = _ensure_matplotlib()
    rows: list[tuple[Path, str, float, dict[str, float]]] = []
    for d in iter_config_run_directories(run_root):
        logf = d / "atlas_grid_simulation.log"
        if not logf.is_file():
            continue
        recs, _ = parse_transfer_log(logf)
        by_k: dict[str, float] = defaultdict(float)
        tot = 0.0
        for r in recs:
            if r.bytes <= 0:
                continue
            by_k[stack_segment_key(r)] += r.bytes
            tot += r.bytes
        rows.append((d, d.name, tot, dict(by_k)))

    if not rows:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, "No configuration logs found", ha="center", va="center")
        ax.axis("off")
        fig.savefig(out_path, dpi=int(PLOT_DEFAULTS["dpi_empty"]))
        plt.close(fig)
        return

    rows.sort(key=lambda r: r[2], reverse=True)
    all_keys = _sort_stack_segment_keys(list({k for _, _, _, bk in rows for k in bk}))
    configs = [r[1] for r in rows]

    key_color = _stack_segment_fill_colors(all_keys)

    x = list(range(len(configs)))
    bottom = [0.0] * len(configs)
    fig_w = max(
        float(PLOT_DEFAULTS["cross_bar_min_width"]),
        len(configs) * float(PLOT_DEFAULTS["cross_bar_inches_per_config"]),
    )
    fig, ax = plt.subplots(figsize=(fig_w, float(PLOT_DEFAULTS["cross_bar_fig_height"])))
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="0.75", linestyle="-", linewidth=0.7, alpha=0.95)
    for key in all_keys:
        heights = [r[3].get(key, 0.0) for r in rows]
        if sum(heights) == 0:
            continue
        ax.bar(
            x,
            heights,
            bottom=bottom,
            label=stack_segment_display_label(key),
            color=key_color[key],
            width=float(PLOT_DEFAULTS["stack_bar_width"]),
        )
        bottom = [bottom[i] + heights[i] for i in range(len(configs))]
    ax.set_xticks(x)
    ax.set_xticklabels(
        configs,
        rotation=45,
        ha="right",
        fontsize=int(PLOT_DEFAULTS["cross_label_fontsize"]),
    )
    ax.set_ylabel("Bytes")
    ax.set_title(title)
    _bytes_axis_scientific(ax.yaxis)

    time_vals: list[float] = []
    e2e_avg: list[float | None] = []
    queue_avg: list[float | None] = []
    timing_entries: list[tuple[str, Path | None, SeriesStats | None, SeriesStats | None]] = []
    for r in rows:
        jp, e2e_st, q_st = job_timing_stats_bundle_for_config_dir(r[0])
        timing_entries.append((r[1], jp, e2e_st, q_st))
        if e2e_st is not None and q_st is not None:
            e2e_avg.append(e2e_st.mean_s)
            queue_avg.append(q_st.mean_s)
            time_vals.append(e2e_st.mean_s)
            time_vals.append(q_st.mean_s)
        else:
            e2e_avg.append(None)
            queue_avg.append(None)

    report_path = out_path.with_name("cross_config_job_timing_stats.txt")
    report_text = format_cross_config_timing_report(run_root, timing_entries)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report_text, encoding="utf-8")
    print(report_text, end="")

    ax2 = None
    if time_vals:
        ax2 = ax.twinx()
        e2e_y = [v if v is not None else float("nan") for v in e2e_avg]
        queue_y = [v if v is not None else float("nan") for v in queue_avg]
        ax2.plot(
            x,
            e2e_y,
            "o-",
            color="#c0392b",
            linewidth=1.5,
            markersize=5,
            label="Avg job lifetime (s)",
            zorder=4,
        )
        ax2.plot(
            x,
            queue_y,
            "s-",
            color="#1e8449",
            linewidth=1.5,
            markersize=5,
            label="Avg queue time (s)",
            zorder=4,
        )
        ax2.set_ylabel("Time (s)", fontsize=11)
        ax2.tick_params(axis="y", labelsize=int(PLOT_DEFAULTS["cross_label_fontsize"]))
        plot_max = 0.0
        for v in e2e_avg + queue_avg:
            if v is not None:
                plot_max = max(plot_max, float(v))
        if plot_max > 0.0:
            ax2.set_ylim(0.0, plot_max * 1.12 + 1.0)
        else:
            ax2.set_ylim(0.0, 1.0)

    leg_fs = int(PLOT_DEFAULTS["stack_legend_fontsize"])
    ncol = 2 if len(all_keys) > 12 else int(PLOT_DEFAULTS["stack_legend_ncol"])
    h1, l1 = ax.get_legend_handles_labels()
    if ax2 is not None:
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(
            h1 + h2,
            l1 + l2,
            loc="upper right",
            fontsize=leg_fs,
            frameon=True,
            fancybox=True,
            framealpha=0.92,
            ncol=ncol,
        )
    else:
        ax.legend(
            h1,
            l1,
            loc="upper right",
            fontsize=leg_fs,
            frameon=True,
            fancybox=True,
            framealpha=0.92,
            ncol=ncol,
        )
    fig.tight_layout()
    _set_stacked_bar_ylim(ax, bottom)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=int(PLOT_DEFAULTS["dpi"]), bbox_inches="tight")
    plt.close(fig)


def plot_per_configuration_transfers(
    config_dir: Path,
    site_info_json: Path,
    *,
    stem_title: str | None = None,
) -> None:
    logf = config_dir / "atlas_grid_simulation.log"
    if not logf.is_file():
        return
    sites_json = site_info_path_for_config_directory(config_dir, fallback=site_info_json)
    sites = load_site_names(sites_json)
    records, _ = parse_transfer_log(logf)
    title = stem_title or config_dir.name
    plot_site_heatmap_bytes(
        sites,
        records,
        config_dir / "transfer_heatmap_bytes.png",
        title=f"{title} — site × site bytes (linear scale)",
    )
    plot_stacked_transfers_by_edge(
        records,
        config_dir / "transfer_stacked_by_edge.png",
        title=f"{title} — bytes by edge (src→dst), stacked by layer / mode / template",
    )


def plot_cross_configuration_summary(
    run_root: Path,
    site_info_json: Path,
) -> None:
    _ = site_info_json
    plot_cross_configuration_stacked(
        run_root,
        run_root / "cross_config_transfer_stacked.png",
        title="Cross-configuration total bytes (stacked by layer, mode, and template)",
    )
