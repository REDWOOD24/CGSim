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


def _tab20_colors(n: int) -> list:
    import matplotlib.cm as cm  # noqa: WPS433

    cmap = cm.get_cmap(str(PLOT_DEFAULTS["stack_colormap"]))
    if n <= 0:
        return []
    return [cmap(i / max(n - 1, 1)) for i in range(n)]


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
    colors = _tab20_colors(len(keys))
    key_color = {keys[i]: colors[i] for i in range(len(keys))}

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
    rows: list[tuple[str, float, dict[str, float]]] = []
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
        rows.append((d.name, tot, dict(by_k)))

    if not rows:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(0.5, 0.5, "No configuration logs found", ha="center", va="center")
        ax.axis("off")
        fig.savefig(out_path, dpi=int(PLOT_DEFAULTS["dpi_empty"]))
        plt.close(fig)
        return

    rows.sort(key=lambda r: r[1], reverse=True)
    all_keys = _sort_stack_segment_keys(list({k for _, _, bk in rows for k in bk}))
    configs = [r[0] for r in rows]

    colors = _tab20_colors(len(all_keys))
    key_color = {all_keys[i]: colors[i] for i in range(len(all_keys))}

    x = list(range(len(configs)))
    bottom = [0.0] * len(configs)
    fig_w = max(
        float(PLOT_DEFAULTS["cross_bar_min_width"]),
        len(configs) * float(PLOT_DEFAULTS["cross_bar_inches_per_config"]),
    )
    fig, ax = plt.subplots(figsize=(fig_w, float(PLOT_DEFAULTS["cross_bar_fig_height"])))
    for key in all_keys:
        heights = [r[2].get(key, 0.0) for r in rows]
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
    leg_fs = int(PLOT_DEFAULTS["stack_legend_fontsize"])
    ncol = 2 if len(all_keys) > 12 else int(PLOT_DEFAULTS["stack_legend_ncol"])
    ax.legend(
        loc="upper right",
        fontsize=leg_fs,
        frameon=True,
        fancybox=True,
        framealpha=0.92,
        ncol=ncol,
    )
    fig.tight_layout()
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
