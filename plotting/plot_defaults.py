"""Default figure settings for data-management transfer plots (single source of truth)."""

from pathlib import Path

PLOT_DEFAULTS: dict[str, float | int | str] = {
    "dpi": 150,
    "dpi_empty": 120,
    "max_edges": 35,
    "heatmap_cmap": "viridis",
    "stack_colormap": "tab20",
    "fig_cap_inches": 48.0,
    "heatmap_inches_per_site": 0.22,
    "heatmap_min_inches": 8.0,
    "heatmap_min_height": 7.0,
    "edge_bar_fig_height": 6.0,
    "edge_bar_inches_per_label": 0.45,
    "edge_bar_min_width": 10.0,
    "cross_bar_fig_height": 6.0,
    "cross_bar_inches_per_config": 0.55,
    "cross_bar_min_width": 10.0,
    "stack_bar_width": 0.82,
    "heatmap_fontsize": 6,
    "edge_label_fontsize": 6,
    "cross_label_fontsize": 7,
    "legend_fontsize": 7,
    "stack_legend_fontsize": 11,
    "stack_legend_ncol": 1,
}


def matplotlib_install_hint(repo_root: Path) -> str:
    req = repo_root / "requirements-viz.txt"
    return (
        "matplotlib is required for plots (enabled by default).\n"
        f"  pip install -r {req}\n"
        "  pip install matplotlib\n"
        "Or skip plots:  --no-plots"
    )
