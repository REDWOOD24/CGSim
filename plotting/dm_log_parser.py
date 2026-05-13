"""Parse CGSim atlas_grid_simulation logs for reactive + proactive transfer events."""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence


@dataclass(frozen=True)
class TransferRecord:
    """One logical data movement (reactive pull or proactive rebalance initiation)."""

    layer: str  # "reactive" | "proactive"
    template: str
    mode: str  # COPY | MOVE
    src: str
    dst: str
    bytes: int
    filename: str | None
    jobid: str | None


PROACTIVE_INIT_RE = re.compile(
    r"Proactive Data Management: initiating (MOVE|COPY) of '([^']*)' \((\d+)\) \[([^\]]+)\] "
    r"from (\S+) \(util [\d.]+%\) to (\S+) \(util [\d.]+%\) at time (\d+)"
)

REACTIVE_LINE_RE = re.compile(
    r"Reactive Data Management: job (\d+) requesting file '([^']*)'; replicas=\d+, "
    r"chosen_src_site='([^']*)', dst_site='([^']*)', decision_mode=(COPY|MOVE), "
    r"remote_source_template=(\S+) \[replica_sites=([^\]]*)\](?:,\s*file_bytes=(\d+))?"
)


class NegativeTransferBytesError(RuntimeError):
    """Log implied a negative transfer byte count (invalid for heatmaps / log1p scale)."""


def validate_transfer_records_bytes(records: list[TransferRecord], *, log_path: Path | str) -> None:
    for r in records:
        if r.bytes < 0:
            msg = (
                f"Invalid negative transfer byte count ({r.bytes}) in {log_path} — "
                f"layer={r.layer} mode={r.mode} src={r.src!r} dst={r.dst!r} "
                f"template={r.template!r} file={r.filename!r} job={r.jobid!r}"
            )
            print(msg, file=sys.stderr)
            raise NegativeTransferBytesError(msg)


_SITE_TRAILING_INDEX_RE = re.compile(r"^(.*)_(\d+)$")


def sort_site_names_natural(sites: Sequence[str]) -> list[str]:
    """Order site ids by trailing ``_<integer>`` numerically (e.g. ``..._2`` before ``..._10``)."""

    def key(name: str) -> tuple[str, int]:
        m = _SITE_TRAILING_INDEX_RE.match(name)
        if m:
            return (m.group(1), int(m.group(2)))
        return (name, -1)

    return sorted(sites, key=key)


def site_axis_label_padded(name: str, *, width: int = 3) -> str:
    """Axis label: last ``_<n>`` segment zero-padded to ``width`` digits (e.g. ``..._001``)."""
    m = _SITE_TRAILING_INDEX_RE.match(name)
    if m:
        return f"{m.group(1)}_{int(m.group(2)):0{width}d}"
    return name


def load_site_names(site_info_json: Path) -> list[str]:
    """Top-level keys of mimic_new_site_info.json are site identifiers."""
    with site_info_json.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object at root: {site_info_json}")
    sites = sorted(data.keys())
    if not sites:
        raise ValueError(f"No sites found in {site_info_json}")
    return sites


def parse_transfer_log(
    log_path: Path,
) -> tuple[list[TransferRecord], dict[str, int]]:
    """
    Parse a single simulation log.

    Reactive lines may include ``file_bytes=<n>`` (written by the simulator). When that
    suffix is absent (older logs), proactive initiations are scanned first to build
    filename→size hints for imputing reactive transfer bytes.
    """
    text = log_path.read_text(encoding="utf-8", errors="replace")
    file_sizes: dict[str, int] = {}
    for m in PROACTIVE_INIT_RE.finditer(text):
        _mode, fname, size_s, _template, _src, _dst, _sim_t = m.groups()
        size = int(size_s)
        file_sizes[fname] = max(file_sizes.get(fname, 0), size)

    proactive_records: list[TransferRecord] = []
    for m in PROACTIVE_INIT_RE.finditer(text):
        mode, fname, size_s, template, src, dst, _sim_t = m.groups()
        size = int(size_s)
        if size < 0:
            msg = f"Invalid negative proactive file_bytes ({size}) in {log_path}"
            print(msg, file=sys.stderr)
            raise NegativeTransferBytesError(msg)
        proactive_records.append(
            TransferRecord(
                layer="proactive",
                template=template.strip(),
                mode=mode,
                src=src,
                dst=dst,
                bytes=size,
                filename=fname,
                jobid=None,
            )
        )

    reactive_records: list[TransferRecord] = []
    for m in REACTIVE_LINE_RE.finditer(text):
        jobid, fname, chosen, dst, mode, template, rep_blob, nbytes_opt = m.groups()
        chosen = chosen.strip()
        dst = dst.strip()
        template = template.strip()
        if not dst or chosen in ("", "<default>"):
            continue
        if nbytes_opt is not None:
            nbytes = int(nbytes_opt)
        else:
            nbytes = file_sizes.get(fname, 0)
        reactive_records.append(
            TransferRecord(
                layer="reactive",
                template=template,
                mode=mode,
                src=chosen,
                dst=dst,
                bytes=nbytes,
                filename=fname,
                jobid=jobid,
            )
        )

    all_recs = proactive_records + reactive_records
    validate_transfer_records_bytes(all_recs, log_path=log_path)
    return all_recs, file_sizes


def iter_config_run_directories(run_root: Path) -> Iterator[Path]:
    if not run_root.is_dir():
        return
    yield from (
        child
        for child in sorted(run_root.iterdir())
        if child.is_dir() and (child / "toy_config.json").is_file()
    )


def site_info_path_for_config_directory(config_dir: Path, *, fallback: Path) -> Path:
    """
    JSON whose top-level keys define heatmap site axes.

    If ``config_dir/toy_config.json`` has a string ``Sites_Information`` pointing at an
    existing file, use that (site-sweep and per-run layouts). Otherwise use ``fallback``
    (e.g. mimic or ``--site-info``).
    """
    cfg_path = config_dir / "toy_config.json"
    if cfg_path.is_file():
        with cfg_path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
        raw = cfg.get("Sites_Information")
        if isinstance(raw, str) and raw.strip():
            p = Path(raw).expanduser().resolve()
            if p.is_file():
                return p
    return fallback.expanduser().resolve()


def resolve_site_info_json(
    cli_path: Path | None,
    *,
    repo_root: Path,
    base_config_path: Path | None = None,
) -> Path:
    """
    Resolve site-list JSON for heatmap axes.

    - If ``cli_path`` is set, that file must exist (explicit user choice).
    - Otherwise prefer ``toy_data/mimic_new_site_info.json``, then
      ``Sites_Information`` from ``base_config_path``, then
      ``toy_data/toy_new_site_info_1.json``.
    """
    if cli_path is not None:
        p = cli_path.expanduser().resolve()
        if not p.is_file():
            raise FileNotFoundError(f"--site-info not found: {p}")
        return p

    mimic = (repo_root / "toy_data" / "mimic_new_site_info.json").resolve()
    if mimic.is_file():
        return mimic

    if base_config_path is not None and base_config_path.is_file():
        with base_config_path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
        raw = cfg.get("Sites_Information")
        if isinstance(raw, str) and raw.strip():
            p = Path(raw).expanduser().resolve()
            if p.is_file():
                return p

    fallback = (repo_root / "toy_data" / "toy_new_site_info_1.json").resolve()
    if fallback.is_file():
        return fallback

    tried = [str(mimic)]
    if base_config_path:
        tried.append(f"Sites_Information in {base_config_path}")
    tried.append(str(fallback))
    raise FileNotFoundError(
        "No site information JSON found for heatmap axes. Tried:\n  " + "\n  ".join(tried)
    )


def stack_segment_key(rec: TransferRecord) -> str:
    """Stable aggregation key: layer, transfer mode, and policy template."""
    return f"{rec.layer}|{rec.mode}|{rec.template}"


def stack_segment_display_label(segment_key: str) -> str:
    """Human-readable legend text for :func:`stack_segment_key` output."""
    parts = segment_key.split("|", 2)
    if len(parts) == 3:
        layer, mode, template = parts
        layer_t = layer[:1].upper() + layer[1:] if layer else layer
        return f"{layer_t} ({mode}) — {template}"
    if len(parts) == 2:
        return f"{parts[0]} — {parts[1]}"
    return segment_key
