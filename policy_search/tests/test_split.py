import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from policy_search.data.split import compute_cutoffs, load_and_sort, write_splits

SOURCE_CSV = Path(__file__).resolve().parents[2] / "config" / "job_data.csv"


def test_split_covers_all_rows_with_no_overlap_or_gap(tmp_path):
    rows = load_and_sort(SOURCE_CSV)
    cutoffs = compute_cutoffs(rows)
    manifest = write_splits(rows, cutoffs, tmp_path, SOURCE_CSV)

    assert manifest["total_rows"] == len(rows)
    total_written = sum(s["row_count"] for s in manifest["splits"].values())
    assert total_written == len(rows)

    calib = list(csv.DictReader(open(tmp_path / "calib.csv")))
    dev = list(csv.DictReader(open(tmp_path / "dev.csv")))
    test = list(csv.DictReader(open(tmp_path / "test.csv")))
    assert len(calib) + len(dev) + len(test) == len(rows)

    # chronological, contiguous: dev starts exactly where calib ends, etc.
    assert float(calib[-1]["creationtime"]) <= float(dev[0]["creationtime"])
    assert float(dev[-1]["creationtime"]) <= float(test[0]["creationtime"])


def test_no_site_is_underrepresented_in_any_split(tmp_path):
    rows = load_and_sort(SOURCE_CSV)
    cutoffs = compute_cutoffs(rows)
    manifest = write_splits(rows, cutoffs, tmp_path, SOURCE_CSV)

    for split_name, split_info in manifest["splits"].items():
        n = split_info["row_count"]
        for site, count in split_info["site_counts"].items():
            share = count / n
            assert 0.15 < share < 0.45, f"{split_name}/{site} share {share} out of bounds"


def test_manifest_cutoffs_are_reproducible(tmp_path):
    rows = load_and_sort(SOURCE_CSV)
    cutoffs_a = compute_cutoffs(rows)
    manifest_a = write_splits(rows, cutoffs_a, tmp_path / "run_a", SOURCE_CSV)

    cutoffs_b = compute_cutoffs(rows)
    manifest_b = write_splits(rows, cutoffs_b, tmp_path / "run_b", SOURCE_CSV)

    assert manifest_a["t_calib_end"] == manifest_b["t_calib_end"]
    assert manifest_a["t_dev_end"] == manifest_b["t_dev_end"]
    assert manifest_a["calib_end_index"] == manifest_b["calib_end_index"]
    assert manifest_a["dev_end_index"] == manifest_b["dev_end_index"]


def test_split_csv_columns_match_source():
    with open(SOURCE_CSV) as f:
        source_header = next(csv.reader(f))

    rows = load_and_sort(SOURCE_CSV)
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        write_splits(rows, compute_cutoffs(rows), Path(d), SOURCE_CSV)
        with open(Path(d) / "dev.csv") as f:
            split_header = next(csv.reader(f))
        assert split_header == source_header
