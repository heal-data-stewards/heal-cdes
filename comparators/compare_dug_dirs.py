#
# Compare two directories of Dug-format JSON files (*.dug.json) and produce a
# report summarizing differences. Each input file is a JSON array of node
# records (variables, sections, studies) as emitted by the CDE pipeline.
#
# Usage:
#   uv run python comparators/compare_dug_dirs.py <dir_a> <dir_b> <report_dir>
#
# Outputs (in <report_dir>):
#   - summary.md            high-level summary (counts + file lists)
#   - detail.md             per-file classification and concrete differences
#   - classification.json   machine-readable classification of every file
#   - diffs/<file>.diff     unified diff for each differing common file
#

import json
import re
import difflib
from collections import defaultdict
from pathlib import Path

import click


# Keys whose values are expected to drift between pipeline runs and should not
# count as meaningful differences.
TRIVIAL_KEYS = {"drupal_id"}

# The HEAL CDE REDCap export path includes a run timestamp. Normalize it so
# files that only differ because of the export date are classified as trivial.
HEAL_CDE_EXPORT_PATTERN = re.compile(r"Heal_CDE_\d{4}-\d{2}-\d{2}T\d{6}")

# URLs in the NIH CDE Repository download include a YYYY-MM directory reflecting
# when the file was uploaded. Normalize those too.
NIH_CDE_UPLOAD_PATTERN = re.compile(r"/sites/default/files/CDEs/\d{4}-\d{2}/")


def normalize(obj):
    """Recursively strip trivial keys and normalize volatile strings."""
    if isinstance(obj, dict):
        return {
            k: normalize(v) for k, v in obj.items() if k not in TRIVIAL_KEYS
        }
    if isinstance(obj, list):
        return [normalize(x) for x in obj]
    if isinstance(obj, str):
        s = HEAL_CDE_EXPORT_PATTERN.sub("Heal_CDE_<TIMESTAMP>", obj)
        s = NIH_CDE_UPLOAD_PATTERN.sub("/sites/default/files/CDEs/<YYYY-MM>/", s)
        return s
    return obj


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def index_by_id(records):
    """Index a Dug file's records by (type, id). Records without an id are
    grouped under a synthetic key."""
    out = {}
    unkeyed = []
    for rec in records:
        rid = rec.get("id")
        rtype = rec.get("type", "?")
        if rid is None:
            unkeyed.append(rec)
        else:
            out[(rtype, rid)] = rec
    return out, unkeyed


def diff_records(a_idx, b_idx):
    """Return (only_in_a, only_in_b, changed) sets of (type, id) keys."""
    a_keys = set(a_idx)
    b_keys = set(b_idx)
    only_a = a_keys - b_keys
    only_b = b_keys - a_keys
    changed = {
        k for k in a_keys & b_keys if normalize(a_idx[k]) != normalize(b_idx[k])
    }
    return only_a, only_b, changed


def classify(a_obj, b_obj, max_samples=5):
    """Classify the difference between two Dug files.

    Returns (kind, payload) where kind is one of:
      - 'identical'       byte-identical after JSON re-serialization
      - 'trivial_only'    only differs in drupal_id / timestamp paths
      - 'record_changes'  some records have semantic differences
    payload carries structured details for the report.
    """
    if a_obj == b_obj:
        return "identical", {}

    a_norm = normalize(a_obj)
    b_norm = normalize(b_obj)
    if a_norm == b_norm:
        return "trivial_only", {}

    a_idx, a_unkeyed = index_by_id(a_obj)
    b_idx, b_unkeyed = index_by_id(b_obj)
    only_a, only_b, changed = diff_records(a_idx, b_idx)

    # Figure out which fields actually changed in each changed record.
    field_changes = defaultdict(int)
    sample_changes = []
    for key in changed:
        a_rec = normalize(a_idx[key])
        b_rec = normalize(b_idx[key])
        changed_fields = diff_fields(a_rec, b_rec)
        for fld in changed_fields:
            field_changes[fld] += 1
        if len(sample_changes) < max_samples:
            sample_changes.append((key, changed_fields))

    return "record_changes", {
        "records_only_in_a": sorted(only_a),
        "records_only_in_b": sorted(only_b),
        "records_changed": sorted(changed),
        "field_change_counts": dict(field_changes),
        "sample_changes": sample_changes,
        "unkeyed_a": len(a_unkeyed),
        "unkeyed_b": len(b_unkeyed),
    }


def diff_fields(a, b, prefix=""):
    """Return the set of dotted field paths that differ between two dicts."""
    if a == b:
        return set()
    if not (isinstance(a, dict) and isinstance(b, dict)):
        return {prefix or "<root>"}
    out = set()
    for k in set(a) | set(b):
        sub = f"{prefix}.{k}" if prefix else k
        if k not in a or k not in b:
            out.add(sub)
        else:
            out |= diff_fields(a[k], b[k], sub)
    return out


def unified_diff_text(a_path, b_path):
    with open(a_path, encoding="utf-8") as f:
        a = f.readlines()
    with open(b_path, encoding="utf-8") as f:
        b = f.readlines()
    return "".join(
        difflib.unified_diff(a, b, fromfile=str(a_path), tofile=str(b_path), n=2)
    )


@click.command()
@click.argument(
    "new_dir",
    metavar="NEW_DIR",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.argument(
    "old_dir",
    metavar="OLD_DIR",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.argument("report_dir", type=click.Path(path_type=Path))
@click.option(
    "--glob", default="*.dug.json", show_default=True,
    help="File pattern to compare.",
)
@click.option(
    "--new-label", default=None, help="Short label for NEW_DIR (default: dir name)."
)
@click.option(
    "--old-label", default=None, help="Short label for OLD_DIR (default: dir name)."
)
@click.option(
    "--diffs/--no-diffs",
    default=True,
    help="Write per-file unified diffs to <report_dir>/diffs/.",
)
@click.option(
    "--max-samples", default=5, show_default=True,
    help="Max sample changed records to include per file in detail.md.",
)
def main(new_dir, old_dir, report_dir, glob, new_label, old_label, diffs, max_samples):
    """Compare Dug JSON files in NEW_DIR against OLD_DIR and write a report to REPORT_DIR."""
    label_a = new_label or new_dir.name
    label_b = old_label or old_dir.name

    report_dir.mkdir(parents=True, exist_ok=True)
    diffs_dir = report_dir / "diffs"
    if diffs:
        diffs_dir.mkdir(exist_ok=True)

    a_files = {p.name for p in new_dir.glob(glob)}
    b_files = {p.name for p in old_dir.glob(glob)}

    only_a = sorted(a_files - b_files)
    only_b = sorted(b_files - a_files)
    common = sorted(a_files & b_files)

    classifications = {}
    buckets = defaultdict(list)  # kind -> [filename, ...]

    for name in common:
        try:
            a_obj = load(new_dir / name)
            b_obj = load(old_dir / name)
        except json.JSONDecodeError as e:
            classifications[name] = {"kind": "parse_error", "error": str(e)}
            buckets["parse_error"].append(name)
            continue

        kind, payload = classify(a_obj, b_obj, max_samples=max_samples)
        classifications[name] = {"kind": kind, **payload}
        buckets[kind].append(name)

        if diffs and kind != "identical":
            diff_text = unified_diff_text(new_dir / name, old_dir / name)
            (diffs_dir / f"{name}.diff").write_text(diff_text, encoding="utf-8")

    (report_dir / "classification.json").write_text(
        json.dumps(
            {
                "new_dir": str(new_dir),
                "old_dir": str(old_dir),
                "label_a": label_a,
                "label_b": label_b,
                "only_in_a": only_a,
                "only_in_b": only_b,
                "common_count": len(common),
                "bucket_counts": {k: len(v) for k, v in buckets.items()},
                "by_file": classifications,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    write_summary(
        report_dir / "summary.md", new_dir, old_dir, label_a, label_b, only_a, only_b, buckets
    )
    write_detail(
        report_dir / "detail.md", new_dir, old_dir, label_a, label_b, classifications, buckets
    )

    click.echo(f"Report written to {report_dir}")
    click.echo(f"  only in {label_a} (new): {len(only_a)}")
    click.echo(f"  only in {label_b} (old): {len(only_b)}")
    for kind, files in sorted(buckets.items()):
        click.echo(f"  {kind}: {len(files)}")


def write_summary(path, dir_a, dir_b, label_a, label_b, only_a, only_b, buckets):
    lines = [
        f"# Comparison summary",
        "",
        f"- {label_a}: `{dir_a}`",
        f"- {label_b}: `{dir_b}`",
        "",
        "## Counts",
        "",
        f"- Files only in {label_a}: **{len(only_a)}**",
        f"- Files only in {label_b}: **{len(only_b)}**",
        f"- Common files: **{sum(len(v) for v in buckets.values())}**",
        "",
    ]
    for kind in ("identical", "trivial_only", "record_changes", "parse_error"):
        if kind in buckets:
            lines.append(f"  - `{kind}`: {len(buckets[kind])}")
    lines.append("")
    lines.append("## Classification legend")
    lines.append("")
    lines.append(
        "- **identical** — byte-identical after JSON re-serialization."
    )
    lines.append(
        "- **trivial_only** — differs only in volatile fields "
        "(`drupal_id`, HEAL CDE export timestamp, NIH CDE upload month)."
    )
    lines.append(
        "- **record_changes** — at least one record was added, removed, "
        "or has semantic field changes beyond the volatile fields above."
    )
    lines.append("")

    if only_a:
        lines.append(f"## Files only in {label_a} ({len(only_a)})")
        lines.append("")
        lines.extend(f"- `{f}`" for f in only_a)
        lines.append("")
    if only_b:
        lines.append(f"## Files only in {label_b} ({len(only_b)})")
        lines.append("")
        lines.extend(f"- `{f}`" for f in only_b)
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def write_detail(path, dir_a, dir_b, label_a, label_b, classifications, buckets):
    lines = [
        f"# Detailed comparison",
        "",
        f"- {label_a}: `{dir_a}`",
        f"- {label_b}: `{dir_b}`",
        "",
    ]

    trivial = sorted(buckets.get("trivial_only", []))
    if trivial:
        lines.append(f"## Trivial-only differences ({len(trivial)})")
        lines.append("")
        lines.append(
            "These files differ only in `drupal_id` values or timestamped "
            "paths — expected between pipeline runs."
        )
        lines.append("")
        for name in trivial:
            lines.append(f"- `{name}`")
        lines.append("")

    changed = sorted(buckets.get("record_changes", []))
    if changed:
        # Sort by total record-level churn so the loudest files are listed first.
        def churn(name):
            info = classifications[name]
            return (
                len(info.get("records_only_in_a", []))
                + len(info.get("records_only_in_b", []))
                + len(info.get("records_changed", []))
            )

        changed_sorted = sorted(changed, key=churn, reverse=True)
        lines.append(f"## Files with record-level changes ({len(changed)})")
        lines.append("")
        lines.append(
            "Listed in descending order of record churn. See "
            "`diffs/<file>.diff` for the full unified diff of each entry."
        )
        lines.append("")

        for name in changed_sorted:
            info = classifications[name]
            only_a_recs = info.get("records_only_in_a", [])
            only_b_recs = info.get("records_only_in_b", [])
            recs_changed = info.get("records_changed", [])
            field_counts = info.get("field_change_counts", {})

            lines.append(f"### `{name}`")
            lines.append("")
            lines.append(
                f"- Records only in {label_a}: **{len(only_a_recs)}**, "
                f"only in {label_b}: **{len(only_b_recs)}**, "
                f"changed: **{len(recs_changed)}**"
            )
            if only_a_recs:
                lines.append(
                    f"  - Only in {label_a}: "
                    + ", ".join(f"`{t}:{i}`" for t, i in only_a_recs[:10])
                    + (" …" if len(only_a_recs) > 10 else "")
                )
            if only_b_recs:
                lines.append(
                    f"  - Only in {label_b}: "
                    + ", ".join(f"`{t}:{i}`" for t, i in only_b_recs[:10])
                    + (" …" if len(only_b_recs) > 10 else "")
                )
            if field_counts:
                top = sorted(field_counts.items(), key=lambda x: -x[1])[:10]
                lines.append(
                    "  - Fields changed (count of records affected): "
                    + ", ".join(f"`{f}` ({n})" for f, n in top)
                )
            lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()