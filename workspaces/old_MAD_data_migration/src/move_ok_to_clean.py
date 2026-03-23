"""Move successful batch-conversion folders into CLEAN while preserving hierarchy.

The script reads the `run_log.json` summary written by `ttk2json_batch.py`,
selects the parent directories listed under `selected_parent_dirs.OK`, and
moves those directories from `--base-dir` into `--clean-dir`, retaining the
same relative path structure.

The `--roots` argument matches the batch processor's interface so you can
limit the move to the same top-level roots if desired.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Move OK batch-conversion folders into CLEAN while preserving "
            "the relative hierarchy."
        )
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path.cwd(),
        help="Workspace root that contains the MAD result tree and run_log.json (default: current directory).",
    )
    parser.add_argument(
        "--clean-dir",
        type=Path,
        default=Path("CLEAN"),
        help="Destination root for moved directories (default: CLEAN under --base-dir).",
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        default=Path("run_log.json"),
        help="Path to the JSON run summary produced by ttk2json_batch.py (default: run_log.json under --base-dir).",
    )
    parser.add_argument(
        "--roots",
        nargs="+",
        default=["SkyWater", "DBH", "Leti", "Onsemi"],
        help="Top-level directories to consider, matching ttk2json_batch.py.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be moved without changing anything.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Move even if the destination already exists by skipping that entry silently.",
    )
    return parser.parse_args(argv)


def load_summary(summary_path: Path) -> dict[str, object]:
    with summary_path.open("r", encoding="utf-8") as summary_file:
        return json.load(summary_file)


def normalize_relative_path(rel_path: str) -> Path:
    return Path(rel_path.replace("/", "\\"))


def selected_ok_dirs(summary: dict[str, object]) -> list[str]:
    selected = summary.get("selected_parent_dirs", {})
    if not isinstance(selected, dict):
        return []

    ok_entries = selected.get("OK", [])
    if not isinstance(ok_entries, list):
        return []

    parent_dirs: list[str] = []
    for entry in ok_entries:
        if isinstance(entry, str):
            parent_dirs.append(entry)
        elif isinstance(entry, dict):
            parent_dir = entry.get("parent_dir")
            if isinstance(parent_dir, str):
                parent_dirs.append(parent_dir)

    return parent_dirs


def top_level_root(rel_path: Path) -> str | None:
    parts = rel_path.parts
    if not parts:
        return None
    return parts[0]


def move_directory(source: Path, destination: Path, dry_run: bool, force: bool) -> tuple[bool, str | None]:
    if not source.exists():
        return False, f"source not found: {source}"
    if not source.is_dir():
        return False, f"source is not a directory: {source}"

    if destination.exists():
        if force:
            return True, None
        return False, f"destination already exists: {destination}"

    if dry_run:
        return True, None

    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))
    return True, None


def source_status(source: Path, destination: Path) -> str:
    source_exists = source.exists()
    destination_exists = destination.exists()

    if source_exists and destination_exists:
        return "both"
    if source_exists:
        return "source"
    if destination_exists:
        return "destination"
    return "missing"


def prune_empty_directories(root_dir: Path, dry_run: bool) -> int:
    if not root_dir.exists() or not root_dir.is_dir():
        return 0

    def prune_node(current_dir: Path) -> int:
        removed = 0

        for child in current_dir.iterdir():
            if child.is_dir():
                removed += prune_node(child)

        if current_dir == root_dir:
            return removed

        try:
            has_children = any(current_dir.iterdir())
        except OSError:
            return removed

        if has_children:
            return removed

        if dry_run:
            return removed + 1

        try:
            current_dir.rmdir()
        except OSError:
            return removed
        return removed + 1

    return prune_node(root_dir)


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    args = parse_args(argv)

    base_dir = args.base_dir.resolve()
    clean_dir = (base_dir / args.clean_dir).resolve() if not args.clean_dir.is_absolute() else args.clean_dir.resolve()
    summary_path = (base_dir / args.summary_json).resolve() if not args.summary_json.is_absolute() else args.summary_json.resolve()

    print(f"[INFO] Base directory: {base_dir}")
    print(f"[INFO] Clean directory: {clean_dir}")
    print(f"[INFO] Summary JSON: {summary_path}")
    print(f"[INFO] Roots: {', '.join(args.roots)}")

    if not summary_path.exists():
        print(f"[ERROR] Summary JSON not found: {summary_path}")
        return 2

    try:
        summary = load_summary(summary_path)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[ERROR] Failed to read summary JSON: {exc}")
        return 2

    ok_rel_dirs = selected_ok_dirs(summary)
    if not ok_rel_dirs:
        print("[INFO] No OK directories found in the summary; pruning empty directories only.")

    root_filter = set(args.roots)
    moved = 0
    skipped = 0
    failed = 0
    touched_roots: set[str] = {root_name for root_name in args.roots if (base_dir / root_name).exists()}

    for rel_dir_text in ok_rel_dirs:
        rel_dir = normalize_relative_path(rel_dir_text)
        if root_filter and top_level_root(rel_dir) not in root_filter:
            skipped += 1
            continue

        source = base_dir / rel_dir
        destination = clean_dir / rel_dir
        touched_roots.add(rel_dir.parts[0])

        status = source_status(source, destination)
        if status == "destination":
            print(f"[SKIP] Already moved: {rel_dir.as_posix()}")
            moved += 1
            continue
        if status == "missing":
            failed += 1
            print(f"[FAIL] {rel_dir.as_posix()}: source and destination both missing")
            continue

        ok, message = move_directory(source, destination, args.dry_run, args.force)
        if ok:
            moved += 1
            print(f"[MOVED] {rel_dir.as_posix()}")
        else:
            failed += 1
            print(f"[FAIL] {rel_dir.as_posix()}: {message}")

    pruned = 0
    for root_name in sorted(touched_roots):
        root_dir = base_dir / root_name
        pruned += prune_empty_directories(root_dir, args.dry_run)

    print("\n[SUMMARY]")
    print(f"  Moved: {moved}")
    print(f"  Skipped by root filter: {skipped}")
    print(f"  Failed: {failed}")
    print(f"  Empty directories removed: {pruned}")
    if args.dry_run:
        print("  Dry run: yes")

    return 2 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))