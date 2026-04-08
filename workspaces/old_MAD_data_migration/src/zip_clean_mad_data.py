"""Compress selected TTK parent directories into separate zip files.

The script can either scan a source tree such as `CLEAN` to find parent
directories that contain `.ttk` files, or read `run_log.json` from
`ttk2json_batch.py` and zip the directories listed under
`selected_parent_dirs.OK` directly from the raw data tree.

Zip file names are derived from the folder hierarchy so each archive is unique
and easy to map back to its source directory.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
import os
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from tqdm import tqdm


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Zip each selected TTK parent directory into a separate archive "
            "while preserving the folder hierarchy in the archive name."
        )
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path.cwd(),
        help="Workspace root that contains the source tree (default: current directory).",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("CLEAN"),
        help=(
            "Directory to scan for TTK parent folders when --summary-json is "
            "not provided (default: CLEAN under --base-dir)."
        ),
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        default=None,
        help=(
            "Path to the JSON run summary produced by ttk2json_batch.py. "
            "When provided, zip the directories listed under "
            "selected_parent_dirs.OK directly from --base-dir."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Directory for output zip files (default: 'CLEAN_zips' next to "
            "the source tree, or 'run_log_ok_zips' under --base-dir when "
            "--summary-json is provided)."
        ),
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
        help="Show what would be zipped without creating archives.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing zip files instead of skipping them.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(32, os.cpu_count() or 1),
        help="Number of worker processes to use for zipping (default: min(32, cpu_count)).",
    )
    return parser.parse_args(argv)


def normalize_path(path_text: str) -> Path:
    return Path(path_text.replace("/", "\\"))


def load_summary(summary_path: Path) -> dict[str, object]:
    with summary_path.open("r", encoding="utf-8") as summary_file:
        loaded = json.load(summary_file)

    if isinstance(loaded, dict):
        if isinstance(loaded.get("selected_parent_dirs"), dict):
            return loaded

        latest = loaded.get("latest")
        if isinstance(latest, dict):
            return latest

        runs = loaded.get("runs")
        if isinstance(runs, list):
            for entry in reversed(runs):
                if isinstance(entry, dict):
                    return entry

        return loaded

    if isinstance(loaded, list):
        for entry in reversed(loaded):
            if isinstance(entry, dict):
                return entry

    raise ValueError(f"unsupported summary JSON shape in {summary_path}")


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


def relative_root(rel_path: Path) -> str | None:
    parts = rel_path.parts
    if not parts:
        return None
    return parts[0]


def collect_ttk_parent_directories(source_dir: Path, roots: list[str]) -> list[Path]:
    root_filter = set(roots)
    parents: list[Path] = []
    seen: set[Path] = set()

    for ttk_path in source_dir.rglob("*.ttk"):
        if not ttk_path.is_file():
            continue

        parent_dir = ttk_path.parent
        try:
            rel_parent = parent_dir.relative_to(source_dir)
        except ValueError:
            continue

        if root_filter and relative_root(rel_parent) not in root_filter:
            continue

        if parent_dir not in seen:
            seen.add(parent_dir)
            parents.append(parent_dir)

    return sorted(parents, key=lambda path: path.as_posix())


def collect_summary_parent_directories(base_dir: Path, roots: list[str], summary_path: Path) -> list[Path]:
    summary = load_summary(summary_path)
    root_filter = set(roots)
    parents: list[Path] = []
    seen: set[Path] = set()

    for rel_dir_text in selected_ok_dirs(summary):
        rel_dir = normalize_path(rel_dir_text)
        if root_filter and relative_root(rel_dir) not in root_filter:
            continue

        directory = base_dir / rel_dir
        if directory not in seen:
            seen.add(directory)
            parents.append(directory)

    return sorted(parents, key=lambda path: path.relative_to(base_dir).as_posix())


def archive_name_for_directory(source_dir: Path, directory: Path) -> str:
    rel_dir = directory.relative_to(source_dir)
    flat_name = "__".join(rel_dir.parts)
    return f"{flat_name}.zip"


def zip_directory(source_dir: Path, directory: Path, zip_path: Path, dry_run: bool, overwrite: bool) -> tuple[bool, str | None]:
    if not directory.exists():
        return False, f"source directory not found: {directory}"
    if not directory.is_dir():
        return False, f"source path is not a directory: {directory}"

    if zip_path.exists() and not overwrite:
        return False, f"zip already exists: {zip_path}"

    if dry_run:
        return True, None

    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.exists():
        zip_path.unlink()

    with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED, compresslevel=9) as zip_file:
        for child in directory.rglob("*"):
            if not child.is_file():
                continue
            zip_file.write(child, arcname=child.relative_to(source_dir).as_posix())

    return True, None


def zip_directory_task(source_dir: Path, directory: Path, zip_path: Path, dry_run: bool, overwrite: bool) -> tuple[str, bool, str | None]:
    ok, message = zip_directory(source_dir, directory, zip_path, dry_run, overwrite)
    return directory.relative_to(source_dir).as_posix(), ok, message


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    args = parse_args(argv)

    base_dir = args.base_dir.resolve()
    summary_path = None
    if args.summary_json is not None:
        summary_path = (base_dir / args.summary_json).resolve() if not args.summary_json.is_absolute() else args.summary_json.resolve()

    source_dir = (base_dir / args.source_dir).resolve() if not args.source_dir.is_absolute() else args.source_dir.resolve()
    if args.output_dir is None:
        if summary_path is not None:
            output_dir = (base_dir / "run_log_ok_zips").resolve()
        else:
            output_dir = (source_dir.parent / f"{source_dir.name}_zips").resolve()
    elif args.output_dir.is_absolute():
        output_dir = args.output_dir.resolve()
    else:
        output_dir = (base_dir / args.output_dir).resolve()

    tqdm.write(f"[INFO] Base directory: {base_dir}")
    tqdm.write(f"[INFO] Output directory: {output_dir}")
    tqdm.write(f"[INFO] Roots: {', '.join(args.roots)}")
    if summary_path is not None:
        tqdm.write(f"[INFO] Summary JSON: {summary_path}")
    else:
        tqdm.write(f"[INFO] Source directory: {source_dir}")

    if summary_path is not None:
        if not summary_path.exists():
            tqdm.write(f"[ERROR] Summary JSON not found: {summary_path}")
            return 2

        try:
            parent_dirs = collect_summary_parent_directories(base_dir, args.roots, summary_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            tqdm.write(f"[ERROR] Failed to read summary JSON: {exc}")
            return 2
    else:
        if not source_dir.exists():
            tqdm.write(f"[ERROR] Source directory not found: {source_dir}")
            return 2

        parent_dirs = collect_ttk_parent_directories(source_dir, args.roots)

    if not parent_dirs:
        tqdm.write("[INFO] No TTK parent directories found.")
        return 0

    archive_root = base_dir if summary_path is not None else source_dir

    zipped = 0
    skipped = 0
    failed = 0

    worker_count = max(1, args.workers)
    tqdm.write(f"[INFO] Workers: {worker_count}")

    tasks = [
        (directory, output_dir / archive_name_for_directory(archive_root, directory))
        for directory in parent_dirs
    ]

    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(zip_directory_task, archive_root, directory, zip_path, args.dry_run, args.overwrite): (directory, zip_path)
            for directory, zip_path in tasks
        }

        for future in tqdm(as_completed(future_map), total=len(future_map), desc="Zipping", unit="dir"):
            rel_dir, ok, message = future.result()
            directory, zip_path = future_map[future]

            if ok:
                zipped += 1
                tqdm.write(f"[ZIPPED] {rel_dir} -> {zip_path.name}")
            else:
                if message and message.startswith("zip already exists"):
                    skipped += 1
                    tqdm.write(f"[SKIP] {rel_dir}: {message}")
                else:
                    failed += 1
                    tqdm.write(f"[FAIL] {rel_dir}: {message}")

    summary = {
        "base_dir": str(base_dir),
        "source_dir": str(source_dir),
        "summary_json": str(summary_path) if summary_path is not None else None,
        "output_dir": str(output_dir),
        "roots": args.roots,
        "zipped": zipped,
        "skipped": skipped,
        "failed": failed,
        "dry_run": args.dry_run,
    }

    tqdm.write("\n[SUMMARY]")
    tqdm.write(json.dumps(summary, indent=2))

    return 2 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))